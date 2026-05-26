#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER
"""

import os
import json
import asyncio
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler

import aiohttp
from dotenv import load_dotenv

load_dotenv()

_LOG_LEVEL = logging.DEBUG if os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG" else logging.INFO
logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS = {
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {"name": "TheSpirit",  "risk_type": "price_based"},
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "WalletA179", "risk_type": "fixed", "fixed_risk": 0.20},
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
CLOB_API_KEY     = os.getenv("POLY_API_KEY", "")
CLOB_SECRET      = os.getenv("POLY_SECRET", "")
CLOB_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")

BANKROLL_FALLBACK = float(os.getenv("BANKROLL", "0"))

MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "9999"))  # no hard limit
POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.20"))   # 20%
MAX_EXPOSURE       = 0.80                                       # 80% max total exposure
STOP_LOSS          = 0.50                                       # close if price drops 50% below entry
TRAIL_STOP         = 0.25                                       # close if price drops 25% below peak
MAX_PER_TRADE      = 0.03                                       # 3% per trade
MIN_TRADE_FRAC     = 0.006                                      # 0.6% of bankroll floor per trade
MAX_TRADE_FRAC     = 0.03                                       # 3% of bankroll ceiling per trade
MIN_SOURCE_SIZE    = 1.0                                        # only copy source trades ≥ $1
LIMIT_ORDER_TICK   = 0.01                                       # place limit 1 tick inside best ask

HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 24                                         # pause 24h on drawdown

POLYGON_RPCS  = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-rpc.com",
]
BALANCE_OF_SELECTOR = "0x70a08231"
PUSD_CONTRACTS = [
    ("pUSD",             "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB", 6),
    ("CTF-v2",           "0xE111180000d2663C0091e4f400237545B87B996B", 6),
    ("NegRisk-v2",       "0xe2222d279d744050d28e00520010520000310F59", 6),
]

peak_bankroll: float = BANKROLL_FALLBACK
bot_paused_until: Optional[datetime] = None


# ==================== DATA CLASS ====================
@dataclass
class Position:
    market_id: str
    question: str
    outcome: str
    token_id: str
    side: str               # "BUY" or "SELL"
    entry_price: float
    size_usd: float
    shares: float
    source_wallet: str
    source_name: str
    order_type: str = "LIMIT"
    status: str = "open"
    exit_price: float = 0.0
    pnl: float = 0.0
    opened_at: datetime = field(default_factory=datetime.now)
    peak_price: float = 0.0
    current_price: float = 0.0

    # ========== PnL METHODS (merged from Rust bot) ==========
    def pnl_unrealized(self) -> float:
        """
        Calculate unrealized PnL for open position.
        Formula: (size_usd / entry_price) * current_price - size_usd
        (identical to Rust's SimPosition::pnl)
        """
        if self.status != "open" or self.entry_price <= 0 or self.current_price <= 0:
            return 0.0
        tokens = self.size_usd / self.entry_price
        return tokens * self.current_price - self.size_usd

    def pnl_pct(self) -> float:
        """
        Calculate PnL percentage for open position.
        Formula: (current_price - entry_price) / entry_price * 100
        (identical to Rust's SimPosition::pnl_pct)
        """
        if self.status != "open" or self.entry_price <= 0:
            return 0.0
        return (self.current_price - self.entry_price) / self.entry_price * 100.0

    def current_value(self) -> float:
        """Current market value of the position."""
        if self.status != "open" or self.entry_price <= 0 or self.current_price <= 0:
            return 0.0
        tokens = self.size_usd / self.entry_price
        return tokens * self.current_price

    def total_pnl(self) -> float:
        """Return realized PnL if closed, unrealized if open."""
        if self.status == "closed":
            return self.pnl
        return self.pnl_unrealized()


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = 0
        self._breakdown: dict[str, float] = {}

    def _call_payload(self, contract: str, wallet: str) -> dict:
        padded = wallet.lower().replace("0x", "").zfill(64)
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": contract, "data": BALANCE_OF_SELECTOR + padded}, "latest"],
        }

    async def _query_contract(
        self,
        session: aiohttp.ClientSession,
        rpc_url: str,
        label: str,
        contract: str,
        decimals: int,
        wallet: str,
    ) -> Optional[float]:
        try:
            payload = self._call_payload(contract, wallet)
            async with session.post(
                rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logging.debug(f"  {label}: HTTP {resp.status} — {text[:120]}")
                    return None
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = json.loads(text)
                if "error" in data:
                    logging.debug(f"  {label}: RPC error {data['error']}")
                    return None
                hex_val = data.get("result") or "0x0"
                if not hex_val or hex_val in ("0x", "0x0"):
                    return 0.0
                amount = int(hex_val, 16) / (10 ** decimals)
                logging.debug(f"  {label}: ${amount:.6f}")
                return amount
        except Exception as e:
            logging.debug(f"  {label} @ {rpc_url}: {e}")
            return None

    async def _fetch_rpc(self, session: aiohttp.ClientSession, wallet: str) -> Optional[float]:
        logging.debug(f"Fetching balance for wallet {wallet}")
        for rpc_url in POLYGON_RPCS:
            total = 0.0
            breakdown: dict[str, float] = {}
            rpc_alive = False

            for label, contract, decimals in PUSD_CONTRACTS:
                amount = await self._query_contract(
                    session, rpc_url, label, contract, decimals, wallet
                )
                if amount is None:
                    continue
                rpc_alive = True
                if amount > 0:
                    breakdown[label] = amount
                    total += amount

            if rpc_alive:
                self._breakdown = breakdown
                if total > 0:
                    parts = ", ".join(f"{k}=${v:.4f}" for k, v in breakdown.items())
                    logging.info(f"RPC balance: ${total:.4f} ({parts}) via {rpc_url}")
                else:
                    logging.info(f"RPC balance: $0.0000 (all contracts zero) via {rpc_url}")
                return total

            logging.warning(f"RPC {rpc_url} unreachable — trying next")

        return None

    async def _fetch_polymarket_api(
        self, session: aiohttp.ClientSession, wallet: str
    ) -> Optional[float]:
        try:
            url = f"https://data-api.polymarket.com/value?user={wallet}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                if isinstance(data, (int, float)):
                    value = float(data)
                elif isinstance(data, dict):
                    value = float(
                        data.get("portfolioValue")
                        or data.get("value")
                        or data.get("balance")
                        or 0
                    )
                else:
                    return None

                if value >= 0:
                    logging.info(f"Polymarket API balance: ${value:.4f}")
                    return value
        except Exception as e:
            logging.warning(f"Polymarket API balance fetch failed: {e}")
        return None

    async def get(self, session: aiohttp.ClientSession, force: bool = False) -> float:
        global peak_bankroll

        cache_age = time.time() - self.last_update
        if not force and cache_age < 60 and self.cached_balance > 0:
            return self.cached_balance

        if not YOUR_WALLET:
            logging.warning("DEPOSIT_WALLET_ADDRESS not set — using cached/fallback balance")
            return self.cached_balance
        if not YOUR_WALLET.startswith("0x") or len(YOUR_WALLET) != 42:
            logging.error(f"DEPOSIT_WALLET_ADDRESS looks invalid: '{YOUR_WALLET}'")
            return self.cached_balance
        logging.debug(f"Balance check for {YOUR_WALLET}")

        fetched = await self._fetch_rpc(session, YOUR_WALLET)

        if fetched is None:
            fetched = await self._fetch_polymarket_api(session, YOUR_WALLET)

        if fetched is not None:
            self.cached_balance = fetched
            self.last_update = time.time()
            if fetched > peak_bankroll:
                peak_bankroll = fetched
        else:
            logging.warning("All balance sources failed — using cached $%.4f", self.cached_balance)

        return self.cached_balance

    def adjust(self, delta: float) -> None:
        self.cached_balance = max(0.0, self.cached_balance + delta)
        logging.debug(f"Balance adjusted by {delta:+.4f} → ${self.cached_balance:.4f}")


# ==================== EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self._client = None

        if not dry_run:
            self._init_client()

    def _init_client(self):
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key=CLOB_API_KEY,
                api_secret=CLOB_SECRET,
                api_passphrase=CLOB_PASSPHRASE,
            )
            self._client = ClobClient(
                host="https://clob.polymarket.com",
                key=YOUR_PRIVATE_KEY,
                chain_id=137,
                creds=creds,
            )
            logging.info("CLOB client initialised (LIVE mode)")
        except ImportError:
            logging.error("py-clob-client not installed. Run: pip install py-clob-client")
            raise
        except Exception as e:
            logging.error(f"CLOB client init failed: {e}")
            raise

    async def place_limit_buy(
        self,
        token_id: str,
        amount_usd: float,
        best_ask: float,
    ) -> tuple[bool, str]:
        amount_usd = min(round(amount_usd, 2), 1.0)
        limit_price = round(max(best_ask - LIMIT_ORDER_TICK, 0.01), 4)
        shares = round(amount_usd / limit_price, 4)

        if self.dry_run:
            fake_id = f"dry-lmt-{int(time.time())}-{token_id[:8]}"
            logging.info(
                f"[DRY RUN] LIMIT BUY {shares:.4f} sh @ {limit_price:.4f} "
                f"(${amount_usd:.2f}) → {fake_id}"
            )
            return True, fake_id

        if not self._client:
            return False, "client_not_initialised"

        try:
            from py_clob_client.clob_types import LimitOrderArgs, OrderType

            order_args = LimitOrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side="BUY",
            )
            signed_order = self._client.create_limit_order(order_args)
            resp = self._client.post_order(signed_order, OrderType.GTC)

            order_id = resp.get("orderID", "") or resp.get("id", "")
            status = resp.get("status", "unknown")

            if status in ("matched", "live", "open"):
                logging.info(
                    f"[LIVE] Limit order live: {order_id} "
                    f"price={limit_price:.4f} shares={shares:.4f} status={status}"
                )
                return True, order_id
            else:
                logging.warning(f"[LIVE] Limit order rejected: {resp}")
                return False, status

        except Exception as e:
            logging.error(f"place_limit_buy error: {e}")
            return False, str(e)

    async def place_sell(
        self,
        token_id: str,
        shares: float,
        price: float,
    ) -> tuple[bool, str]:
        limit_price = round(min(price + LIMIT_ORDER_TICK, 0.99), 4)

        if self.dry_run:
            fake_id = f"dry-sell-{int(time.time())}-{token_id[:8]}"
            logging.info(
                f"[DRY RUN] LIMIT SELL {shares:.4f} sh @ {limit_price:.4f} → {fake_id}"
            )
            return True, fake_id

        if not self._client:
            return False, "client_not_initialised"

        try:
            from py_clob_client.clob_types import LimitOrderArgs, OrderType

            order_args = LimitOrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side="SELL",
            )
            signed_order = self._client.create_limit_order(order_args)
            resp = self._client.post_order(signed_order, OrderType.GTC)

            order_id = resp.get("orderID", "") or resp.get("id", "")
            status = resp.get("status", "unknown")

            if status in ("matched", "live", "open"):
                logging.info(f"[LIVE] Limit sell live: {order_id} status={status}")
                return True, order_id
            else:
                logging.warning(f"[LIVE] Limit sell rejected: {resp}")
                return False, status

        except Exception as e:
            logging.error(f"place_sell error: {e}")
            return False, str(e)


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.positions: Dict[str, Position] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.balance = RobustBalanceManager()

    # ========== PnL Methods (merged from Rust bot) ==========
    
    def get_wallet_stats(self) -> Dict[str, dict]:
        """
        Return PnL statistics per source wallet (like Rust leaderboard).
        Returns dict with keys: profit_trades, loss_trades, total_pnl, win_rate
        """
        stats: Dict[str, dict] = {}
        for pos in self.positions.values():
            wallet = pos.source_wallet
            if wallet not in stats:
                stats[wallet] = {
                    "profit_trades": 0,
                    "loss_trades": 0,
                    "total_pnl": 0.0,
                    "wallet_name": pos.source_name,
                }
            pnl_val = pos.total_pnl()
            stats[wallet]["total_pnl"] += pnl_val
            if pnl_val >= 0:
                stats[wallet]["profit_trades"] += 1
            else:
                stats[wallet]["loss_trades"] += 1
        
        # Add win rate percentage
        for wallet in stats:
            total = stats[wallet]["profit_trades"] + stats[wallet]["loss_trades"]
            stats[wallet]["win_rate"] = (stats[wallet]["profit_trades"] / total * 100.0) if total > 0 else 0.0
        
        return stats

    def total_unrealized_pnl(self) -> float:
        """Total unrealized PnL across all open positions."""
        return sum(pos.pnl_unrealized() for pos in self.positions.values() if pos.status == "open")

    def total_realized_pnl(self) -> float:
        """Total realized PnL from closed positions."""
        return sum(pos.pnl for pos in self.positions.values() if pos.status == "closed")

    def total_pnl(self) -> float:
        """Combined realized + unrealized PnL (like Rust's total_pnl)."""
        return self.total_unrealized_pnl() + self.total_realized_pnl()

    def total_open_value(self) -> float:
        """Current market value of all open positions."""
        return sum(pos.current_value() for pos in self.positions.values() if pos.status == "open")

    # -------- helpers --------

    def _trade_size(self, bankroll: float, wallet_addr: str, mid_price: float) -> float:
        config = WALLETS[wallet_addr]
        risk_type = config.get("risk_type", "fixed")

        if risk_type == "price_based":
            fraction = max(0.05, min(mid_price, 1.0)) * MAX_TRADE_FRAC
        else:
            fraction = config.get("fixed_risk", MAX_TRADE_FRAC)

        fraction = max(MIN_TRADE_FRAC, min(fraction, MAX_TRADE_FRAC))
        size = round(bankroll * fraction, 2)

        headroom = max(0.0, bankroll * MAX_EXPOSURE - self._total_exposure())
        size = min(size, round(headroom, 2))

        return round(size, 2)

    def _total_exposure(self) -> float:
        return sum(p.size_usd for p in self.positions.values() if p.status == "open")

    def _check_drawdown(self, bankroll: float) -> bool:
        global bot_paused_until
        if peak_bankroll <= 0:
            return False
        drawdown = (peak_bankroll - bankroll) / peak_bankroll
        if drawdown >= MAX_DRAWDOWN:
            bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
            logging.warning(
                f"Drawdown {drawdown:.1%} ≥ {MAX_DRAWDOWN:.1%} — "
                f"pausing until {bot_paused_until:%Y-%m-%d %H:%M}"
            )
            return True
        return False

    # -------- API calls --------

    async def get_positions(self, session: aiohttp.ClientSession, wallet_addr: str):
        try:
            url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=100"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    logging.warning(f"positions API {r.status} for {wallet_addr[:10]}…")
                    return None
                data = await r.json()

                cleaned = []
                for p in (data if isinstance(data, list) else []):
                    token_id = p.get("asset")
                    if not token_id:
                        continue

                    value = float(p.get("currentValue") or p.get("value") or 0)
                    shares = float(p.get("size") or p.get("shares") or 0)

                    if value == 0 and shares > 0:
                        price_hint = float(p.get("price") or p.get("lastTradePrice") or 0)
                        value = shares * price_hint

                    if value < MIN_SOURCE_SIZE:
                        continue

                    raw_side = (p.get("side") or "").upper()
                    if raw_side in ("BUY", "SELL"):
                        side = raw_side
                    else:
                        side = "SELL" if value < 0 else "BUY"

                    cleaned.append({
                        "asset":   token_id,
                        "title":   p.get("title", "Unknown Market"),
                        "outcome": p.get("outcome", "YES"),
                        "side":    side,
                        "value":   abs(value),
                        "shares":  abs(shares),
                    })
                return cleaned
        except Exception as e:
            logging.error(f"Fetch positions error ({wallet_addr[:10]}…): {e}")
            return None

    async def get_orderbook(
        self, session: aiohttp.ClientSession, token_id: str
    ) -> tuple[float, float]:
        try:
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bb = float(bids[0]["price"]) if bids else 0.0
                    ba = float(asks[0]["price"]) if asks else 0.0
                    return bb, ba
        except Exception as e:
            logging.warning(f"get_orderbook error for {token_id[:12]}…: {e}")
        return 0.0, 0.0

    async def get_mid_price(self, session: aiohttp.ClientSession, token_id: str) -> float:
        bb, ba = await self.get_orderbook(session, token_id)
        if bb and ba:
            return (bb + ba) / 2
        return bb or ba or 0.0

    # -------- source wallet tracking --------

    async def _get_source_position(
        self,
        session: aiohttp.ClientSession,
        wallet_addr: str,
        token_id: str,
    ) -> Optional[dict]:
        try:
            url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=100"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    logging.warning(f"_get_source_position: API {r.status} for {wallet_addr[:10]}…")
                    return {"_network_error": True}
                data = await r.json()
                positions = data if isinstance(data, list) else []
                match = next((p for p in positions if p.get("asset") == token_id), None)
                if match is None:
                    return None
                value = float(match.get("currentValue") or match.get("value") or match.get("size") or 0)
                shares = float(match.get("size") or match.get("shares") or 0)
                raw_side = (match.get("side") or "").upper()
                side = raw_side if raw_side in ("BUY", "SELL") else ("SELL" if value < 0 else "BUY")
                return {
                    "asset":   token_id,
                    "title":   match.get("title", ""),
                    "outcome": match.get("outcome", "YES"),
                    "side":    side,
                    "value":   abs(value),
                    "shares":  abs(shares),
                }
        except Exception as e:
            logging.warning(f"_get_source_position error ({wallet_addr[:10]}…): {e}")
            return {"_network_error": True}

    # -------- update PnL for current positions --------
    
    async def update_positions_pnl(self, session: aiohttp.ClientSession):
        """
        Update PnL for all open positions based on current market prices.
        Uses Rust formulas for PnL calculation.
        """
        for pos_key, pos in self.positions.items():
            if pos.status != "open":
                continue
            
            mid_price = await self.get_mid_price(session, pos.token_id)
            
            if mid_price > 0:
                pos.current_price = mid_price
                
                if pos.peak_price <= 0:
                    pos.peak_price = pos.entry_price
                if mid_price > pos.peak_price:
                    pos.peak_price = mid_price
                
                # Update pnl field using Rust formula
                pos.pnl = pos.pnl_unrealized()
                
                logging.debug(
                    f"PnL UPDATE {pos.question[:40]} | "
                    f"price={mid_price:.4f} entry={pos.entry_price:.4f} "
                    f"unrealized PnL=${pos.pnl:+.2f} ({pos.pnl_pct():+.1f}%)"
                )

    # -------- execute + refresh --------

    async def _execute_and_refresh(
        self,
        session: aiohttp.ClientSession,
        action: str,
        token_id: str,
        shares: float,
        size_usd: float,
        price: float,
        best_ask: float = 0.0,
    ) -> tuple[bool, str]:
        if action == "BUY":
            ask = best_ask if best_ask > 0 else price
            ok, oid = await self.executor.place_limit_buy(token_id, size_usd, ask)
        else:
            ok, oid = await self.executor.place_sell(token_id, shares, price)

        if ok:
            delta = -size_usd if action == "BUY" else size_usd
            self.balance.adjust(delta)
            asyncio.ensure_future(self.balance.get(session, force=True))

        return ok, oid

    # -------- exit scanner --------

    async def scan_for_exits(self, session: aiohttp.ClientSession):
        wallet_snapshot: dict[str, dict] = {}
        wallet_error: set[str] = set()

        for wallet_addr in set(p.source_wallet for p in self.positions.values() if p.status == "open"):
            try:
                url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=500"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        logging.warning(f"scan_for_exits: API {r.status} for {wallet_addr[:10]}… — holding all")
                        wallet_error.add(wallet_addr)
                        continue
                    data = await r.json()
                    positions = data if isinstance(data, list) else []
                    wallet_snapshot[wallet_addr] = {p.get("asset"): p for p in positions if p.get("asset")}
                    logging.info(f"EXIT-SCAN fetched {len(wallet_snapshot[wallet_addr])} positions for {wallet_addr[:10]}…")
            except Exception as e:
                logging.warning(f"scan_for_exits fetch error ({wallet_addr[:10]}…): {e} — holding all")
                wallet_error.add(wallet_addr)

        to_close: list[tuple[str, Position, float, str]] = []

        for pos_key, pos in list(self.positions.items()):
            if pos.status != "open":
                continue

            if pos.source_wallet in wallet_error:
                continue

            snapshot = wallet_snapshot.get(pos.source_wallet, {})
            source_raw = snapshot.get(pos.token_id)

            mid_price = await self.get_mid_price(session, pos.token_id)

            if mid_price > 0:
                pos.current_price = mid_price
                if pos.peak_price <= 0:
                    pos.peak_price = pos.entry_price
                if mid_price > pos.peak_price:
                    pos.peak_price = mid_price

            src_val = float(source_raw.get("currentValue") or source_raw.get("value") or 0) if source_raw else 0
            logging.info(
                f"  CHECK {pos.question[:45]} | "
                f"mid={mid_price:.3f} entry={pos.entry_price:.3f} peak={pos.peak_price:.3f} "
                f"source={'$'+str(round(src_val,2)) if source_raw else 'EXITED'}"
            )

            reason = None

            if source_raw is None:
                reason = "source_exited"
            elif mid_price > 0 and mid_price <= pos.entry_price * (1 - STOP_LOSS):
                reason = f"stop_loss_50% (entry={pos.entry_price:.3f} now={mid_price:.3f})"
            elif mid_price > 0 and pos.peak_price > 0 and mid_price <= pos.peak_price * (1 - TRAIL_STOP):
                reason = f"trail_stop_25% (peak={pos.peak_price:.3f} now={mid_price:.3f})"

            if reason:
                to_close.append((pos_key, pos, mid_price or pos.entry_price, reason))

        for pos_key, pos, exit_price, reason in to_close:
            ok, _ = await self._execute_and_refresh(
                session, "SELL", pos.token_id, pos.shares, pos.size_usd, exit_price
            )
            if ok:
                pos.status = "closed"
                pos.exit_price = exit_price
                pos.pnl = (exit_price - pos.entry_price) * pos.shares
                logging.info(
                    f"CLOSED [{reason}] {pos.question[:50]} | "
                    f"entry={pos.entry_price:.3f} peak={pos.peak_price:.3f} "
                    f"exit={pos.exit_price:.3f} pnl=${pos.pnl:+.2f}"
                )
                del self.positions[pos_key]

    # -------- main scan loop --------

    async def scan_and_copy(self):
        global bot_paused_until, peak_bankroll

        if bot_paused_until and datetime.now() < bot_paused_until:
            remaining = bot_paused_until - datetime.now()
            logging.info(f"Bot paused — {remaining} remaining")
            return

        async with aiohttp.ClientSession() as session:
            bankroll = await self.balance.get(session, force=True)
            if bankroll < 1.0:
                logging.warning(f"Bankroll too low (${bankroll:.4f}) — skipping cycle")
                return

            if bankroll > peak_bankroll:
                peak_bankroll = bankroll

            if self._check_drawdown(bankroll):
                return

            open_count = sum(1 for p in self.positions.values() if p.status == "open")
            logging.info(
                f"Scanning | bankroll=${bankroll:.4f} | peak=${peak_bankroll:.4f} | "
                f"open={open_count}/{MAX_POSITIONS} | "
                f"exposure=${self._total_exposure():.2f}"
            )

            # Update PnL for all open positions using Rust formulas
            await self.update_positions_pnl(session)
            
            await self.scan_for_exits(session)

            for wallet_addr, config in WALLETS.items():
                raw = await self.get_positions(session, wallet_addr)
                if not raw:
                    continue

                for pos in raw:
                    token_id = pos["asset"]
                    question = pos["title"]
                    side = pos["side"]
                    pos_key = f"{wallet_addr}_{token_id}_{side}"

                    if pos_key in self.positions:
                        continue

                    if pos["value"] < MIN_SOURCE_SIZE:
                        continue

                    best_bid, best_ask = await self.get_orderbook(session, token_id)
                    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask

                    if mid_price <= 0.01:
                        logging.debug(f"Skipping {token_id[:12]}… — no price")
                        continue

                    my_size = self._trade_size(bankroll, wallet_addr, mid_price)

                    if my_size <= 0:
                        available = bankroll * MAX_EXPOSURE - self._total_exposure()
                        logging.info(
                            f"Exposure cap — skipping {question[:50]} "
                            f"(size=${my_size:.2f} headroom=${available:.2f})"
                        )
                        continue

                    shares = round(my_size / mid_price, 4)

                    ok, order_id = await self._execute_and_refresh(
                        session, side, token_id, shares, my_size, mid_price,
                        best_ask=best_ask,
                    )

                    if ok:
                        self.positions[pos_key] = Position(
                            market_id="",
                            question=question,
                            outcome=pos["outcome"],
                            side=side,
                            token_id=token_id,
                            entry_price=mid_price,
                            size_usd=my_size,
                            shares=shares,
                            source_wallet=wallet_addr,
                            source_name=config["name"],
                            order_type="LIMIT",
                            peak_price=mid_price,
                            current_price=mid_price,
                            pnl=0.0,
                        )
                        logging.info(
                            f"COPIED [{config['name']}] LIMIT {side} ${my_size:.2f} "
                            f"@ {mid_price:.3f} ({pos['outcome']}) → {question[:50]}"
                        )

    async def run(self):
        logging.info(f"Bot started | dry_run={self.dry_run} | wallets={len(WALLETS)}")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Loop error: {e}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)


# ==================== DASHBOARD SERVER ====================
def run_dashboard():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            if self.path == "/":
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                try:
                    bankroll = bot.balance.cached_balance
                    open_positions = [p for p in bot.positions.values() if p.status == "open"]
                    closed_positions = [p for p in bot.positions.values() if p.status == "closed"]
                    
                    # Calculate PnL using Rust formulas
                    total_pnl = bot.total_pnl()
                    total_open_pnl = bot.total_unrealized_pnl()
                    total_closed_pnl = bot.total_realized_pnl()
                    
                    # Wallet stats leaderboard
                    wallet_stats = bot.get_wallet_stats()
                    
                    def get_pnl_color(pnl):
                        if pnl > 0:
                            return "#4ade80"
                        elif pnl < 0:
                            return "#f87171"
                        return "#e0e0e0"
                    
                    def format_pnl(pnl):
                        return f"${pnl:+.2f}"
                    
                    # Build table rows for open positions
                    open_rows = ""
                    for p in open_positions:
                        unrealized_pnl = p.pnl_unrealized()
                        pnl_pct = p.pnl_pct()
                        pnl_color = get_pnl_color(unrealized_pnl)
                        current_price_display = f"{p.current_price:.4f}" if p.current_price > 0 else "N/A"
                        
                        open_rows += f"""
                        <tr>
                            <td style="font-family:monospace">{p.source_name}</td>
                            <td style="max-width:300px; overflow:hidden; text-overflow:ellipsis;">{p.question[:60]}</td>
                            <td style="color: {'#4ade80' if p.side == 'BUY' else '#f87171'}">{p.side}</td>
                            <td style="font-family:monospace">{p.outcome}</td>
                            <td style="font-family:monospace">${p.size_usd:.2f}</td>
                            <td style="font-family:monospace">{p.entry_price:.4f}</td>
                            <td style="font-family:monospace">{current_price_display}</td>
                            <td style="font-family:monospace">{p.order_type}</td>
                            <td><span class="status-open">OPEN</span></td>
                            <td style="color: {pnl_color}; font-weight: bold;">{format_pnl(unrealized_pnl)} ({pnl_pct:+.1f}%)</td>
                        </tr>
                        """
                    
                    # Build table rows for closed positions
                    closed_rows = ""
                    for p in closed_positions:
                        pnl_color = get_pnl_color(p.pnl)
                        closed_rows += f"""
                        <tr>
                            <td style="font-family:monospace">{p.source_name}</td>
                            <td style="max-width:300px; overflow:hidden; text-overflow:ellipsis;">{p.question[:60]}</td>
                            <td style="color: {'#4ade80' if p.side == 'BUY' else '#f87171'}">{p.side}</td>
                            <td style="font-family:monospace">{p.outcome}</td>
                            <td style="font-family:monospace">${p.size_usd:.2f}</td>
                            <td style="font-family:monospace">{p.entry_price:.4f}</td>
                            <td style="font-family:monospace">{p.exit_price:.4f}</td>
                            <td style="font-family:monospace">{p.order_type}</td>
                            <td><span class="status-closed">CLOSED</span></td>
                            <td style="color: {pnl_color}; font-weight: bold;">{format_pnl(p.pnl)}</td>
                        </tr>
                        """
                    
                    # Wallet leaderboard rows
                    leaderboard_rows = ""
                    sorted_wallets = sorted(
                        wallet_stats.items(),
                        key=lambda x: (x[1]["profit_trades"], x[1]["total_pnl"]),
                        reverse=True
                    )
                    for rank, (wallet, stats) in enumerate(sorted_wallets, 1):
                        short_wallet = f"{wallet[:8]}..."
                        win_rate_display = f"{stats['win_rate']:.1f}%" if stats['win_rate'] > 0 else "—"
                        leaderboard_rows += f"""
                        <tr>
                            <td style="font-family:monospace">#{rank}</td>
                            <td style="font-family:monospace">{short_wallet}</td>
                            <td style="font-family:monospace">{stats['profit_trades']}</td>
                            <td style="font-family:monospace">{stats['loss_trades']}</td>
                            <td style="font-family:monospace">{win_rate_display}</td>
                            <td style="font-family:monospace; color:{'#4ade80' if stats['total_pnl'] >= 0 else '#f87171'}">${stats['total_pnl']:+.2f}</td>
                        </tr>
                        """
                    
                    pause_str = (
                        f"Paused until {bot_paused_until.strftime('%H:%M %d-%b')}"
                        if bot_paused_until and datetime.now() < bot_paused_until
                        else "Active"
                    )
                    
                    drawdown_pct = 0
                    if peak_bankroll > 0:
                        drawdown_pct = ((peak_bankroll - bankroll) / peak_bankroll) * 100
                    
                    html = f"""<!DOCTYPE html>
                    <html>
                    <head>
                        <meta charset="utf-8">
                        <title>CopyTrader Dashboard</title>
                        <meta http-equiv="refresh" content="30">
                        <style>
                            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 20px; background: #0a0a0a; color: #e0e0e0; }}
                            .container {{ max-width: 1400px; margin: 0 auto; }}
                            h1 {{ color: #ffffff; margin-bottom: 20px; font-size: 28px; border-left: 4px solid #6366f1; padding-left: 15px; }}
                            h2 {{ color: #ffffff; margin: 20px 0 15px 0; font-size: 20px; }}
                            .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px; }}
                            .stat-card {{ background: #1a1a1a; padding: 15px 20px; border-radius: 8px; border: 1px solid #2a2a2a; }}
                            .stat-label {{ font-size: 12px; text-transform: uppercase; color: #888; letter-spacing: 1px; margin-bottom: 8px; }}
                            .stat-value {{ font-size: 28px; font-weight: bold; }}
                            .stat-value.positive {{ color: #4ade80; }}
                            .stat-value.negative {{ color: #f87171; }}
                            .stat-value.warning {{ color: #fbbf24; }}
                            .section {{ background: #0f0f0f; border-radius: 8px; margin-bottom: 25px; border: 1px solid #1f1f1f; overflow: hidden; }}
                            .section-header {{ background: #1a1a1a; padding: 12px 20px; border-bottom: 1px solid #2a2a2a; font-weight: bold; font-size: 18px; }}
                            .section-header span {{ color: #6366f1; }}
                            table {{ width: 100%; border-collapse: collapse; }}
                            th {{ background: #141414; padding: 12px; text-align: left; font-size: 13px; font-weight: 600; color: #aaa; border-bottom: 1px solid #2a2a2a; }}
                            td {{ padding: 10px 12px; border-bottom: 1px solid #1f1f1f; font-size: 13px; }}
                            tr:hover {{ background: #151515; }}
                            .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
                            .badge-active {{ background: #064e3b; color: #4ade80; }}
                            .badge-paused {{ background: #7c2d12; color: #f97316; }}
                            .status-open {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; background: #1e3a5f; color: #60a5fa; }}
                            .status-closed {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; background: #3f3f46; color: #a1a1aa; }}
                            .footer {{ margin-top: 20px; text-align: center; color: #666; font-size: 12px; }}
                            hr {{ border: none; border-top: 1px solid #2a2a2a; margin: 20px 0; }}
                        </style>
                    </head>
                    <body>
                        <div class="container">
                            <h1>📊 Multi-Wallet Copy Trader (with Rust PnL)</h1>
                            
                            <div class="stats">
                                <div class="stat-card">
                                    <div class="stat-label">Mode</div>
                                    <div class="stat-value" style="font-size: 20px;">
                                        <span class="badge {'badge-active' if not bot.dry_run else ''}" style="background: {'#064e3b' if not bot.dry_run else '#3f3f46'}; color: {'#4ade80' if not bot.dry_run else '#a1a1aa'}">
                                            {'🔴 LIVE' if not bot.dry_run else '🟡 DRY RUN'}
                                        </span>
                                    </div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Bot Status</div>
                                    <div class="stat-value" style="font-size: 20px;">
                                        <span class="badge {'badge-active' if pause_str == 'Active' else 'badge-paused'}">
                                            {pause_str}
                                        </span>
                                    </div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Bankroll</div>
                                    <div class="stat-value">${bankroll:.2f}</div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Peak Bankroll</div>
                                    <div class="stat-value">${peak_bankroll:.2f}</div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Drawdown</div>
                                    <div class="stat-value {'warning' if drawdown_pct > 10 else ''}">{drawdown_pct:.1f}%</div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Open Positions</div>
                                    <div class="stat-value">{len(open_positions)}</div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Total PnL</div>
                                    <div class="stat-value {'positive' if total_pnl > 0 else 'negative' if total_pnl < 0 else ''}">
                                        {format_pnl(total_pnl)}
                                    </div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Unrealized PnL</div>
                                    <div class="stat-value {'positive' if total_open_pnl > 0 else 'negative' if total_open_pnl < 0 else ''}">
                                        {format_pnl(total_open_pnl)}
                                    </div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Realized PnL</div>
                                    <div class="stat-value {'positive' if total_closed_pnl > 0 else 'negative' if total_closed_pnl < 0 else ''}">
                                        {format_pnl(total_closed_pnl)}
                                    </div>
                                </div>
                            </div>
                            
                            <div class="section">
                                <div class="section-header">
                                    🏆 Wallet Leaderboard <span>(by wins → total P&L)</span>
                                </div>
                                <div style="overflow-x: auto;">
                                    <table>
                                        <thead>
                                            <tr><th>Rank</th><th>Wallet</th><th>Wins</th><th>Losses</th><th>Win Rate</th><th>Total P&amp;L</th></tr>
                                        </thead>
                                        <tbody>
                                            {leaderboard_rows if leaderboard_rows else '<tr><td colspan="6" style="text-align:center; padding:40px;">📭 No trades yet</td></tr>'}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                            
                            <div class="section">
                                <div class="section-header">
                                    📈 Open Positions <span>({len(open_positions)})</span>
                                </div>
                                <div style="overflow-x: auto;">
                                    <table>
                                        <thead>
                                            <tr>
                                                <th>Source</th><th>Market</th><th>Side</th><th>Outcome</th>
                                                <th>Size</th><th>Entry</th><th>Current</th>
                                                <th>Order</th><th>Status</th><th>PnL</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {open_rows if open_rows else '<tr><td colspan="10" style="text-align:center; padding:40px;">📭 No open positions</td></tr>'}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                            
                            <div class="section">
                                <div class="section-header">
                                    📉 Closed Positions <span>({len(closed_positions)})</span>
                                </div>
                                <div style="overflow-x: auto;">
                                    <table>
                                        <thead>
                                            <tr>
                                                <th>Source</th><th>Market</th><th>Side</th><th>Outcome</th>
                                                <th>Size</th><th>Entry</th><th>Exit</th>
                                                <th>Order</th><th>Status</th><th>PnL</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {closed_rows if closed_rows else '<tr><td colspan="10" style="text-align:center; padding:40px;">📭 No closed positions</td></tr>'}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                            
                            <div class="footer">
                                <hr>
                                <p>🔄 Auto-refresh every 30 seconds | 📍 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                                <p style="margin-top: 5px;">📐 PnL Formula: (size_usd / entry_price) × current_price − size_usd (Rust implementation)</p>
                            </div>
                        </div>
                    </body>
                    </html>"""
                    
                    self.wfile.write(html.encode('utf-8'))
                except Exception as exc:
                    self.wfile.write(f"<html><body><h2>Error</h2><pre>{exc}</pre></body></html>".encode('utf-8'))
            else:
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"OK")
        
        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()
        
        def log_message(self, format, *args):
            pass
    
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), Handler)
    logging.info(f"Dashboard on http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()


# ==================== ENTRY POINT ====================
async def main():
    global bot
    threading.Thread(target=run_dashboard, daemon=True).start()
    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    bot = None  # type: ignore
    asyncio.run(main())
