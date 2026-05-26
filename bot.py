#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - FIXED VERSION
"""

import os
import json
import asyncio
import logging
import time
import threading
import re
from datetime import datetime, timedelta
from typing import Dict, Optional, Set
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

MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "9999"))
POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_EXPOSURE       = 0.80
STOP_LOSS          = 0.50
TRAIL_STOP         = 0.25
MAX_PER_TRADE      = 0.03
MIN_TRADE_FRAC     = 0.006
MAX_TRADE_FRAC     = 0.03
MIN_SOURCE_SIZE    = 1.0
LIMIT_ORDER_TICK   = 0.01

HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 24

# Valid token ID pattern (64 char hex)
VALID_TOKEN_PATTERN = re.compile(r'^0x[a-fA-F0-9]{64}$')

# Polymarket API endpoints
POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-rpc.com",
]

# FIX 1: Only track pUSD (free balance), not escrowed contracts
PUSD_CONTRACT = ("pUSD", "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB", 6)

BALANCE_OF_SELECTOR = "0x70a08231"

peak_bankroll: float = BANKROLL_FALLBACK
bot_paused_until: Optional[datetime] = None

# FIX 2: Track active orders
active_orders: Dict[str, dict] = {}


# ==================== DATA CLASS ====================
@dataclass
class Position:
    market_id: str
    question: str
    outcome: str
    token_id: str
    side: str
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
    order_id: str = ""  # Track order ID for cancellation


# ==================== BALANCE MANAGER (FIXED) ====================
class RobustBalanceManager:
    """
    Fetches your tradeable USDC balance - FIXED to avoid double-counting.
    Now only queries free pUSD balance (not escrowed funds).
    Uses Polymarket API as primary source since it provides true portfolio value.
    """

    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = 0

    async def _query_pusd_balance(
        self,
        session: aiohttp.ClientSession,
        rpc_url: str,
        wallet: str,
    ) -> Optional[float]:
        """Query free pUSD balance from your wallet"""
        try:
            padded = wallet.lower().replace("0x", "").zfill(64)
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": PUSD_CONTRACT[1], "data": BALANCE_OF_SELECTOR + padded}, "latest"],
            }
            async with session.post(
                rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                if "error" in data:
                    return None
                hex_val = data.get("result") or "0x0"
                if not hex_val or hex_val in ("0x", "0x0"):
                    return 0.0
                return int(hex_val, 16) / (10 ** PUSD_CONTRACT[2])
        except Exception:
            return None

    async def _fetch_polymarket_api(
        self, session: aiohttp.ClientSession, wallet: str
    ) -> Optional[float]:
        """Primary source - Polymarket portfolio value"""
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
                    logging.debug(f"Polymarket API balance: ${value:.4f}")
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

        # Primary: Polymarket API (gives true portfolio value)
        balance = await self._fetch_polymarket_api(session, YOUR_WALLET)

        # Fallback: RPC query for free pUSD only
        if balance is None:
            for rpc_url in POLYGON_RPCS:
                balance = await self._query_pusd_balance(session, rpc_url, YOUR_WALLET)
                if balance is not None:
                    logging.info(f"RPC free pUSD balance: ${balance:.4f} via {rpc_url}")
                    break

        if balance is not None:
            self.cached_balance = balance
            self.last_update = time.time()
            if balance > peak_bankroll:
                peak_bankroll = balance
            logging.info(f"Current bankroll: ${balance:.4f}")
        else:
            logging.warning("All balance sources failed — using cached $%.4f", self.cached_balance)

        return self.cached_balance

    def adjust(self, delta: float) -> None:
        self.cached_balance = max(0.0, self.cached_balance + delta)
        logging.debug(f"Balance adjusted by {delta:+.4f} → ${self.cached_balance:.4f}")


# ==================== EXECUTOR (FIXED) ====================
class PolymarketExecutor:
    """
    Places LIMIT orders on Polymarket CLOB with order tracking and cancellation.
    """

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

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an unfilled order"""
        if self.dry_run:
            logging.info(f"[DRY RUN] Cancel order {order_id}")
            return True

        if not self._client:
            return False

        try:
            resp = self._client.cancel_order(order_id)
            success = resp.get("success", False) if isinstance(resp, dict) else False
            if success:
                logging.info(f"Cancelled order {order_id}")
            return success
        except Exception as e:
            logging.error(f"Cancel order error: {e}")
            return False

    async def place_limit_buy(
        self,
        token_id: str,
        amount_usd: float,
        best_ask: float,
    ) -> tuple[bool, str]:
        """Place a GTC limit BUY order"""
        amount_usd = min(round(amount_usd, 2), 1.0)
        limit_price = round(max(best_ask - LIMIT_ORDER_TICK, 0.01), 4)
        shares = round(amount_usd / limit_price, 4)

        if self.dry_run:
            fake_id = f"dry-lmt-{int(time.time())}-{token_id[:8]}"
            logging.info(f"[DRY RUN] LIMIT BUY {shares:.4f} sh @ {limit_price:.4f} → {fake_id}")
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
                logging.info(f"[LIVE] Limit order live: {order_id} status={status}")
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
        """Close a position with a limit sell"""
        limit_price = round(min(price + LIMIT_ORDER_TICK, 0.99), 4)

        if self.dry_run:
            fake_id = f"dry-sell-{int(time.time())}-{token_id[:8]}"
            logging.info(f"[DRY RUN] LIMIT SELL {shares:.4f} sh @ {limit_price:.4f} → {fake_id}")
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


# ==================== COPY TRADER (FIXED) ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.positions: Dict[str, Position] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.balance = RobustBalanceManager()
        self.positions_being_closed: Set[str] = set()  # Prevent double-closing

    def _validate_token_id(self, token_id: str) -> bool:
        """Validate token ID format"""
        return bool(VALID_TOKEN_PATTERN.match(token_id))

    def _trade_size(self, bankroll: float, wallet_addr: str, mid_price: float) -> float:
        """Compute trade size in USD as a fraction of bankroll"""
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

    async def retry_with_backoff(self, func, max_retries=3) -> any:
        """Retry API calls with exponential backoff"""
        for i in range(max_retries):
            try:
                return await func()
            except Exception as e:
                if i == max_retries - 1:
                    raise
                wait_time = 2 ** i
                logging.debug(f"Retry {i+1}/{max_retries} after {wait_time}s: {e}")
                await asyncio.sleep(wait_time)

    async def get_positions(self, session: aiohttp.ClientSession, wallet_addr: str):
        """Fetch positions with retry logic"""
        async def _fetch():
            url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=100"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    logging.warning(f"positions API {r.status} for {wallet_addr[:10]}…")
                    return None
                data = await r.json()

                cleaned = []
                for p in (data if isinstance(data, list) else []):
                    token_id = p.get("asset")
                    if not token_id or not self._validate_token_id(token_id):
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
        
        try:
            return await self.retry_with_backoff(_fetch)
        except Exception as e:
            logging.error(f"Fetch positions error ({wallet_addr[:10]}…): {e}")
            return None

    async def get_orderbook(
        self, session: aiohttp.ClientSession, token_id: str
    ) -> tuple[float, float]:
        """Returns (best_bid, best_ask) with retry"""
        async def _fetch():
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bb = float(bids[0]["price"]) if bids else 0.0
                    ba = float(asks[0]["price"]) if asks else 0.0
                    return bb, ba
                return 0.0, 0.0
        
        try:
            return await self.retry_with_backoff(_fetch)
        except Exception as e:
            logging.warning(f"get_orderbook error for {token_id[:12]}…: {e}")
            return 0.0, 0.0

    async def get_mid_price(self, session: aiohttp.ClientSession, token_id: str) -> float:
        bb, ba = await self.get_orderbook(session, token_id)
        if bb and ba:
            return (bb + ba) / 2
        return bb or ba or 0.0

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

    async def scan_for_exits(self, session: aiohttp.ClientSession):
        """
        Close positions based on exit conditions.
        FIXED: Track which positions are being closed to prevent double-closing.
        """
        wallet_snapshot: dict[str, dict] = {}
        wallet_error: set[str] = set()

        # Fetch each source wallet once
        for wallet_addr in set(p.source_wallet for p in self.positions.values() if p.status == "open"):
            try:
                url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=500"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        logging.warning(f"scan_for_exits: API {r.status} for {wallet_addr[:10]}…")
                        wallet_error.add(wallet_addr)
                        continue
                    data = await r.json()
                    positions = data if isinstance(data, list) else []
                    wallet_snapshot[wallet_addr] = {p.get("asset"): p for p in positions if p.get("asset")}
            except Exception as e:
                logging.warning(f"scan_for_exits fetch error ({wallet_addr[:10]}…): {e}")
                wallet_error.add(wallet_addr)

        # Evaluate each open position
        to_close: list[tuple[str, Position, float, str]] = []

        for pos_key, pos in list(self.positions.items()):
            if pos.status != "open" or pos_key in self.positions_being_closed:
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

            reason = None

            if source_raw is None:
                reason = "source_exited"
            elif mid_price > 0 and mid_price <= pos.entry_price * (1 - STOP_LOSS):
                reason = f"stop_loss_50% (entry={pos.entry_price:.3f} now={mid_price:.3f})"
            elif mid_price > 0 and pos.peak_price > 0 and mid_price <= pos.peak_price * (1 - TRAIL_STOP):
                reason = f"trail_stop_25% (peak={pos.peak_price:.3f} now={mid_price:.3f})"

            if reason:
                to_close.append((pos_key, pos, mid_price or pos.entry_price, reason))

        # Execute closes
        for pos_key, pos, exit_price, reason in to_close:
            self.positions_being_closed.add(pos_key)
            try:
                # Cancel any existing order for this position
                if pos.order_id:
                    await self.executor.cancel_order(pos.order_id)
                
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
            finally:
                self.positions_being_closed.discard(pos_key)

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

                    # Validate slippage
                    expected_price = pos.get("price", mid_price)
                    if abs(mid_price - expected_price) / expected_price > 0.05:  # 5% max slippage
                        logging.warning(f"Skipping {question[:50]} - excessive slippage")
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
                            order_id=order_id,  # Store order ID
                        )
                        logging.info(
                            f"COPIED [{config['name']}] LIMIT {side} ${my_size:.2f} "
                            f"@ {mid_price:.3f} ({pos['outcome']}) → {question[:50]}"
                        )

    async def run(self):
        # Validate config on startup
        if not DRY_RUN:
            assert YOUR_PRIVATE_KEY, "PRIVATE_KEY not set"
            assert YOUR_WALLET, "DEPOSIT_WALLET_ADDRESS not set"
            assert CLOB_API_KEY and CLOB_SECRET, "API credentials missing"
            logging.info("Configuration validated")
        
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
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                try:
                    bankroll = bot.balance.cached_balance
                    open_pos = [p for p in bot.positions.values() if p.status == "open"]
                    
                    def _upnl(p):
                        if p.status == 'closed':
                            return p.pnl
                        price = p.current_price if p.current_price > 0 else p.entry_price
                        return (price - p.entry_price) * p.shares

                    rows = "".join(
                        f"<table>"
                        f"<td>{p.source_name}</td>"
                        f"<td>{p.question[:50]}</td>"
                        f"<td style='color:{'#4ade80' if p.side=='BUY' else '#f87171'}'>{p.side}</td>"
                        f"<td>{p.outcome}</td>"
                        f"<td>${p.size_usd:.2f}</td>"
                        f"<td>{p.entry_price:.3f}</td>"
                        f"<td>{p.order_type}</td>"
                        f"<td>{p.status}</td>"
                        f"<td style='color:{'#4ade80' if _upnl(p)>=0 else '#f87171'}'>${_upnl(p):+.2f}</td>"
                        f"</tr>"
                        for p in bot.positions.values()
                    )
                    pause_str = (
                        f"Paused until {bot_paused_until:%H:%M %d-%b}"
                        if bot_paused_until and datetime.now() < bot_paused_until
                        else "Running"
                    )
                    html = f"""<!doctype html><html><head>
                    <meta charset="utf-8">
                    <title>CopyTrader</title>
                    <meta http-equiv="refresh" content="30">
                    <style>
                      body{{font-family:monospace;padding:20px;background:#0d0d0d;color:#e0e0e0}}
                      table{{border-collapse:collapse;width:100%}}
                      th,td{{border:1px solid #333;padding:6px 10px;text-align:left}}
                      th{{background:#1a1a1a}}
                    </style>
                    </head><body>
                    <h2>CopyTrader — {pause_str}</h2>
                    <p>Mode: <b>{'LIVE' if not bot.dry_run else 'DRY RUN'}</b> &nbsp;|&nbsp;
                       Bankroll: <b>${bankroll:.4f}</b> &nbsp;|&nbsp;
                       Peak: <b>${peak_bankroll:.4f}</b> &nbsp;|&nbsp;
                       Open positions: <b>{len(open_pos)}</b></p>
                    <table>
                       <tr><th>Source</th><th>Market</th><th>Side</th>
                          <th>Outcome</th><th>Size</th><th>Entry</th>
                          <th>Order</th><th>Status</th><th>PnL</th></tr>
                      {rows if rows else "<tr><td colspan='9'>No positions</td></tr>"}
                    </table>
                    </body></html>"""
                    self.wfile.write(html.encode())
                except Exception as exc:
                    self.wfile.write(f"Error: {exc}".encode())
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
    asyncio.run(main())
