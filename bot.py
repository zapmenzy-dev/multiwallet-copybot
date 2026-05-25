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

logging.basicConfig(
    level=logging.INFO,
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

MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.20"))   # 20%
MAX_EXPOSURE       = 0.85
MAX_PER_TRADE      = 0.03                                        # 3% per trade
MIN_TRADE_SIZE     = 1.0                                         # $1 minimum
MIN_SOURCE_SIZE    = 1.0                                         # only copy source trades ≥ $1

HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 24                                          # pause 24h on drawdown

# pUSD is Polymarket's collateral token (ERC-20, 6 decimals) on Polygon.
# Confirmed contract: 0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB
# We also check the CTF Exchange escrow for funds locked mid-trade.
POLYGON_RPCS  = ["https://rpc.ankr.com/polygon", "https://polygon-bor-rpc.publicnode.com"]
BALANCE_OF_SELECTOR = "0x70a08231"
USDC_CONTRACTS = [
    ("pUSD",             "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB", 6),  # Polymarket collateral ✓
    ("CTF-Exchange",     "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", 6),  # trading escrow
    ("NegRisk-Exchange", "0xC5d563A36AE78145C45a50134d48A1215220f80a", 6),  # multi-outcome escrow
    ("USDC.e",           "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", 6),  # legacy fallback
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
    status: str = "open"
    exit_price: float = 0.0
    pnl: float = 0.0
    opened_at: datetime = field(default_factory=datetime.now)


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    """
    Fetches your tradeable USDC balance using three strategies in order:

    1. RPC batch call — checks USDC.e, native USDC, CTF Exchange, and
       NegRisk Exchange balances in a single eth_call batch per RPC node.
       Sums all non-zero results (your funds may be split across contracts).

    2. Polymarket data API — queries /value endpoint which returns your
       portfolio value as Polymarket sees it. Good fallback if RPC is flaky.

    3. Cached value — last successful read, or BANKROLL_FALLBACK from .env.
    """

    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = 0
        self._breakdown: dict[str, float] = {}   # label → amount for logging

    # ── RPC helpers ──────────────────────────────────────────────────────────

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
        """Single balanceOf call. Returns None on network error, 0.0 on zero balance."""
        try:
            async with session.post(
                rpc_url,
                json=self._call_payload(contract, wallet),
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                hex_val = data.get("result") or "0x0"
                if not hex_val or hex_val == "0x":
                    return 0.0
                return int(hex_val, 16) / (10 ** decimals)
        except Exception as e:
            logging.debug(f"  {label} @ {rpc_url}: {e}")
            return None

    async def _fetch_rpc(self, session: aiohttp.ClientSession, wallet: str) -> Optional[float]:
        """
        Query each contract individually (no batching -- more compatible with
        public nodes). Tries RPC nodes in order; stops at first responsive node.
        Returns the summed balance, 0.0 for a valid empty wallet, or None if
        all nodes are unreachable.
        """
        for rpc_url in POLYGON_RPCS:
            total = 0.0
            breakdown: dict[str, float] = {}
            rpc_alive = False

            for label, contract, decimals in USDC_CONTRACTS:
                amount = await self._query_contract(
                    session, rpc_url, label, contract, decimals, wallet
                )
                if amount is None:
                    continue  # this contract call failed, try next
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

        return None  # all RPC nodes unreachable

    # ── Polymarket API fallback ───────────────────────────────────────────────

    async def _fetch_polymarket_api(
        self, session: aiohttp.ClientSession, wallet: str
    ) -> Optional[float]:
        """
        Query Polymarket's data API for the wallet's portfolio value.
        This reflects deposited + unrealised value — a reasonable proxy for
        available balance when RPC can't reach the chain.
        """
        try:
            url = f"https://data-api.polymarket.com/value?user={wallet}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                # Response: {"portfolioValue": 123.45} or a float directly
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

    # ── Public interface ──────────────────────────────────────────────────────

    async def get(self, session: aiohttp.ClientSession, force: bool = False) -> float:
        global peak_bankroll

        cache_age = time.time() - self.last_update
        if not force and cache_age < 60 and self.cached_balance > 0:
            return self.cached_balance

        if not YOUR_WALLET:
            logging.warning("DEPOSIT_WALLET_ADDRESS not set — using cached/fallback balance")
            return self.cached_balance

        # Strategy 1: RPC batch
        fetched = await self._fetch_rpc(session, YOUR_WALLET)

        # Strategy 2: Polymarket API (if RPC totally failed)
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
        """
        Instantly adjust the cached balance by delta (negative for spend,
        positive for proceeds). Called right after each trade so sizing
        decisions don't need to wait for the next RPC round-trip.
        """
        self.cached_balance = max(0.0, self.cached_balance + delta)
        logging.debug(f"Balance adjusted by {delta:+.4f} → ${self.cached_balance:.4f}")


# ==================== EXECUTOR ====================
class PolymarketExecutor:
    """
    Places limit orders on Polymarket CLOB.

    In DRY_RUN mode every call succeeds without hitting the network.
    In live mode it calls the CLOB /order endpoint with a signed payload.
    Requires py-clob-client: pip install py-clob-client
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

    async def place_buy(
        self,
        token_id: str,
        amount: float,
        price: float,
        slippage: float = 0.02,
    ) -> tuple[bool, str]:
        """
        Buy `amount` USD worth of `token_id` at up to `price * (1 + slippage)`.
        Returns (success, order_id_or_reason).
        """
        if self.dry_run:
            fake_id = f"dry-{int(time.time())}-{token_id[:8]}"
            logging.info(f"[DRY RUN] BUY ${amount:.2f} @ {price:.4f} → {fake_id}")
            return True, fake_id

        if not self._client:
            return False, "client_not_initialised"

        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType

            worst_price = round(min(price * (1 + slippage), 0.99), 4)
            shares = round(amount / price, 4)

            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=shares,
            )
            signed_order = self._client.create_market_order(order_args)
            resp = self._client.post_order(signed_order, OrderType.FOK)

            order_id = resp.get("orderID", "") or resp.get("id", "")
            status    = resp.get("status", "unknown")

            if status in ("matched", "live"):
                logging.info(f"[LIVE] Order placed: {order_id} status={status}")
                return True, order_id
            else:
                logging.warning(f"[LIVE] Order rejected: {resp}")
                return False, status

        except Exception as e:
            logging.error(f"place_buy error: {e}")
            return False, str(e)

    async def place_sell(
        self,
        token_id: str,
        shares: float,
        price: float,
    ) -> tuple[bool, str]:
        """Market-sell `shares` of `token_id`."""
        if self.dry_run:
            fake_id = f"dry-sell-{int(time.time())}-{token_id[:8]}"
            logging.info(f"[DRY RUN] SELL {shares:.4f} shares @ {price:.4f} → {fake_id}")
            return True, fake_id

        if not self._client:
            return False, "client_not_initialised"

        try:
            from py_clob_client.clob_types import MarketOrderArgs

            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=shares,
                side="SELL",
            )
            signed_order = self._client.create_market_order(order_args)
            resp = self._client.post_order(signed_order)

            order_id = resp.get("orderID", "") or resp.get("id", "")
            status    = resp.get("status", "unknown")

            if status in ("matched", "live"):
                logging.info(f"[LIVE] Sell placed: {order_id} status={status}")
                return True, order_id
            else:
                logging.warning(f"[LIVE] Sell rejected: {resp}")
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

    # -------- helpers --------

    def _trade_size(self, bankroll: float, wallet_addr: str, mid_price: float) -> float:
        """
        Compute trade size based on wallet's risk_type.
        - price_based: risk fraction scales inversely with price
          (cheap tokens = smaller bet; expensive tokens = larger bet)
        - fixed: always use fixed_risk fraction of bankroll
        """
        config = WALLETS[wallet_addr]
        risk_type = config.get("risk_type", "fixed")

        if risk_type == "price_based":
            # Scale between MIN_TRADE_SIZE and MAX_PER_TRADE based on price
            # High price (near 1.0) → MAX_PER_TRADE; low price (near 0) → small
            fraction = max(0.05, min(mid_price, 1.0)) * MAX_PER_TRADE
        else:
            fraction = config.get("fixed_risk", MAX_PER_TRADE)

        fraction = min(fraction, MAX_PER_TRADE)
        size = round(bankroll * fraction, 2)
        return max(size, MIN_TRADE_SIZE)

    def _total_exposure(self) -> float:
        return sum(p.size_usd for p in self.positions.values() if p.status == "open")

    def _check_drawdown(self, bankroll: float) -> bool:
        """Returns True if drawdown limit breached and bot should pause."""
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
        """
        Returns list of dicts with keys:
          asset, title, outcome, side ("BUY"/"SELL"), value, shares
        Only positions where source value ≥ MIN_SOURCE_SIZE ($1).
        """
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

                    value = float(
                        p.get("currentValue") or p.get("value") or p.get("size") or 0
                    )
                    if value < MIN_SOURCE_SIZE:
                        continue

                    # Detect side: Polymarket returns "side" on some endpoints;
                    # fall back to inferring from outcome name or default to BUY.
                    raw_side = (p.get("side") or "").upper()
                    if raw_side in ("BUY", "SELL"):
                        side = raw_side
                    else:
                        # Negative currentValue or "NO" outcome held by a YES market
                        # often signals a short/sell. Treat negatives as SELL.
                        side = "SELL" if value < 0 else "BUY"

                    shares = float(p.get("size") or p.get("shares") or 0)

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

    async def get_mid_price(self, session: aiohttp.ClientSession, token_id: str) -> float:
        try:
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bb = float(bids[0]["price"]) if bids else 0.0
                    ba = float(asks[0]["price"]) if asks else 0.0
                    if bb and ba:
                        return (bb + ba) / 2
                    return bb or ba or 0.0
        except Exception as e:
            logging.warning(f"get_mid_price error for {token_id[:12]}…: {e}")
        return 0.0

    # -------- source wallet tracking --------

    async def _get_source_position(
        self,
        session: aiohttp.ClientSession,
        wallet_addr: str,
        token_id: str,
    ) -> Optional[dict]:
        """
        Return the source wallet's current position dict for token_id, or None if exited.
        Network errors return a sentinel so we don't accidentally force-exit on flaky APIs.
        """
        positions = await self.get_positions(session, wallet_addr)
        if positions is None:
            return {"_network_error": True}  # keep position on network failure
        return next((p for p in positions if p["asset"] == token_id), None)

    # -------- exit scanner --------

    async def _execute_and_refresh(
        self,
        session: aiohttp.ClientSession,
        action: str,           # "BUY" or "SELL"
        token_id: str,
        shares: float,
        size_usd: float,
        price: float,
    ) -> tuple[bool, str]:
        """
        Execute a buy or sell, then update the balance in two steps:
          1. Instant local adjust (so next trade sizes correctly without waiting)
          2. Async RPC confirm (overwrites with on-chain truth)
        """
        if action == "BUY":
            ok, oid = await self.executor.place_buy(token_id, size_usd, price)
        else:
            ok, oid = await self.executor.place_sell(token_id, shares, price)

        if ok:
            # Step 1: immediately reflect spend/proceeds in cache
            delta = -size_usd if action == "BUY" else size_usd
            self.balance.adjust(delta)

            # Step 2: confirm with on-chain RPC (non-blocking best-effort)
            asyncio.ensure_future(self.balance.get(session, force=True))

        return ok, oid

    # -------- exit scanner --------

    async def scan_for_exits(self, session: aiohttp.ClientSession):
        """
        For each open position:
        1. If source exited → we sell (close).
        2. If source flipped side (BUY→SELL or SELL→BUY) → close our position.
        3. Price-based stops: take-profit at 2× entry, stop-loss below 0.10.
        After each close, balance is immediately refreshed via RPC.
        """
        to_close: list[tuple[str, Position, float, str]] = []

        for pos_key, pos in list(self.positions.items()):
            if pos.status != "open":
                continue

            mid_price    = await self.get_mid_price(session, pos.token_id)
            source_pos   = await self._get_source_position(session, pos.source_wallet, pos.token_id)

            reason = None

            if source_pos and source_pos.get("_network_error"):
                pass  # skip — don't close on flaky network
            elif source_pos is None:
                reason = "source_exited"
            elif source_pos["side"] != pos.side:
                reason = f"source_flipped ({pos.side}→{source_pos['side']})"
            elif mid_price > 0 and mid_price >= pos.entry_price * 2.0:
                reason = f"take_profit (2× @ {mid_price:.3f})"
            elif mid_price > 0 and mid_price < 0.10:
                reason = f"stop_loss (price={mid_price:.3f})"

            if reason:
                to_close.append((pos_key, pos, mid_price or pos.entry_price, reason))

        for pos_key, pos, exit_price, reason in to_close:
            # Always sell/close regardless of original side
            ok, _ = await self._execute_and_refresh(
                session, "SELL", pos.token_id, pos.shares, pos.size_usd, exit_price
            )
            if ok:
                pos.status     = "closed"
                pos.exit_price = exit_price
                pos.pnl        = (exit_price - pos.entry_price) * pos.shares
                logging.info(
                    f"CLOSED [{reason}] {pos.question[:50]} | "
                    f"side={pos.side} entry={pos.entry_price:.3f} "
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

            # --- check exits / flips first ---
            await self.scan_for_exits(session)

            # --- copy new positions (buys AND sells) ---
            for wallet_addr, config in WALLETS.items():
                raw = await self.get_positions(session, wallet_addr)
                if not raw:
                    continue

                for pos in raw:
                    token_id = pos["asset"]
                    question = pos["title"]
                    side     = pos["side"]        # "BUY" or "SELL"
                    pos_key  = f"{wallet_addr}_{token_id}_{side}"

                    # Skip already-tracked (same token + same side)
                    if pos_key in self.positions:
                        continue

                    # Source trade must be ≥ $1
                    if pos["value"] < MIN_SOURCE_SIZE:
                        continue

                    open_count = sum(1 for p in self.positions.values() if p.status == "open")
                    if open_count >= MAX_POSITIONS:
                        logging.info("Max positions reached — skipping remaining")
                        break

                    mid_price = await self.get_mid_price(session, token_id)
                    if mid_price <= 0.01:
                        logging.debug(f"Skipping {token_id[:12]}… — no price")
                        continue

                    my_size = self._trade_size(bankroll, wallet_addr, mid_price)

                    # Hard floor: never trade below $1
                    if my_size < MIN_TRADE_SIZE:
                        logging.debug(f"Skipping — computed size ${my_size:.2f} < $1")
                        continue

                    # Exposure guard (only counts for BUY side)
                    if side == "BUY" and self._total_exposure() + my_size > bankroll * MAX_EXPOSURE:
                        logging.info(f"Exposure cap — skipping {question[:40]}")
                        continue

                    shares = my_size / mid_price

                    ok, order_id = await self._execute_and_refresh(
                        session, side, token_id, shares, my_size, mid_price
                    )

                    if ok:
                        self.positions[pos_key] = Position(
                            market_id    = "",
                            question     = question,
                            outcome      = pos["outcome"],
                            side         = side,
                            token_id     = token_id,
                            entry_price  = mid_price,
                            size_usd     = my_size,
                            shares       = shares,
                            source_wallet= wallet_addr,
                            source_name  = config["name"],
                        )
                        logging.info(
                            f"COPIED [{config['name']}] {side} ${my_size:.2f} "
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
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                try:
                    bankroll  = bot.balance.cached_balance
                    open_pos  = [p for p in bot.positions.values() if p.status == "open"]
                    rows = "".join(
                        f"<tr>"
                        f"<td>{p.source_name}</td>"
                        f"<td>{p.question[:50]}</td>"
                        f"<td style='color:{'#4ade80' if p.side=='BUY' else '#f87171'}'>{p.side}</td>"
                        f"<td>{p.outcome}</td>"
                        f"<td>${p.size_usd:.2f}</td>"
                        f"<td>{p.entry_price:.3f}</td>"
                        f"<td>{p.status}</td>"
                        f"<td style='color:{'#4ade80' if p.pnl>=0 else '#f87171'}'>${p.pnl:+.2f}</td>"
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
                          <th>Outcome</th><th>Size</th><th>Entry</th><th>Status</th><th>PnL</th></tr>
                      {rows if rows else "<tr><td colspan='8'>No positions</td></tr>"}
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
