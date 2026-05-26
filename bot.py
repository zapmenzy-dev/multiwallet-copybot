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

POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_EXPOSURE       = 0.80
STOP_LOSS          = 0.50
TRAIL_STOP         = 0.25
MIN_TRADE_FRAC     = 0.006
MAX_TRADE_FRAC     = 0.03
MIN_SOURCE_SIZE    = 1.0
LIMIT_ORDER_TICK   = 0.01

HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 24

POLYGON_RPCS  = ["https://polygon-bor-rpc.publicnode.com", "https://polygon-rpc.com"]
BALANCE_OF_SELECTOR = "0x70a08231"
PUSD_CONTRACTS = [
    ("pUSD", "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB", 6),
    ("CTF-v2", "0xE111180000d2663C0091e4f400237545B87B996B", 6),
    ("NegRisk-v2", "0xe2222d279d744050d28e00520010520000310F59", 6),
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


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = 0
        self._breakdown: dict[str, float] = {}

    def _call_payload(self, contract: str, wallet: str) -> dict:
        padded = wallet.lower().replace("0x", "").zfill(64)
        return {"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": contract, "data": BALANCE_OF_SELECTOR + padded}, "latest"]}

    async def _query_contract(self, session, rpc_url, label, contract, decimals, wallet):
        try:
            payload = self._call_payload(contract, wallet)
            async with session.post(rpc_url, json=payload, timeout=8) as resp:
                if resp.status != 200: return None
                data = await resp.json(content_type=None)
                hex_val = data.get("result") or "0x0"
                return int(hex_val, 16) / (10 ** decimals)
        except Exception:
            return None

    async def _fetch_rpc(self, session, wallet):
        for rpc_url in POLYGON_RPCS:
            total = 0.0
            for label, contract, decimals in PUSD_CONTRACTS:
                amount = await self._query_contract(session, rpc_url, label, contract, decimals, wallet)
                if amount and amount > 0:
                    total += amount
            if total > 0:
                return total
        return None

    async def _fetch_polymarket_api(self, session, wallet):
        try:
            url = f"https://data-api.polymarket.com/value?user={wallet}"
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return float(data.get("portfolioValue") or data.get("value") or 0)
        except Exception:
            return None

    async def get(self, session: aiohttp.ClientSession, force: bool = False) -> float:
        global peak_bankroll
        if not force and time.time() - self.last_update < 60:
            return self.cached_balance

        if not YOUR_WALLET:
            return self.cached_balance

        fetched = await self._fetch_rpc(session, YOUR_WALLET)
        if fetched is None:
            fetched = await self._fetch_polymarket_api(session, YOUR_WALLET)

        if fetched is not None:
            self.cached_balance = fetched
            self.last_update = time.time()
            if fetched > peak_bankroll:
                peak_bankroll = fetched
        return self.cached_balance

    def adjust(self, delta: float):
        self.cached_balance = max(0.0, self.cached_balance + delta)


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
            creds = ApiCreds(api_key=CLOB_API_KEY, api_secret=CLOB_SECRET, api_passphrase=CLOB_PASSPHRASE)
            self._client = ClobClient(host="https://clob.polymarket.com", key=YOUR_PRIVATE_KEY, chain_id=137, creds=creds)
            logging.info("CLOB client initialised")
        except Exception as e:
            logging.error(f"CLOB init failed: {e}")

    async def place_limit_buy(self, token_id: str, amount_usd: float, best_ask: float):
        amount_usd = min(round(amount_usd, 2), 1.0)
        limit_price = round(max(best_ask - LIMIT_ORDER_TICK, 0.01), 4)
        shares = round(amount_usd / limit_price, 4)

        if self.dry_run:
            logging.info(f"[DRY RUN] BUY ${amount_usd:.2f} @ {limit_price:.4f}")
            return True, "dry-run"
        return False, "live-not-implemented"

    async def place_sell(self, token_id: str, shares: float, price: float):
        if self.dry_run:
            logging.info(f"[DRY RUN] SELL {shares:.4f} shares")
            return True, "dry-run"
        return False, "live-not-implemented"


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.positions: Dict[str, Position] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.balance = RobustBalanceManager()

    async def update_all_positions_pnl(self, session: aiohttp.ClientSession):
        """Update prices and PnL for accurate dashboard"""
        for pos in list(self.positions.values()):
            if pos.status != "open":
                continue
            mid = await self.get_mid_price(session, pos.token_id)
            if mid > 0.01:
                pos.current_price = mid
                if pos.peak_price == 0 or mid > pos.peak_price:
                    pos.peak_price = mid
                if pos.side == "BUY":
                    pos.pnl = (mid - pos.entry_price) * pos.shares
                else:
                    pos.pnl = (pos.entry_price - mid) * pos.shares

    def _trade_size(self, bankroll: float, wallet_addr: str, mid_price: float) -> float:
        config = WALLETS[wallet_addr]
        if config.get("risk_type") == "price_based":
            fraction = max(0.05, min(mid_price, 1.0)) * MAX_TRADE_FRAC
        else:
            fraction = config.get("fixed_risk", MAX_TRADE_FRAC)

        fraction = max(MIN_TRADE_FRAC, min(fraction, MAX_TRADE_FRAC))
        size = round(bankroll * fraction, 2)
        headroom = max(0, bankroll * MAX_EXPOSURE - self._total_exposure())
        return round(min(size, headroom), 2)

    def _total_exposure(self) -> float:
        return sum(p.size_usd for p in self.positions.values() if p.status == "open")

    def _check_drawdown(self, bankroll: float) -> bool:
        global bot_paused_until
        if peak_bankroll <= 0: return False
        drawdown = (peak_bankroll - bankroll) / peak_bankroll
        if drawdown >= MAX_DRAWDOWN:
            bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
            logging.warning(f"Drawdown {drawdown:.1%} — bot paused for {PAUSE_HOURS}h")
            return True
        return False

    async def get_positions(self, session: aiohttp.ClientSession, wallet_addr: str):
        try:
            url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=100"
            async with session.get(url, timeout=12) as r:
                if r.status != 200: return None
                data = await r.json()
                cleaned = []
                for p in data if isinstance(data, list) else []:
                    token_id = p.get("asset")
                    if not token_id: continue
                    value = float(p.get("currentValue") or p.get("value") or 0)
                    if value < MIN_SOURCE_SIZE: continue
                    side = (p.get("side") or "").upper()
                    if side not in ("BUY", "SELL"):
                        side = "SELL" if value < 0 else "BUY"
                    cleaned.append({
                        "asset": token_id,
                        "title": p.get("title", "Unknown"),
                        "outcome": p.get("outcome", "YES"),
                        "side": side,
                        "value": abs(value),
                        "shares": float(p.get("size") or 0)
                    })
                return cleaned
        except Exception:
            return None

    async def get_orderbook(self, session: aiohttp.ClientSession, token_id: str):
        try:
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            async with session.get(url, timeout=8) as r:
                if r.status == 200:
                    data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bb = float(bids[0]["price"]) if bids else 0.0
                    ba = float(asks[0]["price"]) if asks else 0.0
                    return bb, ba
        except Exception:
            return 0.0, 0.0

    async def get_mid_price(self, session: aiohttp.ClientSession, token_id: str) -> float:
        bb, ba = await self.get_orderbook(session, token_id)
        if bb and ba: return (bb + ba) / 2
        return bb or ba or 0.0

    async def _execute_and_refresh(self, session, action, token_id, shares, size_usd, price, best_ask=0):
        if action == "BUY":
            ok, oid = await self.executor.place_limit_buy(token_id, size_usd, best_ask or price)
        else:
            ok, oid = await self.executor.place_sell(token_id, shares, price)
        if ok:
            delta = -size_usd if action == "BUY" else size_usd
            self.balance.adjust(delta)
        return ok, oid

    async def scan_for_exits(self, session):
        # Basic version - you can expand later
        for pos_key, pos in list(self.positions.items()):
            if pos.status != "open": continue
            mid = await self.get_mid_price(session, pos.token_id)
            if mid > 0:
                if mid <= pos.entry_price * (1 - STOP_LOSS) or mid <= pos.peak_price * (1 - TRAIL_STOP):
                    await self._execute_and_refresh(session, "SELL", pos.token_id, pos.shares, pos.size_usd, mid)
                    pos.status = "closed"
                    pos.exit_price = mid
                    logging.info(f"CLOSED {pos.question[:50]} | PnL: ${pos.pnl:+.2f}")

    async def scan_and_copy(self):
        global bot_paused_until, peak_bankroll

        if bot_paused_until and datetime.now() < bot_paused_until:
            logging.info("Bot is paused due to drawdown")
            return

        async with aiohttp.ClientSession() as session:
            bankroll = await self.balance.get(session, force=True)
            if bankroll < 1.0:
                return

            if bankroll > peak_bankroll:
                peak_bankroll = bankroll

            if self._check_drawdown(bankroll):
                return

            logging.info(f"Scanning | bankroll=${bankroll:.4f} | peak=${peak_bankroll:.4f} | open={len(self.positions)}")

            # Update PnL every cycle
            await self.update_all_positions_pnl(session)

            await self.scan_for_exits(session)

            for wallet_addr, config in WALLETS.items():
                raw = await self.get_positions(session, wallet_addr)
                if not raw: continue

                for p in raw:
                    token_id = p["asset"]
                    side = p["side"]
                    pos_key = f"{wallet_addr}_{token_id}_{side}"

                    if pos_key in self.positions: continue

                    best_bid, best_ask = await self.get_orderbook(session, token_id)
                    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
                    if mid_price <= 0.01: continue

                    my_size = self._trade_size(bankroll, wallet_addr, mid_price)
                    if my_size < 0.10: continue

                    shares = round(my_size / mid_price, 4)

                    ok, _ = await self._execute_and_refresh(session, side, token_id, shares, my_size, mid_price, best_ask)
                    if ok:
                        self.positions[pos_key] = Position(
                            market_id="", question=p["title"], outcome=p["outcome"], side=side,
                            token_id=token_id, entry_price=mid_price, size_usd=my_size, shares=shares,
                            source_wallet=wallet_addr, source_name=config["name"],
                            peak_price=mid_price, current_price=mid_price
                        )
                        logging.info(f"COPIED [{config['name']}] {side} ${my_size:.2f} @ {mid_price:.3f}")

    async def run(self):
        logging.info(f"Bot started | DRY_RUN={self.dry_run}")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Loop error: {e}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)


# ==================== DASHBOARD ====================
def run_dashboard():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            if self.path == "/":
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                try:
                    def _upnl(p):
                        return p.pnl if p.status == 'closed' else p.pnl

                    rows = "".join(
                        f"<tr><td>{p.source_name}</td><td>{p.question[:50]}</td><td>{p.side}</td>"
                        f"<td>{p.outcome}</td><td>${p.size_usd:.2f}</td><td>{p.entry_price:.3f}</td>"
                        f"<td>{p.status}</td><td style='color:{'#4ade80' if _upnl(p)>=0 else '#f87171'}'>${_upnl(p):+.2f}</td></tr>"
                        for p in bot.positions.values()
                    )

                    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>CopyTrader</title>
                    <meta http-equiv="refresh" content="30">
                    <style>body{{font-family:monospace;padding:20px;background:#0d0d0d;color:#e0e0e0}} table{{width:100%;border-collapse:collapse}} th,td{{border:1px solid #444;padding:8px}}</style>
                    </head><body>
                    <h2>Multi-Wallet CopyTrader</h2>
                    <p>Bankroll: <b>${bot.balance.cached_balance:.4f}</b> | Peak: <b>${peak_bankroll:.4f}</b> | Open: <b>{len(bot.positions)}</b></p>
                    <table><tr><th>Source</th><th>Market</th><th>Side</th><th>Outcome</th><th>Size</th><th>Entry</th><th>Status</th><th>PnL</th></tr>
                    {rows if rows else "<tr><td colspan='8'>No positions yet</td></tr>"}
                    </table></body></html>"""
                    self.wfile.write(html.encode())
                except Exception as e:
                    self.wfile.write(f"Error: {e}".encode())
            else:
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"OK")

    server = HTTPServer(("0.0.0.0", HEALTH_PORT), Handler)
    logging.info(f"Dashboard running on port {HEALTH_PORT}")
    server.serve_forever()


# ==================== ENTRY POINT ====================
async def main():
    global bot
    threading.Thread(target=run_dashboard, daemon=True).start()
    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
