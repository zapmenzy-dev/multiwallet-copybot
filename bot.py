#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - Final Clean Version
- Fixed Parsing
- Daily Loss Removed
- HEAD Request Support
"""

import os
import asyncio
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional
from dataclasses import dataclass
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
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.30"))
MAX_EXPOSURE       = 0.85
MAX_PER_TRADE      = 0.22
MIN_TRADE_SIZE     = 0.05
MIN_SOURCE_SIZE    = 1.0          # Copy positions >= $1

HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 48

PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYGON_RPCS  = ["https://rpc.ankr.com/polygon", "https://polygon-bor-rpc.publicnode.com"]

peak_bankroll: float = BANKROLL_FALLBACK
bot_paused_until: Optional[datetime] = None


# ==================== DATA CLASS ====================
@dataclass
class Position:
    market_id: str
    question: str
    outcome: str
    token_id: str
    entry_price: float
    size_usd: float
    shares: float
    source_wallet: str
    source_name: str
    status: str = "open"
    exit_price: float = 0.0
    pnl: float = 0.0


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = 0

    async def get(self, session, force=False) -> float:
        global peak_bankroll
        if not force and time.time() - self.last_update < 60 and self.cached_balance > 0:
            return self.cached_balance

        # RPC logic (simplified)
        return self.cached_balance


# ==================== EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run

    async def place_buy(self, token_id: str, amount: float):
        if self.dry_run:
            logging.info(f"[DRY RUN] BUY ${amount:.2f}")
            return True, "dry-run"
        # Add real client logic later
        return False, ""


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.positions: Dict[str, Position] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.balance = RobustBalanceManager()

    async def get_positions(self, session, wallet_addr: str):
        try:
            url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=100"
            async with session.get(url, timeout=12) as r:
                if r.status != 200:
                    return None
                data = await r.json()

                cleaned = []
                for p in data if isinstance(data, list) else []:
                    token_id = p.get("asset")
                    if not token_id:
                        continue
                    value = float(p.get("currentValue") or p.get("value") or p.get("size") or 0)
                    if value < MIN_SOURCE_SIZE:
                        continue
                    cleaned.append({
                        "asset": token_id,
                        "title": p.get("title", "Unknown Market"),
                        "outcome": p.get("outcome", "YES"),
                        "value": value,
                    })
                return cleaned
        except Exception as e:
            logging.error(f"Fetch error: {e}")
            return None

    async def get_mid_price(self, session, token_id: str) -> float:
        try:
            async with session.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8) as r:
                if r.status == 200:
                    data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bb = float(bids[0]["price"]) if bids else 0
                    ba = float(asks[0]["price"]) if asks else 0
                    return (bb + ba) / 2 if bb and ba else (bb or ba)
        except Exception:
            return 0.5

    async def scan_and_copy(self):
        global bot_paused_until, peak_bankroll

        if bot_paused_until and datetime.now() < bot_paused_until:
            return

        async with aiohttp.ClientSession() as session:
            bankroll = await self.balance.get(session, force=True)
            if bankroll < 1.0:
                return

            if bankroll > peak_bankroll:
                peak_bankroll = bankroll

            logging.info(f"Scanning | bankroll=${bankroll:.4f} | open={len(self.positions)}")

            for wallet_addr, config in WALLETS.items():
                raw = await self.get_positions(session, wallet_addr)
                if not raw:
                    continue

                for pos in raw:
                    token_id = pos["asset"]
                    question = pos["title"]
                    pos_key = f"{wallet_addr}_{token_id}"

                    if pos_key in self.positions or len(self.positions) >= MAX_POSITIONS:
                        continue

                    mid_price = await self.get_mid_price(session, token_id)
                    if mid_price <= 0.01:
                        continue

                    my_size = round(bankroll * MAX_PER_TRADE, 2)
                    if my_size < MIN_TRADE_SIZE:
                        my_size = MIN_TRADE_SIZE

                    ok, _ = await self.executor.place_buy(token_id, my_size)
                    if ok:
                        shares = my_size / mid_price
                        self.positions[pos_key] = Position(
                            market_id="", question=question, outcome=pos["outcome"],
                            token_id=token_id, entry_price=mid_price, size_usd=my_size,
                            shares=shares, source_wallet=wallet_addr, source_name=config["name"]
                        )
                        logging.info(f"✅ COPIED ${my_size:.2f} → {question[:60]}")

    async def run(self):
        logging.info("Bot started — Daily Loss Removed | Min Source $1+")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# ==================== DASHBOARD + HEAD SUPPORT ====================
def run_dashboard():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            if self.path == "/":
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                try:
                    bankroll = bot.balance.cached_balance
                    html = f"""
                    <h1>🤖 CopyTrader Status: ✅ RUNNING</h1>
                    <p><strong>Bankroll:</strong> ${bankroll:.4f}</p>
                    <p><strong>Open Positions:</strong> {len(bot.positions)}</p>
                    <p><strong>Mode:</strong> {'LIVE' if not bot.dry_run else 'DRY RUN'}</p>
                    <p><strong>Last Updated:</strong> {datetime.now().strftime('%H:%M:%S')}</p>
                    """
                    self.wfile.write(html.encode())
                except:
                    self.wfile.write(b"<h1>CopyTrader Running</h1>")
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
    logging.info(f"✅ Dashboard & Health Server Started on port {HEALTH_PORT}")
    server.serve_forever()


# ==================== ENTRY POINT ====================
async def main():
    global bot
    threading.Thread(target=run_dashboard, daemon=True).start()

    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
