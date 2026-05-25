#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - Fixed Parsing + $1+ Copy Threshold
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
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "WalletA179", "risk_type": "fixed", "fixed_risk": 0.15},
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
CLOB_API_KEY     = os.getenv("POLY_API_KEY", "")
CLOB_SECRET      = os.getenv("POLY_SECRET", "")
CLOB_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")

BANKROLL_FALLBACK = float(os.getenv("BANKROLL", "0"))

MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.25"))
MAX_EXPOSURE       = 0.70
MAX_PER_TRADE      = 0.18
MIN_TRADE_SIZE     = 0.05
MIN_SOURCE_SIZE    = 1.0          # Copy positions >= $1
DAILY_LOSS_LIMIT   = 0.10         # 10% daily loss limit
HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 48

PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYGON_RPCS  = ["https://rpc.ankr.com/polygon", "https://polygon-bor-rpc.publicnode.com"]

peak_bankroll: float = BANKROLL_FALLBACK
bot_paused_until: Optional[datetime] = None
daily_start_balance: float = 0.0
daily_start_date: str = ""


# ==================== HTML DASHBOARD ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html><head><title>CopyTrader</title><meta http-equiv="refresh" content="15">
<style>body{font-family:Arial;background:#0a0a0a;color:#00cc00;margin:0;padding:20px;}
h1{color:#00ff00;text-align:center;}.card{background:#111;padding:20px;margin:15px 0;border-radius:10px;}
.green{color:#00ff88;}.red{color:#ff4444;}</style></head><body>
<div class="container"><h1>🤖 CopyTrader Dashboard</h1>
<div class="card"><h2>Status: <span style="color:{status_color}">{status}</span></h2>
<p><strong>Bankroll:</strong> ${bankroll:.2f} | Drawdown: <span class="{dd_class}">{drawdown:.1f}%</span></p>
<p><strong>Daily P&L:</strong> <span class="{daily_class}">${daily_pnl:.2f} ({daily_pct:.1f}%)</span></p>
<p>Positions: {open_pos}/{max_pos} | Exposure: ${exposure:.2f} ({exposure_pct:.1f}%)</p></div>
<div class="card"><h2>Open Positions</h2>{positions_table}</div></div></body></html>"""


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

        # Your RPC logic here (kept simple)
        return self.cached_balance


# ==================== EXECUTOR ====================
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs
    from py_clob_client.constants import POLYGON
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False


class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client = None
        # ... (your existing client init)

    async def place_buy(self, token_id: str, amount: float):
        if self.dry_run:
            logging.info(f"[DRY RUN] BUY ${amount:.2f}")
            return True, "dry"
        return False, ""

    async def place_sell(self, token_id: str, shares: float):
        if self.dry_run:
            logging.info(f"[DRY RUN] SELL {shares:.4f}")
            return True, "dry"
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
                        "title": p.get("title", "Unknown"),
                        "outcome": p.get("outcome", "YES"),
                        "value": value,
                    })
                return cleaned
        except Exception as e:
            logging.error(f"Fetch error {wallet_addr[:10]}: {e}")
            return None

    async def scan_and_copy(self):
        global bot_paused_until, daily_start_balance, daily_start_date, peak_bankroll

        async with aiohttp.ClientSession() as session:
            bankroll = await self.balance.get(session, force=True)
            if bankroll < 1.0:
                return

            if bankroll > peak_bankroll:
                peak_bankroll = bankroll

            # Daily loss 10%
            today = datetime.now().date().isoformat()
            if daily_start_date != today:
                daily_start_balance = bankroll
                daily_start_date = today
            if daily_start_balance > 0 and (bankroll - daily_start_balance) / daily_start_balance <= -DAILY_LOSS_LIMIT:
                return

            logging.info(f"Scanning | bankroll=${bankroll:.2f} | open={len(self.positions)}")

            for wallet_addr, config in WALLETS.items():
                raw = await self.get_positions(session, wallet_addr)
                if not raw:
                    continue

                for pos in raw:
                    token_id = pos["asset"]
                    question = pos["title"]
                    size_usd = pos["value"]
                    pos_key = f"{wallet_addr}_{token_id}"

                    if pos_key in self.positions or len(self.positions) >= MAX_POSITIONS:
                        continue

                    mid_price = 0.5  # placeholder - replace with real call if needed
                    my_size = round(bankroll * 0.15, 2)   # aggressive for small balance

                    if my_size < MIN_TRADE_SIZE:
                        my_size = MIN_TRADE_SIZE

                    ok, _ = await self.executor.place_buy(token_id, my_size)
                    if ok:
                        self.positions[pos_key] = Position(
                            market_id="", question=question, outcome=pos["outcome"],
                            token_id=token_id, entry_price=mid_price, size_usd=my_size,
                            shares=my_size/mid_price, source_wallet=wallet_addr,
                            source_name=config["name"]
                        )
                        logging.info(f"COPIED ${my_size:.2f} → {question[:50]}")

    async def run(self):
        logging.info("Bot started (Fixed Parsing + $1+ copy)")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# ==================== DASHBOARD SERVER ====================
def run_dashboard():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"Dashboard placeholder - OK")
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), Handler)
    server.serve_forever()


# ==================== ENTRY POINT ====================
async def main():
    threading.Thread(target=run_dashboard, daemon=True).start()
    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
