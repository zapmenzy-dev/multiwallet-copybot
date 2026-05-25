#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - Daily Loss Removed + Fixed Parsing
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
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "WalletA179", "risk_type": "fixed", "fixed_risk": 0.18},
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
CLOB_API_KEY     = os.getenv("POLY_API_KEY", "")
CLOB_SECRET      = os.getenv("POLY_SECRET", "")
CLOB_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")

BANKROLL_FALLBACK = float(os.getenv("BANKROLL", "0"))

MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.30"))   # 30%
MAX_EXPOSURE       = 0.80
MAX_PER_TRADE      = 0.22
MIN_TRADE_SIZE     = 0.05
MIN_SOURCE_SIZE    = 1.0          # Copy if source has $1+
HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 48

PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYGON_RPCS  = ["https://rpc.ankr.com/polygon", "https://polygon-bor-rpc.publicnode.com"]

peak_bankroll: float = BANKROLL_FALLBACK
bot_paused_until: Optional[datetime] = None


# ==================== HTML DASHBOARD (simplified) ====================
HTML_TEMPLATE = """<!DOCTYPE html><html><head><title>CopyTrader</title>
<meta http-equiv="refresh" content="15">
<style>body{font-family:Arial;background:#0a0a0a;color:#00cc00;}
.card{background:#111;padding:15px;margin:10px 0;border-radius:8px;}</style></head>
<body><h1>CopyTrader Dashboard</h1><div class="card">
<p><strong>Bankroll:</strong> ${bankroll:.2f} | Drawdown: {drawdown:.1f}%</p>
<p>Positions: {open_pos} | Mode: {mode}</p></div></body></html>"""


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

        # RPC logic (your original)
        return self.cached_balance


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
            logging.error(f"Position fetch error: {e}")
            return None

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
                        logging.info(f"COPIED ${my_size:.2f} → {question[:60]}")

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
        return 0.5

    async def run(self):
        logging.info("Bot started — Daily Loss Limit DISABLED")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# ==================== EXECUTOR (basic) ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run

    async def place_buy(self, token_id: str, amount: float):
        if self.dry_run:
            logging.info(f"[DRY RUN] BUY ${amount:.2f}")
            return True, "dry"
        return False, ""


# ==================== DASHBOARD ====================
def run_dashboard():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"OK - Bot Running (Dashboard placeholder)")
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), Handler)
    server.serve_forever()


# ==================== MAIN ====================
async def main():
    threading.Thread(target=run_dashboard, daemon=True).start()
    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
