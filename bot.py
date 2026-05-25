#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - Fixed Parsing + $1+ Copy
"""

import os
import asyncio
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
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
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "WalletA179", "risk_type": "fixed", "fixed_risk": 0.15},  # Increased for small bankroll
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
MIN_SOURCE_SIZE    = 1.0          # ← Changed to $1 as requested
DAILY_LOSS_LIMIT   = 0.10         # ← Changed to 10%
HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 48
MAX_RETRIES        = 3

PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYGON_RPCS  = ["https://rpc.ankr.com/polygon", "https://polygon-bor-rpc.publicnode.com"]

peak_bankroll: float = BANKROLL_FALLBACK
bot_paused_until: Optional[datetime] = None
daily_start_balance: float = 0.0
daily_start_date: str = ""


# ==================== DATA CLASSES & HTML (unchanged) ====================
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
    order_id: str = ""


# ==================== BALANCE MANAGER (unchanged) ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = 0

    async def get(self, session, force=False) -> float:
        global peak_bankroll
        if not force and time.time() - self.last_update < 60 and self.cached_balance > 0:
            return self.cached_balance

        # ... (keep your existing RPC logic)
        # I'll keep it short here - assume it's working
        return self.cached_balance


# ==================== EXECUTOR (unchanged) ====================
# ... (your existing PolymarketExecutor class)


# ==================== COPY TRADER - FIXED ====================
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
                logging.info(f"[DEBUG] {wallet_addr[:10]}... positions status={r.status}")
                if r.status != 200:
                    return None

                data = await r.json()
                if not isinstance(data, list):
                    return None

                cleaned = []
                for p in data:
                    token_id = p.get("asset") or p.get("tokenId")
                    if not token_id:
                        continue

                    # === FIXED PARSING ===
                    value = float(
                        p.get("currentValue") or 
                        p.get("value") or 
                        p.get("size") or 
                        p.get("amount") or 0
                    )

                    if value < MIN_SOURCE_SIZE:
                        continue

                    cleaned.append({
                        "asset": token_id,
                        "title": p.get("title") or p.get("question", "Unknown"),
                        "outcome": p.get("outcome", "YES"),
                        "value": value,
                        "price": float(p.get("price") or p.get("curPrice") or 0),
                    })

                logging.info(f"✅ Parsed {len(cleaned)} positions ≥ ${MIN_SOURCE_SIZE} from {wallet_addr[:10]}...")
                return cleaned

        except Exception as e:
            logging.error(f"Error fetching positions: {e}")
            return None

    # ... keep your other methods (get_mid_price, get_ask_depth, get_risk_percent, etc.)

    async def scan_and_copy(self):
        global bot_paused_until, daily_start_balance, daily_start_date, peak_bankroll

        if bot_paused_until and datetime.now() < bot_paused_until:
            return

        async with aiohttp.ClientSession() as session:
            bankroll = await self.balance.get(session, force=True)
            if bankroll < 1.0:
                logging.warning("Bankroll too low")
                return

            # Update peak
            if bankroll > peak_bankroll:
                peak_bankroll = bankroll

            # Daily loss (10%)
            today = datetime.now().date().isoformat()
            if daily_start_date != today:
                daily_start_balance = bankroll
                daily_start_date = today

            daily_loss = (bankroll - daily_start_balance) / daily_start_balance if daily_start_balance > 0 else 0
            if daily_loss <= -DAILY_LOSS_LIMIT:
                logging.warning(f"Daily loss limit (10%) hit — skipping trades")
                return

            logging.info(f"Scanning | bankroll=${bankroll:.4f} | open={len(self.positions)}")

            for wallet_addr, config in WALLETS.items():
                raw = await self.get_positions(session, wallet_addr)
                if not raw:
                    continue

                source_token_ids = {pos["asset"] for pos in raw}

                # BUY Logic
                for pos in raw:
                    token_id = pos["asset"]
                    question = pos["title"]
                    size_usd = pos["value"]
                    pos_key = f"{wallet_addr}_{token_id}"

                    if pos_key in self.positions or len(self.positions) >= MAX_POSITIONS:
                        continue

                    mid_price = await self.get_mid_price(session, token_id)
                    if mid_price <= 0.01:
                        continue

                    risk_pct = self.get_risk_percent(mid_price, config)
                    my_size = round(bankroll * risk_pct, 2)
                    if my_size < MIN_TRADE_SIZE:
                        my_size = MIN_TRADE_SIZE

                    ok, order_id = await self.executor.place_buy(token_id, my_size)
                    if ok:
                        shares = my_size / mid_price
                        self.positions[pos_key] = Position(
                            market_id="", question=question, outcome=pos["outcome"],
                            token_id=token_id, entry_price=mid_price, size_usd=my_size,
                            shares=shares, source_wallet=wallet_addr,
                            source_name=config["name"], order_id=order_id
                        )
                        logging.info(f"COPIED {config['name']} | ${my_size:.2f} on {question[:50]}")

                # SELL Logic (unchanged)
                # ... keep your existing sell logic

    async def run(self):
        logging.info("Bot started with fixed parsing + $1+ copy threshold")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# ==================== ENTRY POINT ====================
async def main():
    # Dashboard thread...
    threading.Thread(target=run_dashboard, daemon=True).start()   # Keep your dashboard

    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
