#!/usr/bin/env python3
"""
MULTI-WALLET POLYMARKET COPY TRADER - Fixed for Render
"""

import os
import json
import asyncio
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from dataclasses import dataclass, field   # ← FIXED: Added this import

import aiohttp
from dotenv import load_dotenv

load_dotenv()

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

BANKROLL_FALLBACK = float(os.getenv("BANKROLL", "0.0"))

MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "9999"))
POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "60"))
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

POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-rpc.com",
]
BALANCE_OF_SELECTOR = "0x70a08231"
PUSD_CONTRACTS = [
    ("pUSD",      "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB", 6),
    ("CTF-v2",    "0xE111180000d2663C0091e4f400237545B87B996B", 6),
    ("NegRisk-v2","0xe2222d279d744050d28e00520010520000310F59", 6),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self, cache_seconds: int = 60):
        self.cached_balance: float = BANKROLL_FALLBACK
        self.last_update: float = 0.0
        self.cache_seconds = cache_seconds
        self._breakdown: Dict[str, float] = {}
        self._last_source: str = "fallback"

    def _call_payload(self, contract: str, wallet: str) -> dict:
        padded = wallet.lower().replace("0x", "").zfill(64)
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": contract, "data": BALANCE_OF_SELECTOR + padded}, "latest"],
        }

    async def _query_contract(
        self, session: "aiohttp.ClientSession", rpc_url: str, label: str,
        contract: str, decimals: int, wallet: str
    ) -> Optional[float]:
        try:
            payload = self._call_payload(contract, wallet)
            async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                if "error" in data:
                    return None
                hex_val = data.get("result") or "0x0"
                if hex_val in ("0x", "0x0", None):
                    return 0.0
                return int(hex_val, 16) / (10 ** decimals)
        except Exception:
            return None

    async def _fetch_rpc(self, session: "aiohttp.ClientSession", wallet: str) -> Optional[float]:
        for rpc_url in POLYGON_RPCS:
            total = 0.0
            breakdown = {}
            for label, contract, decimals in PUSD_CONTRACTS:
                amount = await self._query_contract(session, rpc_url, label, contract, decimals, wallet)
                if amount is None:
                    continue
                if amount > 0:
                    breakdown[label] = amount
                    total += amount

            if breakdown or total == 0:
                self._breakdown = breakdown
                self._last_source = "RPC"
                logging.info(f"Balance → ${total:.4f} via RPC")
                return total
        return None

    async def _fetch_polymarket_api(self, session: "aiohttp.ClientSession", wallet: str) -> Optional[float]:
        try:
            url = f"https://data-api.polymarket.com/value?user={wallet}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                value = float(data) if isinstance(data, (int, float)) else float(
                    data.get("portfolioValue") or data.get("value") or data.get("balance") or 0)
                if value >= 0:
                    self._last_source = "Polymarket API"
                    logging.info(f"Balance → ${value:.4f} via API")
                    return value
        except Exception as e:
            logging.warning(f"API balance error: {e}")
        return None

    async def get(self, session: "aiohttp.ClientSession", force: bool = False) -> float:
        if not force and (time.time() - self.last_update) < self.cache_seconds:
            return self.cached_balance

        if not YOUR_WALLET:
            return self.cached_balance

        fetched = await self._fetch_rpc(session, YOUR_WALLET)
        if fetched is None:
            fetched = await self._fetch_polymarket_api(session, YOUR_WALLET)

        if fetched is not None:
            self.cached_balance = fetched
            self.last_update = time.time()

        return self.cached_balance

    def adjust(self, delta: float):
        self.cached_balance = max(0.0, self.cached_balance + delta)


# ==================== DATA CLASS ====================
@dataclass
class Position:
    question: str
    outcome: str
    token_id: str
    side: str
    entry_price: float
    size_usd: float
    shares: float
    source_wallet: str
    source_name: str
    status: str = "open"
    exit_price: float = 0.0
    pnl: float = 0.0
    peak_price: float = 0.0
    current_price: float = 0.0


# ==================== COPY TRADER (Minimal for deployment) ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.positions: Dict[str, Position] = {}
        self.balance = RobustBalanceManager()
        self.peak_bankroll: float = BANKROLL_FALLBACK
        self.bot_paused_until: Optional[datetime] = None

    async def run(self):
        logging.info(f"✅ CopyTrader started | Dry-run: {self.dry_run} | PORT: {HEALTH_PORT}")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def scan_and_copy(self):
        async with aiohttp.ClientSession() as session:
            bankroll = await self.balance.get(session, force=True)
            if bankroll > self.peak_bankroll:
                self.peak_bankroll = bankroll
            logging.info(f"Bankroll: ${bankroll:.2f} | Peak: ${self.peak_bankroll:.2f} | Positions: {len(self.positions)}")


# ==================== DASHBOARD ====================
def run_dashboard(bot: CopyTrader):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                data = {
                    "status": "running",
                    "dry_run": bot.dry_run,
                    "bankroll": round(bot.balance.cached_balance, 4),
                    "peak_bankroll": round(bot.peak_bankroll, 4),
                    "open_positions": len(bot.positions),
                    "port": HEALTH_PORT
                }
                self.wfile.write(json.dumps(data, indent=2).encode())
                return

            # Simple HTML
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            html = f"""
            <h1>Polymarket CopyTrader</h1>
            <p><strong>Status:</strong> Running</p>
            <p><strong>Mode:</strong> {'DRY RUN' if bot.dry_run else 'LIVE'}</p>
            <p><strong>Bankroll:</strong> ${bot.balance.cached_balance:.4f}</p>
            <p><strong>Peak:</strong> ${bot.peak_bankroll:.4f}</p>
            <p><strong>Open Positions:</strong> {len(bot.positions)}</p>
            """
            self.wfile.write(html.encode())

    server = HTTPServer(("0.0.0.0", HEALTH_PORT), Handler)
    logging.info(f"🌐 Dashboard running on http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()


# ==================== ENTRY POINT ====================
async def main():
    bot = CopyTrader(dry_run=DRY_RUN)
    threading.Thread(target=run_dashboard, args=(bot,), daemon=True).start()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
