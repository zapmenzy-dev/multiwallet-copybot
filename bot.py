#!/usr/bin/env python3
"""
MULTI-WALLET POLYMARKET COPY TRADER - FINAL LIVE VERSION
"""

import os
import json
import asyncio
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from dataclasses import dataclass, field

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

POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "60"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_EXPOSURE       = 0.80
STOP_LOSS          = 0.50
TRAIL_STOP         = 0.25
MIN_TRADE_FRAC     = 0.006
MAX_TRADE_FRAC     = 0.03
MIN_SOURCE_SIZE    = 1.0
LIMIT_ORDER_TICK   = 0.01

# Render.com compatibility
HEALTH_PORT = int(os.getenv("PORT", "8080"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


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
    opened_at: datetime = field(default_factory=datetime.now)


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = 0

    async def get(self, session: "aiohttp.ClientSession", force: bool = False) -> float:
        if not force and time.time() - self.last_update < 60:
            return self.cached_balance

        if not YOUR_WALLET:
            return self.cached_balance

        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = time.time()
        return self.cached_balance


# ==================== EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client = None
        if not dry_run:
            self._init_client()

    def _init_client(self):
        try:
            from py_clob_client_v2 import ClobClient, OrderArgs, OrderType, Side
            self.Side = Side
            self.OrderType = OrderType
            self.OrderArgs = OrderArgs

            self.client = ClobClient(
                host="https://clob.polymarket.com",
                key=YOUR_PRIVATE_KEY,
                chain_id=137
            )
            logging.info("✅ CLOB Client initialized for LIVE trading")
        except Exception as e:
            logging.error(f"Failed to initialize CLOB client: {e}")

    async def place_order(self, token_id: str, side: str, size_usd: float, price: float) -> tuple[bool, str]:
        size_usd = min(round(size_usd, 2), 0.99)

        if self.dry_run:
            logging.info(f"[DRY RUN] {side} ${size_usd:.2f} @ {price:.4f}")
            return True, "dry-run-success"

        if not self.client:
            return False, "client_not_initialized"

        try:
            limit_price = round(price - LIMIT_ORDER_TICK if side == "BUY" else price + LIMIT_ORDER_TICK, 4)
            shares = round(size_usd / limit_price, 4)

            order_args = self.OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side=self.Side.BUY if side == "BUY" else self.Side.SELL
            )

            resp = self.client.create_and_post_order(order_args, order_type=self.OrderType.GTC)
            order_id = resp.get("orderID") or resp.get("id", "unknown")
            logging.info(f"✅ LIVE ORDER PLACED | {side} ${size_usd:.2f} @ {limit_price:.4f} | ID: {order_id}")
            return True, order_id
        except Exception as e:
            logging.error(f"Order placement failed: {e}")
            return False, str(e)


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.positions: Dict[str, Position] = {}
        self.balance = RobustBalanceManager()
        self.executor = PolymarketExecutor(dry_run)
        self.peak_bankroll = BANKROLL_FALLBACK

    def _position_key(self, wallet: str, token_id: str) -> str:
        return f"{wallet}_{token_id}"

    def _trade_size(self, bankroll: float, source_wallet: str, price: float) -> float:
        config = WALLETS.get(source_wallet, {})
        if config.get("risk_type") == "price_based":
            fraction = max(0.05, min(price, 1.0)) * MAX_TRADE_FRAC
        else:
            fraction = config.get("fixed_risk", MAX_TRADE_FRAC)

        fraction = max(MIN_TRADE_FRAC, min(fraction, MAX_TRADE_FRAC))
        size = bankroll * fraction

        exposure = sum(p.size_usd for p in self.positions.values() if p.status == "open")
        headroom = bankroll * MAX_EXPOSURE - exposure
        return round(min(size, headroom, 0.99), 2)

    async def get_source_positions(self, session: aiohttp.ClientSession, wallet: str) -> List[dict]:
        try:
            url = f"https://data-api.polymarket.com/positions?user={wallet}&limit=100"
            async with session.get(url, timeout=12) as r:
                if r.status != 200:
                    return []
                data = await r.json()
                return data if isinstance(data, list) else []
        except Exception:
            return []

    async def get_mid_price(self, session: aiohttp.ClientSession, token_id: str) -> float:
        try:
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            async with session.get(url, timeout=8) as r:
                if r.status == 200:
                    data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bb = float(bids[0]["price"]) if bids else 0.0
                    ba = float(asks[0]["price"]) if asks else 0.0
                    return (bb + ba) / 2 if bb and ba else bb or ba
        except Exception:
            return 0.0

    async def close_position(self, session: aiohttp.ClientSession, pos_key: str, price: float, reason: str):
        pos = self.positions.get(pos_key)
        if not pos or pos.status != "open":
            return
        success, _ = await self.executor.place_order(pos.token_id, "SELL", pos.size_usd, price)
        if success:
            pos.status = "closed"
            pos.exit_price = price
            pos.pnl = (price - pos.entry_price) * pos.shares if pos.side == "BUY" else (pos.entry_price - price) * pos.shares
            logging.info(f"CLOSED [{reason}] {pos.question[:50]} | PnL: ${pos.pnl:+.2f}")

    async def scan_and_copy(self):
        async with aiohttp.ClientSession() as session:
            bankroll = await self.balance.get(session, force=True)
            if bankroll > self.peak_bankroll:
                self.peak_bankroll = bankroll

            logging.info(f"Scan | Bankroll=${bankroll:.2f} | Open Positions={len(self.positions)} | Peak=${self.peak_bankroll:.2f}")

            # Update PnL + Check exits
            for key, pos in list(self.positions.items()):
                if pos.status != "open":
                    continue
                mid = await self.get_mid_price(session, pos.token_id)
                if mid > 0:
                    pos.current_price = mid
                    if mid > pos.peak_price:
                        pos.peak_price = mid
                    pos.pnl = (mid - pos.entry_price) * pos.shares if pos.side == "BUY" else (pos.entry_price - mid) * pos.shares

                    if mid <= pos.entry_price * (1 - STOP_LOSS) or mid <= pos.peak_price * (1 - TRAIL_STOP):
                        await self.close_position(session, key, mid, "risk_exit")

            # Copy new trades
            for wallet, config in WALLETS.items():
                source_pos = await self.get_source_positions(session, wallet)
                for p in source_pos:
                    token_id = p.get("asset")
                    if not token_id:
                        continue
                    value = float(p.get("currentValue") or p.get("value") or 0)
                    if value < MIN_SOURCE_SIZE:
                        continue

                    key = self._position_key(wallet, token_id)
                    if key in self.positions:
                        continue

                    side = (p.get("side") or "BUY").upper()
                    mid_price = await self.get_mid_price(session, token_id)
                    if mid_price < 0.01:
                        continue

                    size_usd = self._trade_size(bankroll, wallet, mid_price)
                    if size_usd < 0.10:
                        continue

                    success, _ = await self.executor.place_order(token_id, side, size_usd, mid_price)

                    if success:
                        self.positions[key] = Position(
                            question=p.get("title", "Unknown"),
                            outcome=p.get("outcome", ""),
                            token_id=token_id,
                            side=side,
                            entry_price=mid_price,
                            size_usd=size_usd,
                            shares=round(size_usd / mid_price, 4),
                            source_wallet=wallet,
                            source_name=config["name"],
                            peak_price=mid_price,
                        )
                        logging.info(f"✅ COPIED [{config['name']}] {side} ${size_usd:.2f} @ {mid_price:.3f}")

    async def run(self):
        logging.info(f"🚀 Bot Started | Mode: {'🟢 LIVE' if not self.dry_run else '🔵 DRY RUN'}")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Main loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# ==================== DASHBOARD (Updated for Render) ====================
def run_dashboard(bot: CopyTrader):
    from http.server import HTTPServer, BaseHTTPRequestHandler
    import socket

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/home"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                html = f"""
                <h1>Polymarket Multi-Wallet CopyTrader</h1>
                <p><strong>Status:</strong> Running</p>
                <p><strong>Mode:</strong> {'<span style="color:red">LIVE TRADING</span>' if not bot.dry_run else 'DRY RUN'}</p>
                <p><strong>Bankroll:</strong> ${bot.balance.cached_balance:.2f}</p>
                <p><strong>Peak Bankroll:</strong> ${bot.peak_bankroll:.2f}</p>
                <p><strong>Open Positions:</strong> {len(bot.positions)}</p>
                <hr>
                <p><a href="/health">View JSON Health Check</a></p>
                """
                self.wfile.write(html.encode())
                return

            elif self.path == "/health":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                total_pnl = sum(p.pnl for p in bot.positions.values() if p.status == "open")
                data = {
                    "status": "running",
                    "mode": "LIVE" if not bot.dry_run else "DRY RUN",
                    "bankroll": round(bot.balance.cached_balance, 4),
                    "peak_bankroll": round(bot.peak_bankroll, 4),
                    "open_positions": len(bot.positions),
                    "total_unrealized_pnl": round(total_pnl, 4),
                    "port": HEALTH_PORT
                }
                self.wfile.write(json.dumps(data, indent=2).encode())
                return

            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    try:
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), Handler)
        logging.info(f"🌐 Dashboard running on http://0.0.0.0:{HEALTH_PORT}")
        logging.info(f"🌍 Render URL should be: https://multiwallet-copybot.onrender.com")
        server.serve_forever()
    except Exception as e:
        logging.error(f"Dashboard server failed: {e}")


# ==================== ENTRY POINT ====================
async def main():
    bot = CopyTrader(dry_run=DRY_RUN)
    threading.Thread(target=run_dashboard, args=(bot,), daemon=True).start()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
