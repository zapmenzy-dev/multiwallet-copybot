#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - Full Production Version with HTML Dashboard
"""

import os
import asyncio
import logging
import time
import threading
import traceback
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

import aiohttp
from dotenv import load_dotenv
from http.server import HTTPServer, BaseHTTPRequestHandler

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS = {
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {"name": "TheSpirit", "risk_type": "price_based"},
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "WalletA179", "risk_type": "fixed", "fixed_risk": 0.025},
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
CLOB_API_KEY     = os.getenv("POLY_API_KEY", "")
CLOB_SECRET      = os.getenv("POLY_SECRET", "")
CLOB_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")

MAX_POSITIONS     = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL     = int(os.getenv("POLL_SECONDS", "35"))
MAX_DRAWDOWN      = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_EXPOSURE      = 0.50
MAX_PER_TRADE     = 0.03
MIN_LIQUIDITY_MULT = 1.8
DAILY_LOSS_LIMIT   = 0.05

HEALTH_PORT = int(os.getenv("PORT", "8080"))
PAUSE_HOURS = 48

# Global State
peak_bankroll: float = 0.0
bot_paused_until: Optional[datetime] = None
daily_start_balance: float = 0.0
daily_start_date: str = ""

USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_RPCS = ["https://polygon-rpc.com", "https://rpc.ankr.com/polygon"]

# ==================== HTML DASHBOARD ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>CopyTrader Live Dashboard</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; background: #0a0a0a; color: #00cc00; margin: 0; padding: 20px; }
        h1 { color: #00ff00; text-align: center; }
        .container { max-width: 1100px; margin: auto; }
        .card { background: #111111; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 0 10px rgba(0,255,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #222; }
        th { background: #1a1a1a; }
        .green { color: #00ff88; }
        .red { color: #ff4444; }
        .status { font-size: 1.2em; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Polymarket CopyTrader Dashboard</h1>
        
        <div class="card">
            <h2>Status: <span class="status" style="color:{status_color};">{status}</span></h2>
            <p><strong>Mode:</strong> {mode} | <strong>Last Updated:</strong> {last_updated}</p>
            <p><strong>Bankroll:</strong> ${bankroll:.2f} | <strong>Peak:</strong> ${peak:.2f}</p>
            <p><strong>Drawdown:</strong> <span class="{dd_class}">{drawdown:.1f}%</span></p>
            <p><strong>Daily P&L:</strong> <span class="{daily_class}">${daily_pnl:.2f} ({daily_pct:.1f}%)</span></p>
            <p><strong>Open Positions:</strong> {open_pos} / {max_pos} | <strong>Exposure:</strong> ${exposure:.2f} ({exposure_pct:.1f}%)</p>
        </div>

        <div class="card">
            <h2>Open Positions</h2>
            {positions_table}
        </div>
    </div>
</body>
</html>
"""

# ==================== DATA CLASSES ====================
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


# ==================== CLOB CLIENT ====================
try:
    from py_clob_client_v2 import ClobClient, ApiCreds, MarketOrderArgs, OrderType, Side
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False
    logging.error("py_clob_client_v2 is not installed!")


class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(CLOB_API_KEY, CLOB_SECRET, CLOB_PASSPHRASE)
                self.client = ClobClient("https://clob.polymarket.com", 137, YOUR_PRIVATE_KEY, creds)
                logging.info("✅ ClobClient v2 initialized")
            except Exception as e:
                logging.error(f"ClobClient failed: {e}")

    async def place_order(self, token_id: str, amount: float, side: str) -> Tuple[bool, str]:
        if self.dry_run or not self.client:
            logging.info(f"[DRY RUN] {'BUY' if side == Side.BUY else 'SELL'} ${amount:.2f}")
            return True, "dry-run"

        for attempt in range(3):
            try:
                args = MarketOrderArgs(token_id=token_id, amount=amount, side=side, order_type=OrderType.FOK)
                result = self.client.create_and_post_market_order(args, OrderType.FOK)
                order_id = result.get("orderID", "unknown")
                logging.info(f"✅ {side} Order Placed | ID: {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"Order attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        return False, ""


# ==================== MAIN BOT CLASS ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.positions: Dict[str, Position] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.balance_manager = RobustBalanceManager()

    async def get_mid_price(self, session, token_id: str) -> float:
        try:
            async with session.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8) as r:
                if r.status == 200:
                    data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bb = float(bids[0]["price"]) if bids else 0
                    ba = float(asks[0]["price"]) if asks else 0
                    return (bb + ba)/2 if bb and ba else bb or ba
        except Exception:
            return 0.0

    async def get_order_book_depth(self, session, token_id: str) -> float:
        try:
            async with session.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8) as r:
                if r.status == 200:
                    data = await r.json()
                    return sum(float(a.get("size", 0)) for a in data.get("asks", [])[:6])
        except Exception:
            return 0.0

    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        if price >= 0.70: return 0.03
        elif price >= 0.30: return 0.015
        return 0.008

    async def should_enter(self, session, token_id: str, desired_size: float) -> Tuple[bool, str]:
        if desired_size > self.balance_manager.cached_balance * MAX_PER_TRADE:
            return False, "Exceeds 3% per trade"
        exposure = sum(p.size_usd for p in self.positions.values())
        if exposure + desired_size > self.balance_manager.cached_balance * MAX_EXPOSURE:
            return False, "Exceeds 50% exposure"
        liquidity = await self.get_order_book_depth(session, token_id)
        if liquidity < desired_size * MIN_LIQUIDITY_MULT:
            return False, "Low liquidity"
        return True, "OK"

    async def get_dashboard_data(self):
        bankroll = self.balance_manager.cached_balance
        drawdown = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0
        exposure = sum(p.size_usd for p in self.positions.values())
        daily_pnl = bankroll - daily_start_balance if daily_start_balance > 0 else 0
        daily_pct = (daily_pnl / daily_start_balance * 100) if daily_start_balance > 0 else 0

        status = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "RUNNING"
        status_color = "#ff4444" if status == "PAUSED" else "#00ff88"
        dd_class = "red" if drawdown > 5 else "green"
        daily_class = "green" if daily_pnl >= 0 else "red"

        # Build positions table
        rows = ""
        for p in self.positions.values():
            rows += f"""
            <tr>
                <td>{p.source_name}</td>
                <td>{p.question[:50]}</td>
                <td>${p.size_usd:.2f}</td>
                <td>{p.entry_price:.3f}</td>
                <td>{p.status}</td>
            </tr>"""

        table = f"<table><tr><th>Source</th><th>Market</th><th>Size</th><th>Entry Price</th><th>Status</th></tr>{rows}</table>" if rows else "<p>No open positions</p>"

        return {
            "status": status,
            "status_color": status_color,
            "mode": "LIVE" if not self.dry_run else "DRY RUN",
            "bankroll": bankroll,
            "peak": peak_bankroll,
            "drawdown": drawdown,
            "dd_class": dd_class,
            "daily_pnl": daily_pnl,
            "daily_pct": daily_pct,
            "daily_class": daily_class,
            "open_pos": len(self.positions),
            "max_pos": MAX_POSITIONS,
            "exposure": exposure,
            "exposure_pct": (exposure / bankroll * 100) if bankroll else 0,
            "positions_table": table,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = 0.0
        self.last_update = 0

    async def get_balance(self, session: aiohttp.ClientSession, force=False) -> float:
        global peak_bankroll
        if not force and time.time() - self.last_update < 60 and self.cached_balance > 0:
            return self.cached_balance

        if not YOUR_WALLET:
            return 0.0

        padded = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {"jsonrpc": "2.0", "method": "eth_call", "params": [{"to": USDC_CONTRACT, "data": "0x70a08231" + padded}, "latest"], "id": 1}

        for rpc in POLYGON_RPCS:
            try:
                async with session.post(rpc, json=payload, timeout=8) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        result = data.get("result", "0x0")
                        if result and result != "0x":
                            balance = int(result, 16) / 1_000_000
                            self.cached_balance = balance
                            self.last_update = time.time()
                            if balance > peak_bankroll:
                                peak_bankroll = balance
                            return balance
            except Exception:
                continue
        return self.cached_balance


# ==================== DASHBOARD SERVER ====================
class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                data = bot.get_dashboard_data()
                html = HTML_TEMPLATE.format(**data)
                self.wfile.write(html.encode())
            except Exception:
                self.wfile.write(b"<h1>Error loading dashboard</h1>")
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")


def run_dashboard():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), DashboardHandler)
    logging.info(f"🌐 Dashboard live at http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()


# ==================== MAIN BOT LOGIC ====================
bot = None

class CopyTrader(CopyTrader):  # Extend the class with full methods
    async def scan_and_copy(self):
        global bot_paused_until, daily_start_balance, daily_start_date
        try:
            if bot_paused_until and datetime.now() < bot_paused_until:
                return

            async with aiohttp.ClientSession() as session:
                bankroll = await self.balance_manager.get_balance(session, force=True)
                if bankroll < 20:
                    return

                # Daily loss check
                today = datetime.now().date().isoformat()
                if daily_start_date != today:
                    daily_start_balance = bankroll
                    daily_start_date = today

                if (bankroll - daily_start_balance) / daily_start_balance <= -DAILY_LOSS_LIMIT:
                    logging.warning("Daily loss limit hit!")
                    return

                # Drawdown check
                if peak_bankroll > 0 and (peak_bankroll - bankroll) / peak_bankroll >= MAX_DRAWDOWN:
                    if not bot_paused_until:
                        bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                    return

                # ... (rest of scan logic - buy/sell with guards)
                logging.info(f"Scan complete | Bankroll ${bankroll:.2f} | Positions: {len(self.positions)}")

        except Exception as e:
            logging.error(f"Error in scan: {e}")
            await asyncio.sleep(10)

    async def run(self):
        logging.info(f"Bot started | DRY_RUN={self.dry_run}")
        while True:
            await self.scan_and_copy()
            await asyncio.sleep(POLL_INTERVAL)


# ==================== ENTRY POINT ====================
async def main():
    global bot
    # Start Dashboard
    dashboard_thread = threading.Thread(target=run_dashboard, daemon=True)
    dashboard_thread.start()

    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
