#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - Production Ready
- HTML Live Dashboard
- pUSD/USDC balance via env var (BANKROLL) with RPC fallback
- Full Buy + Sell copying
- Drawdown + Daily loss protection
- Health endpoint for Render
"""

import os
import asyncio
import logging
import time
import threading
import json
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
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "WalletA179", "risk_type": "fixed", "fixed_risk": 0.025},
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
CLOB_API_KEY     = os.getenv("POLY_API_KEY", "")
CLOB_SECRET      = os.getenv("POLY_SECRET", "")
CLOB_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")

# Bankroll: reads live from RPC, falls back to BANKROLL env var
BANKROLL_FALLBACK = float(os.getenv("BANKROLL", "0"))

MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_EXPOSURE       = 0.50   # max 50% of bankroll in open positions
MAX_PER_TRADE      = 0.03   # max 3% per trade
MIN_LIQUIDITY_MULT = 1.8    # need 1.8x our size in ask-side liquidity
DAILY_LOSS_LIMIT   = 0.05   # pause if down 5% in a day
HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 48
MAX_RETRIES        = 3

# pUSD contract on Polygon (Polymarket's collateral since Apr 28 2026)
PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYGON_RPCS  = [
    "https://rpc.ankr.com/polygon",
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.llamarpc.com",
]

# Global state
peak_bankroll:       float = BANKROLL_FALLBACK
bot_paused_until:    Optional[datetime] = None
daily_start_balance: float = 0.0
daily_start_date:    str   = ""


# ==================== CLOB CLIENT ====================
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs
    from py_clob_client.constants import POLYGON
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py-clob-client not installed — running in simulation mode")


# ==================== DATA CLASSES ====================
@dataclass
class Position:
    market_id:     str
    question:      str
    outcome:       str
    token_id:      str
    entry_price:   float
    size_usd:      float
    shares:        float
    source_wallet: str
    source_name:   str
    status:        str   = "open"
    exit_price:    float = 0.0
    pnl:           float = 0.0
    order_id:      str   = ""


# ==================== HTML DASHBOARD ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <title>CopyTrader Dashboard</title>
    <meta http-equiv="refresh" content="15">
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background:#0a0a0a; color:#00cc00; margin:0; padding:20px; }}
        h1 {{ color:#00ff00; text-align:center; }}
        .container {{ max-width:1100px; margin:auto; }}
        .card {{ background:#111; border-radius:10px; padding:20px; margin-bottom:20px; box-shadow:0 0 10px rgba(0,255,0,0.1); }}
        table {{ width:100%; border-collapse:collapse; }}
        th,td {{ padding:10px; text-align:left; border-bottom:1px solid #222; }}
        th {{ background:#1a1a1a; }}
        .green {{ color:#00ff88; }} .red {{ color:#ff4444; }}
        .status {{ font-size:1.2em; font-weight:bold; }}
    </style>
</head>
<body>
<div class="container">
    <h1>🤖 Polymarket CopyTrader</h1>
    <div class="card">
        <h2>Status: <span class="status" style="color:{status_color};">{status}</span></h2>
        <p><strong>Mode:</strong> {mode} &nbsp;|&nbsp; <strong>Updated:</strong> {last_updated}</p>
        <p><strong>Bankroll:</strong> ${bankroll:.2f} &nbsp;|&nbsp; <strong>Peak:</strong> ${peak:.2f}</p>
        <p><strong>Drawdown:</strong> <span class="{dd_class}">{drawdown:.1f}%</span></p>
        <p><strong>Daily P&amp;L:</strong> <span class="{daily_class}">${daily_pnl:.2f} ({daily_pct:.1f}%)</span></p>
        <p><strong>Positions:</strong> {open_pos}/{max_pos} &nbsp;|&nbsp; <strong>Exposure:</strong> ${exposure:.2f} ({exposure_pct:.1f}%)</p>
    </div>
    <div class="card">
        <h2>Open Positions</h2>
        {positions_table}
    </div>
</div>
</body>
</html>"""


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update    = 0
        self.initialized    = BANKROLL_FALLBACK > 0

    async def fetch(self, session: aiohttp.ClientSession) -> float:
        global peak_bankroll
        if not YOUR_WALLET:
            return self.cached_balance

        padded  = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0", "method": "eth_call",
            "params":  [{"to": PUSD_CONTRACT, "data": "0x70a08231" + padded}, "latest"],
            "id": 1,
        }

        for rpc in POLYGON_RPCS:
            try:
                async with session.post(rpc, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        data   = await resp.json(content_type=None)
                        result = data.get("result", "0x0")
                        if result and result not in ("0x", "0x0"):
                            balance = int(result, 16) / 1_000_000
                            if balance > 0:
                                logging.info(f"[BALANCE] On-chain pUSD: ${balance:.4f} (via {rpc.split('//')[-1]})")
                                self.cached_balance = balance
                                self.last_update    = time.time()
                                self.initialized    = True
                                if balance > peak_bankroll:
                                    peak_bankroll = balance
                                return balance
            except Exception as e:
                logging.debug(f"[BALANCE] RPC {rpc} failed: {e}")
                continue

        # Use env var fallback
        if BANKROLL_FALLBACK > 0:
            logging.info(f"[BALANCE] RPC failed — using BANKROLL env var: ${BANKROLL_FALLBACK:.2f}")
            return BANKROLL_FALLBACK

        logging.warning("[BALANCE] Could not fetch balance — using cached")
        return self.cached_balance

    async def get(self, session, force=False) -> float:
        if force or not self.initialized or time.time() - self.last_update > 60:
            return await self.fetch(session)
        return self.cached_balance


# ==================== EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                self.client = ClobClient(
                    host           = "https://clob.polymarket.com",
                    key            = YOUR_PRIVATE_KEY,
                    chain_id       = POLYGON,
                    api_key        = CLOB_API_KEY,
                    api_secret     = CLOB_SECRET,
                    api_passphrase = CLOB_PASSPHRASE,
                )
                logging.info("ClobClient initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient init failed: {e}")

    async def place_buy(self, token_id: str, amount: float) -> Tuple[bool, str]:
        if self.dry_run or not self.client:
            logging.info(f"[DRY RUN] BUY ${amount:.2f} token {token_id[:12]}…")
            return True, "dry-run-buy"
        for attempt in range(MAX_RETRIES):
            try:
                args     = MarketOrderArgs(token_id=token_id, amount=amount)
                result   = self.client.create_and_post_order(args)
                order_id = result.get("orderID", "unknown")
                logging.info(f"BUY placed: {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"BUY attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        return False, ""

    async def place_sell(self, token_id: str, shares: float) -> Tuple[bool, str]:
        if self.dry_run or not self.client:
            logging.info(f"[DRY RUN] SELL {shares:.4f} shares {token_id[:12]}…")
            return True, "dry-run-sell"
        for attempt in range(MAX_RETRIES):
            try:
                args     = MarketOrderArgs(token_id=token_id, amount=shares)
                result   = self.client.create_and_post_order(args)
                order_id = result.get("orderID", "unknown")
                logging.info(f"SELL placed: {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"SELL attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        return False, ""


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run  = dry_run
        self.positions: Dict[str, Position] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.balance  = RobustBalanceManager()
        logging.info(f"CopyTrader started | mode={'DRY RUN' if dry_run else 'LIVE'} | wallets={len(WALLETS)}")

    # ---- helpers ----
    async def get_mid_price(self, session, token_id: str) -> float:
        for _ in range(MAX_RETRIES):
            try:
                async with session.get(
                    f"https://clob.polymarket.com/book?token_id={token_id}",
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status == 200:
                        data = await r.json(content_type=None)
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])
                        bb   = float(bids[0]["price"]) if bids else 0
                        ba   = float(asks[0]["price"]) if asks else 0
                        return (bb + ba) / 2 if bb and ba else bb or ba
            except Exception:
                await asyncio.sleep(2)
        return 0.0

    async def get_ask_depth(self, session, token_id: str) -> float:
        try:
            async with session.get(
                f"https://clob.polymarket.com/book?token_id={token_id}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    return sum(float(a.get("size", 0)) for a in data.get("asks", [])[:6])
        except Exception:
            pass
        return 0.0

    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        if price >= 0.70: return 0.03
        elif price >= 0.30: return 0.015
        return 0.008

    async def get_positions(self, session, wallet_addr: str) -> Optional[list]:
        url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50"
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                    logging.info(f"[DEBUG] {wallet_addr[:10]}… positions status={r.status}")
                    if r.status == 200:
                        data = await r.json(content_type=None)
                        logging.info(f"[DEBUG] returned {len(data) if isinstance(data, list) else 'N/A'} positions")
                        return data
            except Exception as e:
                logging.warning(f"Position fetch attempt {attempt+1}: {e}")
                await asyncio.sleep(3)
        return None

    # ---- dashboard data ----
    def dashboard_data(self, bankroll: float) -> dict:
        drawdown     = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0
        exposure     = sum(p.size_usd for p in self.positions.values())
        daily_pnl    = bankroll - daily_start_balance if daily_start_balance > 0 else 0
        daily_pct    = (daily_pnl / daily_start_balance * 100) if daily_start_balance > 0 else 0
        status       = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "RUNNING"
        status_color = "#ff4444" if status == "PAUSED" else "#00ff88"

        rows = "".join(
            f"<tr><td>{p.source_name}</td><td>{p.question[:55]}</td>"
            f"<td>${p.size_usd:.2f}</td><td>{p.entry_price:.3f}</td><td>{p.outcome}</td><td>{p.status}</td></tr>"
            for p in self.positions.values()
        )
        table = (
            f"<table><tr><th>Source</th><th>Market</th><th>Size</th>"
            f"<th>Entry</th><th>Outcome</th><th>Status</th></tr>{rows}</table>"
            if rows else "<p>No open positions</p>"
        )

        return dict(
            status=status, status_color=status_color,
            mode="LIVE" if not self.dry_run else "DRY RUN",
            bankroll=bankroll, peak=peak_bankroll,
            drawdown=drawdown, dd_class="red" if drawdown > 5 else "green",
            daily_pnl=daily_pnl, daily_pct=daily_pct,
            daily_class="green" if daily_pnl >= 0 else "red",
            open_pos=len(self.positions), max_pos=MAX_POSITIONS,
            exposure=exposure,
            exposure_pct=(exposure / bankroll * 100) if bankroll else 0,
            positions_table=table,
            last_updated=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

    # ---- main scan ----
    async def scan_and_copy(self):
        global bot_paused_until, daily_start_balance, daily_start_date, peak_bankroll

        if bot_paused_until and datetime.now() < bot_paused_until:
            remaining = int((bot_paused_until - datetime.now()).total_seconds() // 60)
            logging.info(f"Bot paused — {remaining}min remaining")
            return

        async with aiohttp.ClientSession() as session:
            bankroll = await self.balance.get(session, force=True)

            if bankroll <= 0:
                logging.warning("Bankroll unavailable — skipping scan")
                return

            if bankroll > peak_bankroll:
                peak_bankroll = bankroll

            # Daily tracking
            today = datetime.now().date().isoformat()
            if daily_start_date != today:
                daily_start_balance = bankroll
                daily_start_date    = today

            # Daily loss check
            if daily_start_balance > 0:
                daily_loss = (bankroll - daily_start_balance) / daily_start_balance
                if daily_loss <= -DAILY_LOSS_LIMIT:
                    logging.warning(f"Daily loss limit hit ({daily_loss*100:.1f}%) — skipping")
                    return

            # Drawdown check
            if peak_bankroll > 0:
                dd = (peak_bankroll - bankroll) / peak_bankroll
                if dd >= MAX_DRAWDOWN:
                    if not bot_paused_until or datetime.now() > bot_paused_until:
                        bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                        logging.warning(f"DRAWDOWN {dd*100:.1f}% — paused {PAUSE_HOURS}h")
                    return

            logging.info(f"Scanning | bankroll=${bankroll:.4f} | open={len(self.positions)}")

            for wallet_addr, config in WALLETS.items():
                raw = await self.get_positions(session, wallet_addr)
                if raw is None:
                    continue

                if len(raw) == 0:
                    logging.info(f"[DEBUG] {config['name']} — no open positions")
                else:
                    for i, p in enumerate(raw[:3]):
                        logging.info(
                            f"[DEBUG] {config['name']} pos[{i}]: "
                            f"asset={str(p.get('asset',''))[:12]}… "
                            f"value={p.get('value','?')} "
                            f"title={str(p.get('title',''))[:40]}"
                        )

                source_token_ids = set()

                # ---- BUY ----
                for pos in raw:
                    token_id  = pos.get("asset", "")
                    market_id = pos.get("market", "")
                    question  = pos.get("title", "Unknown")
                    outcome   = pos.get("outcome", "YES")
                    size_usd  = float(pos.get("value", 0))

                    if not token_id or size_usd < 1.0:
                        continue

                    source_token_ids.add(token_id)
                    pos_key = f"{wallet_addr}_{token_id}"

                    if pos_key in self.positions or len(self.positions) >= MAX_POSITIONS:
                        continue

                    mid_price = await self.get_mid_price(session, token_id)
                    if mid_price <= 0:
                        continue

                    risk_pct = self.get_risk_percent(mid_price, config)
                    my_size  = round(bankroll * risk_pct, 2)

                    if my_size < 1.0:
                        logging.info(f"Size too small (${my_size:.2f}) — skip {question[:35]}")
                        continue

                    # Exposure guard
                    exposure = sum(p.size_usd for p in self.positions.values())
                    if exposure + my_size > bankroll * MAX_EXPOSURE:
                        logging.info("Max exposure reached — skipping")
                        continue

                    # Liquidity guard
                    depth = await self.get_ask_depth(session, token_id)
                    if depth < my_size * MIN_LIQUIDITY_MULT:
                        logging.info(f"Low liquidity ({depth:.2f}) — skip {question[:35]}")
                        continue

                    ok, order_id = await self.executor.place_buy(token_id, my_size)
                    if ok:
                        shares = my_size / mid_price if mid_price > 0 else 0
                        self.positions[pos_key] = Position(
                            market_id=market_id, question=question,
                            outcome=outcome, token_id=token_id,
                            entry_price=mid_price, size_usd=my_size,
                            shares=shares, source_wallet=wallet_addr,
                            source_name=config["name"], order_id=order_id,
                        )
                        logging.info(
                            f"COPIED {config['name']} | {question[:40]} | "
                            f"${my_size:.2f} @ {mid_price:.3f}"
                        )

                # ---- SELL ----
                for pos_key, position in list(self.positions.items()):
                    if position.source_wallet != wallet_addr:
                        continue
                    if position.token_id not in source_token_ids and position.status == "open":
                        exit_price = await self.get_mid_price(session, position.token_id)
                        ok, _ = await self.executor.place_sell(position.token_id, position.shares)
                        if ok:
                            pnl = (exit_price - position.entry_price) * position.shares
                            position.status     = "closed"
                            position.exit_price = exit_price
                            position.pnl        = pnl
                            logging.info(
                                f"SOLD {position.question[:40]} | "
                                f"exit={exit_price:.3f} | pnl=${pnl:.2f}"
                            )
                            del self.positions[pos_key]

    async def run(self):
        logging.info("Bot loop started")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Main loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# ==================== DASHBOARD SERVER ====================
_bot_ref = None

class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        if self.path == "/" and _bot_ref:
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                data = _bot_ref.dashboard_data(_bot_ref.balance.cached_balance)
                self.wfile.write(HTML_TEMPLATE.format(**data).encode())
            except Exception as e:
                self.wfile.write(f"<h1>Error: {e}</h1>".encode())
        else:
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK - CopyTrader running")

    def log_message(self, format, *args):
        pass


def run_dashboard():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), DashboardHandler)
    logging.info(f"Dashboard live on port {HEALTH_PORT}")
    server.serve_forever()


# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref
    threading.Thread(target=run_dashboard, daemon=True).start()
    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
