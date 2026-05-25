#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY
- Real Execution (py-clob-client)
- Real Mid-Price Fetching
- Full Buy + Sell Copying
- 20% Drawdown Protection
- Live Wallet Balance (no hardcoded bankroll)
- Debug Logging for API responses
- Health endpoint for Render (keeps bot awake)
"""

import os
import asyncio
import requests
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ==================== CLOB CLIENT ====================
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs
    from py_clob_client.constants import POLYGON
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py-clob-client not installed. Running in simulation mode.")

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS = {
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {"name": "TheSpirit", "risk_type": "price_based"},
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "WalletA179", "risk_type": "fixed", "fixed_risk": 0.025},
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")

MAX_POSITIONS    = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL    = int(os.getenv("POLL_SECONDS", "40"))
COMPOUNDING_RATE = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN     = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT      = int(os.getenv("PORT", "8080"))
PAUSE_HOURS      = 48
MAX_RETRIES      = 3
RETRY_DELAY      = 5

# These are set dynamically from real wallet balance on first scan
current_bankroll: float = 0.0
peak_bankroll:    float = 0.0
bot_paused_until: Optional[datetime] = None


# ==================== HEALTH SERVER ====================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK - CopyTrader running")

    def log_message(self, format, *args):
        pass  # suppress noisy access logs


def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"Health server listening on port {HEALTH_PORT}")
    server.serve_forever()


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


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = 0.0
        self.last_update    = 0
        self.peak_balance   = 0.0
        self.initialized    = False

    def _fetch_balance(self) -> float:
        """
        Try multiple Polymarket API endpoints to get real USDC balance.
        Logs the raw response so you can see exactly what comes back.
        """
        endpoints = [
            f"https://data-api.polymarket.com/balance?user={YOUR_WALLET}",
            f"https://data-api.polymarket.com/profile?user={YOUR_WALLET}",
            f"https://data-api.polymarket.com/value?user={YOUR_WALLET}",
        ]

        for url in endpoints:
            try:
                resp = requests.get(url, timeout=8)
                logging.info(f"[BALANCE] {url.split('?')[0].split('/')[-1]} → status={resp.status_code} body={resp.text[:120]}")
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, (int, float)):
                        return float(data)
                    elif isinstance(data, list) and len(data) > 0:
                        # some endpoints return a list with one object
                        item = data[0]
                        val = item.get("balance") or item.get("portfolioValue") or item.get("value") or 0
                        return float(val)
                    elif isinstance(data, dict):
                        val = (
                            data.get("balance")
                            or data.get("portfolioValue")
                            or data.get("value")
                            or data.get("cashBalance")
                            or 0
                        )
                        return float(val)
            except Exception as e:
                logging.warning(f"[BALANCE] Failed to fetch from {url}: {e}")
                continue

        logging.warning("[BALANCE] All endpoints failed — using cached balance")
        return 0.0

    def get_balance(self, force=False) -> float:
        global peak_bankroll
        if force or not self.initialized or (time.time() - self.last_update > 60):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                self.initialized    = True
                if real > self.peak_balance:
                    self.peak_balance = real
                    peak_bankroll     = real
                logging.info(f"[BALANCE] Live wallet balance: ${real:.4f} USDC")
            else:
                if not self.initialized:
                    logging.warning("[BALANCE] Could not fetch real balance yet — waiting for next poll")
        return self.cached_balance

    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if self.peak_balance == 0:
            return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd


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
                    api_key        = POLY_API_KEY,
                    api_secret     = POLY_SECRET,
                    api_passphrase = POLY_PASSPHRASE,
                )
                logging.info("ClobClient initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient init failed: {e}")
                self.client = None

    def place_buy(self, token_id: str, amount_usd: float) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] BUY ${amount_usd:.2f} token {token_id[:12]}…")
            return True, "dry-run-buy"
        for attempt in range(MAX_RETRIES):
            try:
                args     = MarketOrderArgs(token_id=token_id, amount=amount_usd)
                result   = self.client.create_and_post_order(args)
                order_id = result.get("orderID", "unknown")
                logging.info(f"BUY placed: {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"BUY attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, ""

    def place_sell(self, token_id: str, shares: float) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] SELL {shares:.4f} shares token {token_id[:12]}…")
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
                time.sleep(RETRY_DELAY)
        return False, ""


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run   = dry_run
        self.balance   = RobustBalanceManager()
        self.positions: Dict[str, Position] = {}
        self.executor  = PolymarketExecutor(dry_run)

        logging.info(f"Multi-Wallet CopyTrader started | mode={'DRY RUN' if dry_run else 'LIVE'}")
        logging.info(f"Watching {len(WALLETS)} wallets | max positions={MAX_POSITIONS}")
        logging.info(f"Your wallet: {YOUR_WALLET[:10]}..." if YOUR_WALLET else "[BALANCE] WARNING: DEPOSIT_WALLET_ADDRESS not set!")

    def get_mid_price(self, token_id: str) -> float:
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(
                    f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8
                )
                if r.status_code == 200:
                    data     = r.json()
                    bids     = data.get("bids", [])
                    asks     = data.get("asks", [])
                    best_bid = float(bids[0]["price"]) if bids else 0
                    best_ask = float(asks[0]["price"]) if asks else 0
                    if best_bid and best_ask:
                        return (best_bid + best_ask) / 2
                    return best_bid or best_ask
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logging.warning(f"Mid-price fetch failed: {e}")
                time.sleep(RETRY_DELAY)
        return 0.0

    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        if price >= 0.70:
            return 0.03
        elif price >= 0.30:
            return 0.01
        else:
            return 0.006

    def check_drawdown(self) -> bool:
        global peak_bankroll, bot_paused_until
        current = self.balance.get_balance()
        if current > peak_bankroll:
            peak_bankroll = current
        if peak_bankroll == 0:
            return False
        dd = (peak_bankroll - current) / peak_bankroll
        if dd >= MAX_DRAWDOWN:
            if bot_paused_until is None or datetime.now() > bot_paused_until:
                bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                logging.warning(
                    f"DRAWDOWN PROTECTION TRIGGERED ({dd*100:.1f}%) — paused {PAUSE_HOURS}h"
                )
            return True
        return False

    def _get_positions(self, wallet_addr: str) -> Optional[list]:
        for attempt in range(MAX_RETRIES):
            try:
                url  = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50"
                resp = requests.get(url, timeout=12)
                logging.info(f"[DEBUG] positions API status={resp.status_code} wallet={wallet_addr[:10]}...")
                if resp.status_code == 200:
                    data = resp.json()
                    logging.info(f"[DEBUG] response type={type(data).__name__} len={len(data) if isinstance(data, list) else 'N/A'}")
                    return data
                else:
                    logging.warning(f"[DEBUG] Non-200 response: {resp.status_code} body={resp.text[:200]}")
            except Exception as e:
                logging.warning(f"Position fetch attempt {attempt+1} failed for {wallet_addr}: {e}")
                time.sleep(RETRY_DELAY)
        return None

    async def scan_and_copy(self):
        global current_bankroll, bot_paused_until

        if bot_paused_until and datetime.now() < bot_paused_until:
            remaining = (bot_paused_until - datetime.now()).seconds // 60
            logging.info(f"Bot paused — {remaining} minutes remaining")
            return

        if self.check_drawdown():
            return

        # Always read live wallet balance
        current_bankroll = self.balance.get_balance(force=True)

        if current_bankroll <= 0:
            logging.warning("Bankroll is $0 or unavailable — skipping scan until balance is fetched")
            return

        logging.info(f"Scanning | bankroll=${current_bankroll:.4f} | open={len(self.positions)}")

        for wallet_addr, config in WALLETS.items():
            raw = self._get_positions(wallet_addr)
            if raw is None:
                logging.warning(f"Skipping {config['name']} — could not fetch positions")
                continue

            # ---- DEBUG: show what API returned ----
            logging.info(f"[DEBUG] {config['name']} ({wallet_addr[:10]}…) returned {len(raw)} positions")
            if len(raw) == 0:
                logging.info(f"[DEBUG] {config['name']} — wallet has NO open positions right now")
            else:
                for i, p in enumerate(raw[:5]):
                    logging.info(
                        f"[DEBUG] {config['name']} pos[{i}]: "
                        f"asset={str(p.get('asset','?'))[:12]}… "
                        f"value={p.get('value','?')} "
                        f"title={str(p.get('title','?'))[:40]} "
                        f"outcome={p.get('outcome','?')}"
                    )

            source_token_ids = set()

            # ---- BUY LOGIC ----
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

                if pos_key in self.positions:
                    continue  # already copied

                if len(self.positions) >= MAX_POSITIONS:
                    logging.info("Max positions reached — skipping new entries")
                    break

                mid_price = self.get_mid_price(token_id)
                if mid_price <= 0:
                    continue

                risk_pct = self.get_risk_percent(mid_price, config)
                my_size  = round(current_bankroll * risk_pct, 2)

                if my_size < 1.0:
                    logging.info(f"Size too small (${my_size:.2f}) — skipping {question[:40]}")
                    continue

                ok, order_id = self.executor.place_buy(token_id, my_size)
                if ok:
                    shares = my_size / mid_price if mid_price > 0 else 0
                    self.positions[pos_key] = Position(
                        market_id     = market_id,
                        question      = question,
                        outcome       = outcome,
                        token_id      = token_id,
                        entry_price   = mid_price,
                        size_usd      = my_size,
                        shares        = shares,
                        source_wallet = wallet_addr,
                        source_name   = config["name"],
                        order_id      = order_id,
                    )
                    logging.info(
                        f"COPIED {config['name']} | {question[:40]} | "
                        f"${my_size:.2f} @ {mid_price:.3f}"
                    )

            # ---- SELL LOGIC ----
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.token_id not in source_token_ids and position.status == "open":
                    exit_price = self.get_mid_price(position.token_id)
                    ok, _ = self.executor.place_sell(position.token_id, position.shares)
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


# ==================== ENTRY POINT ====================
async def main():
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
