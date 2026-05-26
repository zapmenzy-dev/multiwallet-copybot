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


# ==================== DASHBOARD ====================
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Polymarket CopyTrader</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0c0f;
    --surface: #111418;
    --border: #1e2530;
    --accent: #00e5a0;
    --accent2: #0066ff;
    --warn: #ff6b35;
    --text: #e8edf5;
    --muted: #4a5568;
    --live: #ff3c3c;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Space Mono', monospace;
    min-height: 100vh;
    overflow-x: hidden;
  }
  .grid-bg {
    position: fixed; inset: 0; z-index: 0;
    background-image:
      linear-gradient(rgba(0,229,160,0.03) 1px, transparent 1px),
      linear-gradient(90deg, rgba(0,229,160,0.03) 1px, transparent 1px);
    background-size: 40px 40px;
    pointer-events: none;
  }
  header {
    position: relative; z-index: 1;
    display: flex; align-items: center; justify-content: space-between;
    padding: 24px 40px;
    border-bottom: 1px solid var(--border);
    background: rgba(10,12,15,0.9);
    backdrop-filter: blur(10px);
  }
  .logo {
    font-family: 'Syne', sans-serif;
    font-size: 20px; font-weight: 800; letter-spacing: -0.5px;
    color: var(--text);
  }
  .logo span { color: var(--accent); }
  .mode-badge {
    display: flex; align-items: center; gap: 8px;
    padding: 6px 14px;
    border-radius: 4px;
    font-size: 11px; font-weight: 700; letter-spacing: 1.5px;
    text-transform: uppercase;
  }
  .mode-live { background: rgba(255,60,60,0.15); color: var(--live); border: 1px solid rgba(255,60,60,0.3); }
  .mode-dry  { background: rgba(0,102,255,0.15); color: var(--accent2); border: 1px solid rgba(0,102,255,0.3); }
  .dot { width: 7px; height: 7px; border-radius: 50%; animation: pulse 1.5s infinite; }
  .dot-live { background: var(--live); }
  .dot-dry  { background: var(--accent2); }
  @keyframes pulse {
    0%,100% { opacity: 1; transform: scale(1); }
    50%      { opacity: 0.4; transform: scale(0.8); }
  }
  main { position: relative; z-index: 1; padding: 40px; max-width: 1200px; margin: 0 auto; }
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px; margin-bottom: 40px;
  }
  .stat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 24px;
    position: relative; overflow: hidden;
    transition: border-color 0.2s;
  }
  .stat-card:hover { border-color: var(--accent); }
  .stat-card::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, var(--accent), transparent);
  }
  .stat-label {
    font-size: 10px; letter-spacing: 2px; text-transform: uppercase;
    color: var(--muted); margin-bottom: 12px;
  }
  .stat-value {
    font-family: 'Syne', sans-serif;
    font-size: 32px; font-weight: 800;
    color: var(--text); line-height: 1;
  }
  .stat-value.green { color: var(--accent); }
  .stat-value.red   { color: var(--warn); }
  .stat-value.blue  { color: var(--accent2); }
  .section-title {
    font-family: 'Syne', sans-serif;
    font-size: 13px; font-weight: 700;
    letter-spacing: 2px; text-transform: uppercase;
    color: var(--muted); margin-bottom: 16px;
    display: flex; align-items: center; gap: 10px;
  }
  .section-title::after {
    content: ''; flex: 1; height: 1px; background: var(--border);
  }
  .positions-table {
    width: 100%; border-collapse: collapse;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px; overflow: hidden;
  }
  .positions-table th {
    background: rgba(255,255,255,0.03);
    padding: 12px 16px;
    text-align: left;
    font-size: 10px; letter-spacing: 1.5px; text-transform: uppercase;
    color: var(--muted); font-weight: 400;
    border-bottom: 1px solid var(--border);
  }
  .positions-table td {
    padding: 14px 16px;
    font-size: 13px;
    border-bottom: 1px solid rgba(30,37,48,0.6);
    vertical-align: middle;
  }
  .positions-table tr:last-child td { border-bottom: none; }
  .positions-table tr:hover td { background: rgba(0,229,160,0.02); }
  .badge-buy  { background: rgba(0,229,160,0.12); color: var(--accent);  padding: 3px 8px; border-radius: 3px; font-size: 11px; font-weight: 700; }
  .badge-sell { background: rgba(255,107,53,0.12); color: var(--warn);   padding: 3px 8px; border-radius: 3px; font-size: 11px; font-weight: 700; }
  .pnl-pos { color: var(--accent); }
  .pnl-neg { color: var(--warn); }
  .empty-state {
    text-align: center; padding: 60px 20px;
    color: var(--muted); font-size: 13px;
    border: 1px solid var(--border);
    border-radius: 8px; background: var(--surface);
  }
  .empty-state .icon { font-size: 40px; margin-bottom: 12px; }
  footer {
    position: relative; z-index: 1;
    text-align: center;
    padding: 24px 40px;
    color: var(--muted); font-size: 11px;
    border-top: 1px solid var(--border);
    letter-spacing: 1px;
  }
  #last-updated { color: var(--muted); font-size: 11px; }
  .refresh-bar {
    position: fixed; bottom: 0; left: 0;
    height: 2px; background: var(--accent);
    transition: width 0.5s linear;
    z-index: 999;
  }
</style>
</head>
<body>
<div class="grid-bg"></div>
<div class="refresh-bar" id="refresh-bar"></div>

<header>
  <div class="logo">POLY<span>COPY</span></div>
  <div id="mode-badge" class="mode-badge mode-dry">
    <div class="dot dot-dry" id="mode-dot"></div>
    <span id="mode-text">DRY RUN</span>
  </div>
</header>

<main>
  <div class="stats-grid">
    <div class="stat-card">
      <div class="stat-label">Bankroll</div>
      <div class="stat-value green" id="bankroll">—</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Peak Bankroll</div>
      <div class="stat-value" id="peak">—</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Open Positions</div>
      <div class="stat-value blue" id="open-pos">—</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Unrealized PnL</div>
      <div class="stat-value" id="pnl">—</div>
    </div>
  </div>

  <div class="section-title">Open Positions</div>
  <div id="positions-container">
    <div class="empty-state">
      <div class="icon">📡</div>
      Loading positions…
    </div>
  </div>
</main>

<footer>
  <span id="last-updated">Refreshing…</span>
  &nbsp;·&nbsp; Polymarket CopyTrader
</footer>

<script>
const REFRESH_MS = 15000;
let countdown = REFRESH_MS;

function fmt(val, prefix='$') {
  if (val === null || val === undefined) return '—';
  return prefix + parseFloat(val).toFixed(2);
}

async function fetchHealth() {
  try {
    const r = await fetch('/health');
    return await r.json();
  } catch(e) {
    return null;
  }
}

async function fetchPositions() {
  try {
    const r = await fetch('/positions');
    if (!r.ok) return null;
    return await r.json();
  } catch(e) {
    return null;
  }
}

function renderPositions(positions) {
  const c = document.getElementById('positions-container');
  if (!positions || positions.length === 0) {
    c.innerHTML = `<div class="empty-state"><div class="icon">🔍</div>No open positions yet. Bot is scanning…</div>`;
    return;
  }
  const rows = positions.map(p => {
    const pnlClass = p.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
    const pnlStr = (p.pnl >= 0 ? '+' : '') + '$' + parseFloat(p.pnl).toFixed(2);
    const sideClass = p.side === 'BUY' ? 'badge-buy' : 'badge-sell';
    return `<tr>
      <td>${p.question ? p.question.substring(0,50) + (p.question.length>50?'…':'') : '—'}</td>
      <td>${p.source_name || '—'}</td>
      <td><span class="${sideClass}">${p.side}</span></td>
      <td>$${parseFloat(p.size_usd||0).toFixed(2)}</td>
      <td>${parseFloat(p.entry_price||0).toFixed(3)}</td>
      <td>${parseFloat(p.current_price||0).toFixed(3)}</td>
      <td class="${pnlClass}">${pnlStr}</td>
    </tr>`;
  }).join('');
  c.innerHTML = `<table class="positions-table">
    <thead><tr>
      <th>Market</th><th>Source</th><th>Side</th>
      <th>Size</th><th>Entry</th><th>Current</th><th>PnL</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

async function update() {
  const data = await fetchHealth();
  if (data) {
    const isLive = data.mode === 'LIVE';
    const badge = document.getElementById('mode-badge');
    const dot   = document.getElementById('mode-dot');
    badge.className = 'mode-badge ' + (isLive ? 'mode-live' : 'mode-dry');
    dot.className   = 'dot ' + (isLive ? 'dot-live' : 'dot-dry');
    document.getElementById('mode-text').textContent = data.mode;

    document.getElementById('bankroll').textContent = fmt(data.bankroll);
    document.getElementById('peak').textContent     = fmt(data.peak_bankroll);
    document.getElementById('open-pos').textContent = data.open_positions ?? '—';

    const pnlEl = document.getElementById('pnl');
    const pnl = parseFloat(data.total_unrealized_pnl || 0);
    pnlEl.textContent = (pnl >= 0 ? '+' : '') + '$' + pnl.toFixed(2);
    pnlEl.className = 'stat-value ' + (pnl >= 0 ? 'green' : 'red');
  }

  const pos = await fetchPositions();
  renderPositions(pos);

  document.getElementById('last-updated').textContent =
    'Last updated: ' + new Date().toLocaleTimeString();
}

// Countdown bar
function tickBar() {
  countdown -= 500;
  if (countdown <= 0) { countdown = REFRESH_MS; update(); }
  const pct = ((REFRESH_MS - countdown) / REFRESH_MS * 100).toFixed(1);
  document.getElementById('refresh-bar').style.width = pct + '%';
  setTimeout(tickBar, 500);
}

update();
tickBar();
</script>
</body>
</html>"""


def run_dashboard(bot: "CopyTrader"):
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # silence request logs

        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                total_pnl = sum(p.pnl for p in bot.positions.values() if p.status == "open")
                data = {
                    "status": "running",
                    "mode": "LIVE" if not bot.dry_run else "DRY RUN",
                    "bankroll": round(bot.balance.cached_balance, 4),
                    "peak_bankroll": round(bot.peak_bankroll, 4),
                    "open_positions": len([p for p in bot.positions.values() if p.status == "open"]),
                    "total_unrealized_pnl": round(total_pnl, 4),
                }
                self.wfile.write(json.dumps(data, indent=2).encode())

            elif self.path == "/positions":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                open_pos = [
                    {
                        "question":      p.question,
                        "outcome":       p.outcome,
                        "source_name":   p.source_name,
                        "side":          p.side,
                        "size_usd":      round(p.size_usd, 2),
                        "entry_price":   round(p.entry_price, 4),
                        "current_price": round(p.current_price, 4),
                        "pnl":           round(p.pnl, 4),
                        "peak_price":    round(p.peak_price, 4),
                        "opened_at":     p.opened_at.isoformat(),
                    }
                    for p in bot.positions.values() if p.status == "open"
                ]
                self.wfile.write(json.dumps(open_pos, indent=2).encode())

            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(DASHBOARD_HTML.encode())

    server = HTTPServer(("0.0.0.0", HEALTH_PORT), Handler)
    logging.info(f"🌐 Dashboard running on port {HEALTH_PORT}")
    server.serve_forever()


# ==================== ENTRY POINT ====================
async def main():
    bot = CopyTrader(dry_run=DRY_RUN)
    threading.Thread(target=run_dashboard, args=(bot,), daemon=True).start()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main()
)
