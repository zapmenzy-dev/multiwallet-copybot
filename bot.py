#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER with Real-Time WebSocket Price Updates
"""

import os
import json
import asyncio
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, Set, List
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import deque

import aiohttp
from dotenv import load_dotenv

load_dotenv()

_LOG_LEVEL = logging.DEBUG if os.getenv("LOG_LEVEL", "INFO").upper() == "DEBUG" else logging.INFO
logging.basicConfig(
    level=_LOG_LEVEL,
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

MAX_POSITIONS      = int(os.getenv("MAX_POSITIONS", "9999"))
POLL_INTERVAL      = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_EXPOSURE       = 0.80
STOP_LOSS          = 0.50
TRAIL_STOP         = 0.25
MAX_PER_TRADE      = 0.03
MIN_TRADE_FRAC     = 0.006
MAX_TRADE_FRAC     = 0.03
MIN_SOURCE_SIZE    = 1.0
LIMIT_ORDER_TICK   = 0.01
DAILY_LOSS_LIMIT   = float(os.getenv("DAILY_LOSS_LIMIT", "100"))

HEALTH_PORT        = int(os.getenv("PORT", "8080"))
PAUSE_HOURS        = 24

POLYGON_RPCS  = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon-rpc.com",
]
BALANCE_OF_SELECTOR = "0x70a08231"
PUSD_CONTRACTS = [
    ("pUSD",             "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB", 6),
    ("CTF-v2",           "0xE111180000d2663C0091e4f400237545B87B996B", 6),
    ("NegRisk-v2",       "0xe2222d279d744050d28e00520010520000310F59", 6),
]

peak_bankroll: float = BANKROLL_FALLBACK
bot_paused_until: Optional[datetime] = None
daily_loss_today: float = 0.0
last_loss_reset_date: str = ""


def halt_bot():
    """Emergency halt the bot when daily loss limit is hit."""
    global bot_paused_until
    bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
    logging.error(f"🚨 DAILY LOSS LIMIT HIT (${DAILY_LOSS_LIMIT}) - Bot paused until {bot_paused_until}")


# ==================== REAL-TIME WEBSOCKET PRICE MANAGER ====================
class RealTimeWebSocketManager:
    """
    Real-time WebSocket manager for Polymarket order book updates.
    Provides millisecond-latency price updates for accurate PnL calculation.
    """
    
    def __init__(self):
        self.bid_cache: Dict[str, float] = {}      # token_id -> best_bid
        self.ask_cache: Dict[str, float] = {}      # token_id -> best_ask
        self.mid_cache: Dict[str, float] = {}      # token_id -> mid_price
        self.full_orderbook: Dict[str, dict] = {}   # token_id -> {bids: [], asks: []}
        self._subscribed_tokens: Set[str] = set()
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._price_callbacks: Dict[str, List[callable]] = {}  # token_id -> list of callbacks
        
    def register_callback(self, token_id: str, callback: callable):
        """Register a callback to be called when price updates for a token"""
        if token_id not in self._price_callbacks:
            self._price_callbacks[token_id] = []
        self._price_callbacks[token_id].append(callback)
        
    def unregister_callback(self, token_id: str, callback: callable):
        """Unregister a callback"""
        if token_id in self._price_callbacks and callback in self._price_callbacks[token_id]:
            self._price_callbacks[token_id].remove(callback)
            
    async def start(self, session: aiohttp.ClientSession):
        """Start the WebSocket connection"""
        self._session = session
        self._running = True
        self._task = asyncio.create_task(self._websocket_loop())
        logging.info("Real-time WebSocket price manager started")
        
    async def stop(self):
        """Stop the WebSocket connection"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
        logging.info("Real-time WebSocket price manager stopped")
        
    async def subscribe(self, token_id: str):
        """Subscribe to real-time order book updates for a token_id"""
        if token_id in self._subscribed_tokens:
            return
            
        self._subscribed_tokens.add(token_id)
        
        if self._ws and not self._ws.closed:
            try:
                sub_msg = json.dumps({
                    "type": "subscribe",
                    "channel": "level2",
                    "token_id": token_id
                })
                await self._ws.send_str(sub_msg)
                logging.info(f"📡 Subscribed to real-time order book for {token_id[:12]}...")
            except Exception as e:
                logging.warning(f"Failed to subscribe to {token_id[:12]}: {e}")
                
    async def unsubscribe(self, token_id: str):
        """Unsubscribe from order book updates"""
        if token_id not in self._subscribed_tokens:
            return
            
        self._subscribed_tokens.discard(token_id)
        
        if self._ws and not self._ws.closed:
            try:
                unsub_msg = json.dumps({
                    "type": "unsubscribe",
                    "channel": "level2",
                    "token_id": token_id
                })
                await self._ws.send_str(unsub_msg)
                logging.info(f"📡 Unsubscribed from order book for {token_id[:12]}...")
            except Exception:
                pass
                
    def get_best_bid(self, token_id: str) -> float:
        """Get the latest cached best bid (real-time)"""
        return self.bid_cache.get(token_id, 0.0)
    
    def get_best_ask(self, token_id: str) -> float:
        """Get the latest cached best ask (real-time)"""
        return self.ask_cache.get(token_id, 0.0)
    
    def get_mid_price(self, token_id: str) -> float:
        """Get the latest cached mid price (real-time)"""
        return self.mid_cache.get(token_id, 0.0)
    
    def get_orderbook_depth(self, token_id: str, depth: int = 10) -> tuple:
        """Get order book depth for more accurate PnL calculation"""
        book = self.full_orderbook.get(token_id, {})
        bids = book.get("bids", [])[:depth]
        asks = book.get("asks", [])[:depth]
        return bids, asks
        
    async def _websocket_loop(self):
        """Main WebSocket connection loop with auto-reconnect"""
        while self._running:
            try:
                await self._connect()
                await self._listen()
            except Exception as e:
                logging.error(f"WebSocket error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)
                
    async def _connect(self):
        """Establish WebSocket connection"""
        ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws"
        self._ws = await self._session.ws_connect(ws_url)
        logging.info("🔌 Real-time WebSocket connected to Polymarket")
        
        # Resubscribe to all tokens
        for token_id in self._subscribed_tokens:
            sub_msg = json.dumps({
                "type": "subscribe",
                "channel": "level2",
                "token_id": token_id
            })
            await self._ws.send_str(sub_msg)
            
    async def _listen(self):
        """Listen for incoming WebSocket messages"""
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await self._handle_message(data)
                except json.JSONDecodeError:
                    pass
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                break
                
    async def _handle_message(self, data: dict):
        """Handle incoming order book update messages"""
        event_type = data.get("event_type")
        
        if event_type == "book":
            # Full order book snapshot
            token_id = data.get("asset_id")
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            if token_id:
                self.full_orderbook[token_id] = {"bids": bids, "asks": asks}
                
                if bids:
                    self.bid_cache[token_id] = float(bids[0].get("price", 0))
                if asks:
                    self.ask_cache[token_id] = float(asks[0].get("price", 0))
                if bids and asks:
                    self.mid_cache[token_id] = (self.bid_cache[token_id] + self.ask_cache[token_id]) / 2
                elif bids:
                    self.mid_cache[token_id] = self.bid_cache[token_id]
                elif asks:
                    self.mid_cache[token_id] = self.ask_cache[token_id]
                    
                # Trigger callbacks for real-time PnL updates
                if token_id in self._price_callbacks:
                    for callback in self._price_callbacks[token_id]:
                        try:
                            await callback(token_id, self.get_best_bid(token_id), self.get_best_ask(token_id))
                        except Exception as e:
                            logging.error(f"Callback error for {token_id[:12]}: {e}")
                            
                logging.debug(f"📚 Order book update: {token_id[:12]}... bid={self.bid_cache.get(token_id,0):.4f} ask={self.ask_cache.get(token_id,0):.4f}")
                
        elif event_type == "update":
            # Incremental order book update
            token_id = data.get("asset_id")
            updates = data.get("updates", [])
            
            if token_id and token_id in self.full_orderbook:
                for update in updates:
                    side = update.get("side")
                    price = float(update.get("price", 0))
                    size = float(update.get("size", 0))
                    
                    if side == "buy":
                        # Update bids
                        bids = self.full_orderbook[token_id].get("bids", [])
                        # Find and update or insert
                        updated = False
                        for i, bid in enumerate(bids):
                            if float(bid.get("price", 0)) == price:
                                if size == 0:
                                    bids.pop(i)
                                else:
                                    bids[i] = {"price": price, "size": size}
                                updated = True
                                break
                        if not updated and size > 0:
                            bids.append({"price": price, "size": size})
                            bids.sort(key=lambda x: float(x["price"]), reverse=True)
                        self.full_orderbook[token_id]["bids"] = bids
                        
                    elif side == "sell":
                        # Update asks
                        asks = self.full_orderbook[token_id].get("asks", [])
                        updated = False
                        for i, ask in enumerate(asks):
                            if float(ask.get("price", 0)) == price:
                                if size == 0:
                                    asks.pop(i)
                                else:
                                    asks[i] = {"price": price, "size": size}
                                updated = True
                                break
                        if not updated and size > 0:
                            asks.append({"price": price, "size": size})
                            asks.sort(key=lambda x: float(x["price"]))
                        self.full_orderbook[token_id]["asks"] = asks
                
                # Update best bid/ask
                if self.full_orderbook[token_id].get("bids"):
                    self.bid_cache[token_id] = float(self.full_orderbook[token_id]["bids"][0]["price"])
                if self.full_orderbook[token_id].get("asks"):
                    self.ask_cache[token_id] = float(self.full_orderbook[token_id]["asks"][0]["price"])
                if self.bid_cache.get(token_id, 0) and self.ask_cache.get(token_id, 0):
                    self.mid_cache[token_id] = (self.bid_cache[token_id] + self.ask_cache[token_id]) / 2
                    
                # Trigger callbacks for real-time PnL
                if token_id in self._price_callbacks:
                    for callback in self._price_callbacks[token_id]:
                        try:
                            await callback(token_id, self.get_best_bid(token_id), self.get_best_ask(token_id))
                        except Exception as e:
                            logging.error(f"Callback error: {e}")


# ==================== DATA CLASS with WAP Tracking ====================
@dataclass
class Position:
    market_id: str
    question: str
    outcome: str
    token_id: str
    side: str
    entry_price: float          # Weighted Average Price (WAP)
    size_usd: float
    shares: float
    source_wallet: str
    source_name: str
    order_type: str = "LIMIT"
    status: str = "open"
    exit_price: float = 0.0
    pnl: float = 0.0
    opened_at: datetime = field(default_factory=datetime.now)
    peak_price: float = 0.0
    current_price: float = 0.0
    current_best_bid: float = 0.0
    current_best_ask: float = 0.0
    
    # WAP tracking for DCA/ladder strategies
    total_shares_filled: float = 0.0
    total_cost_usd: float = 0.0
    fill_history: List[dict] = field(default_factory=list)  # Track individual fills
    
    # Real-time PnL tracking
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    last_update_time: datetime = field(default_factory=datetime.now)

    def update_wap(self, new_shares: float, new_price: float):
        """
        Update Weighted Average Price (WAP) when new fills occur.
        This is critical for DCA and ladder strategies.
        """
        new_cost = new_shares * new_price
        self.total_shares_filled += new_shares
        self.total_cost_usd += new_cost
        self.shares = self.total_shares_filled
        self.size_usd = self.total_cost_usd
        self.entry_price = self.total_cost_usd / self.total_shares_filled if self.total_shares_filled > 0 else 0
        
        # Record fill for audit
        self.fill_history.append({
            "shares": new_shares,
            "price": new_price,
            "cost": new_cost,
            "timestamp": datetime.now().isoformat()
        })
        
        logging.info(f"📊 WAP Updated: {self.question[:35]} | New WAP: ${self.entry_price:.4f} | Total Shares: {self.shares:.4f}")

    def update_realized_pnl(self, exit_shares: float, exit_price: float):
        """
        Update realized PnL when partially or fully closing a position.
        Uses FIFO or average cost method based on WAP.
        """
        if exit_shares > self.shares:
            exit_shares = self.shares
            
        cost_basis = exit_shares * self.entry_price
        proceeds = exit_shares * exit_price
        realized = proceeds - cost_basis
        
        self.realized_pnl += realized
        self.shares -= exit_shares
        self.size_usd -= cost_basis
        self.total_shares_filled -= exit_shares
        self.total_cost_usd -= cost_basis
        
        # Recalculate WAP if still have shares
        if self.shares > 0:
            self.entry_price = self.total_cost_usd / self.total_shares_filled
        
        logging.info(f"💰 Realized PnL: ${realized:+.2f} | Total Realized: ${self.realized_pnl:+.2f} | Remaining Shares: {self.shares:.4f}")
        return realized

    def calculate_unrealized_pnl(self, current_best_bid: float, current_best_ask: float) -> float:
        """
        Calculate current unrealized PnL based on live order book best bid/ask.
        Uses best bid for LONG positions (what you can sell at now).
        """
        if self.status != "open" or self.shares <= 0:
            return self.unrealized_pnl
            
        # For long positions, use best bid (exit price)
        if self.side == "BUY":
            exit_price = current_best_bid if current_best_bid > 0 else self.current_price
        else:
            # For short positions, use best ask
            exit_price = current_best_ask if current_best_ask > 0 else self.current_price
            
        if exit_price <= 0:
            return self.unrealized_pnl
            
        self.unrealized_pnl = (exit_price - self.entry_price) * self.shares
        self.current_price = exit_price
        self.current_best_bid = current_best_bid
        self.current_best_ask = current_best_ask
        self.last_update_time = datetime.now()
        
        return self.unrealized_pnl

    def total_pnl(self) -> float:
        """Combined realized + unrealized PnL"""
        if self.status == "closed":
            return self.pnl
        return self.realized_pnl + self.unrealized_pnl

    def pnl_pct(self) -> float:
        """Return PnL percentage based on current price"""
        if self.status != "open" or self.entry_price <= 0:
            return 0.0
        price = self.current_best_bid if self.current_best_bid > 0 else self.current_price
        if price <= 0:
            return 0.0
        return (price - self.entry_price) / self.entry_price * 100.0

    def current_value(self) -> float:
        """Current market value based on best bid"""
        if self.status != "open" or self.shares <= 0:
            return 0.0
        price = self.current_best_bid if self.current_best_bid > 0 else self.current_price
        return self.shares * price


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = BANKROLL_FALLBACK
        self.last_update = 0
        self._breakdown: dict[str, float] = {}

    def _call_payload(self, contract: str, wallet: str) -> dict:
        padded = wallet.lower().replace("0x", "").zfill(64)
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": contract, "data": BALANCE_OF_SELECTOR + padded}, "latest"],
        }

    async def _query_contract(
        self,
        session: aiohttp.ClientSession,
        rpc_url: str,
        label: str,
        contract: str,
        decimals: int,
        wallet: str,
    ) -> Optional[float]:
        try:
            payload = self._call_payload(contract, wallet)
            async with session.post(
                rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logging.debug(f"  {label}: HTTP {resp.status} — {text[:120]}")
                    return None
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = json.loads(text)
                if "error" in data:
                    logging.debug(f"  {label}: RPC error {data['error']}")
                    return None
                hex_val = data.get("result") or "0x0"
                if not hex_val or hex_val in ("0x", "0x0"):
                    return 0.0
                amount = int(hex_val, 16) / (10 ** decimals)
                logging.debug(f"  {label}: ${amount:.6f}")
                return amount
        except Exception as e:
            logging.debug(f"  {label} @ {rpc_url}: {e}")
            return None

    async def _fetch_rpc(self, session: aiohttp.ClientSession, wallet: str) -> Optional[float]:
        logging.debug(f"Fetching balance for wallet {wallet}")
        for rpc_url in POLYGON_RPCS:
            total = 0.0
            breakdown: dict[str, float] = {}
            rpc_alive = False

            for label, contract, decimals in PUSD_CONTRACTS:
                amount = await self._query_contract(
                    session, rpc_url, label, contract, decimals, wallet
                )
                if amount is None:
                    continue
                rpc_alive = True
                if amount > 0:
                    breakdown[label] = amount
                    total += amount

            if rpc_alive:
                self._breakdown = breakdown
                if total > 0:
                    parts = ", ".join(f"{k}=${v:.4f}" for k, v in breakdown.items())
                    logging.info(f"RPC balance: ${total:.4f} ({parts}) via {rpc_url}")
                else:
                    logging.info(f"RPC balance: $0.0000 (all contracts zero) via {rpc_url}")
                return total

            logging.warning(f"RPC {rpc_url} unreachable — trying next")

        return None

    async def _fetch_polymarket_api(
        self, session: aiohttp.ClientSession, wallet: str
    ) -> Optional[float]:
        try:
            url = f"https://data-api.polymarket.com/value?user={wallet}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                if isinstance(data, (int, float)):
                    value = float(data)
                elif isinstance(data, dict):
                    value = float(
                        data.get("portfolioValue")
                        or data.get("value")
                        or data.get("balance")
                        or 0
                    )
                else:
                    return None

                if value >= 0:
                    logging.info(f"Polymarket API balance: ${value:.4f}")
                    return value
        except Exception as e:
            logging.warning(f"Polymarket API balance fetch failed: {e}")
        return None

    async def get(self, session: aiohttp.ClientSession, force: bool = False) -> float:
        global peak_bankroll

        cache_age = time.time() - self.last_update
        if not force and cache_age < 60 and self.cached_balance > 0:
            return self.cached_balance

        if not YOUR_WALLET:
            logging.warning("DEPOSIT_WALLET_ADDRESS not set — using cached/fallback balance")
            return self.cached_balance
        if not YOUR_WALLET.startswith("0x") or len(YOUR_WALLET) != 42:
            logging.error(f"DEPOSIT_WALLET_ADDRESS looks invalid: '{YOUR_WALLET}'")
            return self.cached_balance
        logging.debug(f"Balance check for {YOUR_WALLET}")

        fetched = await self._fetch_rpc(session, YOUR_WALLET)

        if fetched is None:
            fetched = await self._fetch_polymarket_api(session, YOUR_WALLET)

        if fetched is not None:
            self.cached_balance = fetched
            self.last_update = time.time()
            if fetched > peak_bankroll:
                peak_bankroll = fetched
        else:
            logging.warning("All balance sources failed — using cached $%.4f", self.cached_balance)

        return self.cached_balance

    def adjust(self, delta: float) -> None:
        self.cached_balance = max(0.0, self.cached_balance + delta)
        logging.debug(f"Balance adjusted by {delta:+.4f} → ${self.cached_balance:.4f}")


# ==================== EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self._client = None

        if not dry_run:
            self._init_client()

    def _init_client(self):
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key=CLOB_API_KEY,
                api_secret=CLOB_SECRET,
                api_passphrase=CLOB_PASSPHRASE,
            )
            self._client = ClobClient(
                host="https://clob.polymarket.com",
                key=YOUR_PRIVATE_KEY,
                chain_id=137,
                creds=creds,
            )
            logging.info("CLOB client initialised (LIVE mode)")
        except ImportError:
            logging.error("py-clob-client not installed. Run: pip install py-clob-client")
            raise
        except Exception as e:
            logging.error(f"CLOB client init failed: {e}")
            raise

    async def place_limit_buy(
        self,
        token_id: str,
        amount_usd: float,
        best_ask: float,
    ) -> tuple[bool, str, float]:
        amount_usd = min(round(amount_usd, 2), 1.0)
        limit_price = round(max(best_ask - LIMIT_ORDER_TICK, 0.01), 4)
        shares = round(amount_usd / limit_price, 4)

        if self.dry_run:
            fake_id = f"dry-lmt-{int(time.time())}-{token_id[:8]}"
            logging.info(
                f"[DRY RUN] LIMIT BUY {shares:.4f} sh @ {limit_price:.4f} "
                f"(${amount_usd:.2f}) → {fake_id}"
            )
            return True, fake_id, limit_price

        if not self._client:
            return False, "client_not_initialised", 0

        try:
            from py_clob_client.clob_types import LimitOrderArgs, OrderType

            order_args = LimitOrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side="BUY",
            )
            signed_order = self._client.create_limit_order(order_args)
            resp = self._client.post_order(signed_order, OrderType.GTC)

            order_id = resp.get("orderID", "") or resp.get("id", "")
            status = resp.get("status", "unknown")

            if status in ("matched", "live", "open"):
                logging.info(
                    f"[LIVE] Limit order live: {order_id} "
                    f"price={limit_price:.4f} shares={shares:.4f} status={status}"
                )
                return True, order_id, limit_price
            else:
                logging.warning(f"[LIVE] Limit order rejected: {resp}")
                return False, status, 0

        except Exception as e:
            logging.error(f"place_limit_buy error: {e}")
            return False, str(e), 0

    async def place_sell(
        self,
        token_id: str,
        shares: float,
        price: float,
    ) -> tuple[bool, str, float]:
        limit_price = round(min(price + LIMIT_ORDER_TICK, 0.99), 4)

        if self.dry_run:
            fake_id = f"dry-sell-{int(time.time())}-{token_id[:8]}"
            logging.info(
                f"[DRY RUN] LIMIT SELL {shares:.4f} sh @ {limit_price:.4f} → {fake_id}"
            )
            return True, fake_id, limit_price

        if not self._client:
            return False, "client_not_initialised", 0

        try:
            from py_clob_client.clob_types import LimitOrderArgs, OrderType

            order_args = LimitOrderArgs(
                token_id=token_id,
                price=limit_price,
                size=shares,
                side="SELL",
            )
            signed_order = self._client.create_limit_order(order_args)
            resp = self._client.post_order(signed_order, OrderType.GTC)

            order_id = resp.get("orderID", "") or resp.get("id", "")
            status = resp.get("status", "unknown")

            if status in ("matched", "live", "open"):
                logging.info(f"[LIVE] Limit sell live: {order_id} status={status}")
                return True, order_id, limit_price
            else:
                logging.warning(f"[LIVE] Limit sell rejected: {resp}")
                return False, status, 0

        except Exception as e:
            logging.error(f"place_sell error: {e}")
            return False, str(e), 0


# ==================== COPY TRADER with Real-Time PnL ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.positions: Dict[str, Position] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.balance = RobustBalanceManager()
        self.ws_manager = RealTimeWebSocketManager()
        self._pnl_update_queue: deque = deque(maxlen=1000)  # Track recent PnL updates

    # ========== PnL Methods ==========
    
    async def on_price_update(self, token_id: str, best_bid: float, best_ask: float):
        """
        Callback triggered on every WebSocket price update.
        Updates PnL in real-time (millisecond latency).
        """
        for pos_key, pos in self.positions.items():
            if pos.token_id == token_id and pos.status == "open":
                old_pnl = pos.unrealized_pnl
                new_pnl = pos.calculate_unrealized_pnl(best_bid, best_ask)
                
                # Track PnL change for logging
                self._pnl_update_queue.append({
                    "token_id": token_id,
                    "old_pnl": old_pnl,
                    "new_pnl": new_pnl,
                    "timestamp": datetime.now()
                })
                
                # Check for stop loss / take profit conditions on every price update
                await self._check_risk_conditions(pos, best_bid, best_ask)
                
                if abs(new_pnl - old_pnl) > 0.01:  # Only log meaningful changes
                    logging.debug(f"⚡ Real-time PnL | {pos.question[:30]} | ${new_pnl:+.2f} (Δ: ${new_pnl - old_pnl:+.2f})")
    
    async def _check_risk_conditions(self, pos: Position, best_bid: float, best_ask: float):
        """
        Check stop loss and take profit conditions on every price update.
        TP/SL decisions are based on real liquidity and actual market depth.
        """
        if pos.status != "open":
            return
            
        current_price = best_bid if pos.side == "BUY" else best_ask
        
        if current_price <= 0:
            return
            
        # Update peak price for trailing stop
        if current_price > pos.peak_price:
            pos.peak_price = current_price
            
        # Hard stop loss (50% below entry)
        if current_price <= pos.entry_price * (1 - STOP_LOSS):
            logging.warning(f"🔴 STOP LOSS TRIGGERED | {pos.question[:40]} | Entry: ${pos.entry_price:.4f} Current: ${current_price:.4f}")
            # Signal to close position (handled in main loop)
            pos.pnl = pos.unrealized_pnl  # Store current PnL
            pos.status = "closing"  # Mark for closure
            
        # Trailing stop (25% below peak)
        elif pos.peak_price > 0 and current_price <= pos.peak_price * (1 - TRAIL_STOP):
            logging.warning(f"🔴 TRAILING STOP TRIGGERED | {pos.question[:40]} | Peak: ${pos.peak_price:.4f} Current: ${current_price:.4f}")
            pos.pnl = pos.unrealized_pnl
            pos.status = "closing"

    def get_wallet_stats(self) -> Dict[str, dict]:
        stats: Dict[str, dict] = {}
        for pos in self.positions.values():
            wallet = pos.source_wallet
            if wallet not in stats:
                stats[wallet] = {
                    "profit_trades": 0,
                    "loss_trades": 0,
                    "total_pnl": 0.0,
                    "wallet_name": pos.source_name,
                }
            pnl_val = pos.total_pnl()
            stats[wallet]["total_pnl"] += pnl_val
            if pnl_val >= 0:
                stats[wallet]["profit_trades"] += 1
            else:
                stats[wallet]["loss_trades"] += 1
        
        for wallet in stats:
            total = stats[wallet]["profit_trades"] + stats[wallet]["loss_trades"]
            stats[wallet]["win_rate"] = (stats[wallet]["profit_trades"] / total * 100.0) if total > 0 else 0.0
        
        return stats

    def total_unrealized_pnl(self) -> float:
        return sum(pos.unrealized_pnl for pos in self.positions.values() if pos.status == "open")

    def total_realized_pnl(self) -> float:
        return sum(pos.realized_pnl for pos in self.positions.values())

    def total_pnl(self) -> float:
        return self.total_unrealized_pnl() + self.total_realized_pnl()

    def total_open_value(self) -> float:
        return sum(pos.current_value() for pos in self.positions.values() if pos.status == "open")

    def check_daily_loss(self):
        """Track daily loss and halt bot if limit exceeded."""
        global daily_loss_today, last_loss_reset_date, bot_paused_until
        
        today = datetime.now().strftime("%Y-%m-%d")
        if today != last_loss_reset_date:
            daily_loss_today = 0.0
            last_loss_reset_date = today
            logging.info(f"Daily loss counter reset for {today}")
        
        # Calculate total realized loss today
        for pos in self.positions.values():
            if pos.status == "closed" and pos.pnl < 0:
                if pos.opened_at.date() == datetime.now().date():
                    daily_loss_today += abs(pos.pnl)
        
        if daily_loss_today >= DAILY_LOSS_LIMIT:
            halt_bot()

    # -------- helpers --------

    def _trade_size(self, bankroll: float, wallet_addr: str, mid_price: float) -> float:
        config = WALLETS[wallet_addr]
        risk_type = config.get("risk_type", "fixed")

        if risk_type == "price_based":
            fraction = max(0.05, min(mid_price, 1.0)) * MAX_TRADE_FRAC
        else:
            fraction = config.get("fixed_risk", MAX_TRADE_FRAC)

        fraction = max(MIN_TRADE_FRAC, min(fraction, MAX_TRADE_FRAC))
        size = round(bankroll * fraction, 2)

        headroom = max(0.0, bankroll * MAX_EXPOSURE - self._total_exposure())
        size = min(size, round(headroom, 2))

        return round(size, 2)

    def _total_exposure(self) -> float:
        return sum(p.size_usd for p in self.positions.values() if p.status == "open")

    def _check_drawdown(self, bankroll: float) -> bool:
        global bot_paused_until
        if peak_bankroll <= 0:
            return False
        drawdown = (peak_bankroll - bankroll) / peak_bankroll
        if drawdown >= MAX_DRAWDOWN:
            bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
            logging.warning(
                f"Drawdown {drawdown:.1%} ≥ {MAX_DRAWDOWN:.1%} — "
                f"pausing until {bot_paused_until:%Y-%m-%d %H:%M}"
            )
            return True
        return False

    # -------- API calls --------

    async def get_positions(self, session: aiohttp.ClientSession, wallet_addr: str):
        try:
            url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=100"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    logging.warning(f"positions API {r.status} for {wallet_addr[:10]}…")
                    return None
                data = await r.json()

                cleaned = []
                for p in (data if isinstance(data, list) else []):
                    token_id = p.get("asset")
                    if not token_id:
                        continue

                    value = float(p.get("currentValue") or p.get("value") or 0)
                    shares = float(p.get("size") or p.get("shares") or 0)

                    if value == 0 and shares > 0:
                        price_hint = float(p.get("price") or p.get("lastTradePrice") or 0)
                        value = shares * price_hint

                    if value < MIN_SOURCE_SIZE:
                        continue

                    raw_side = (p.get("side") or "").upper()
                    if raw_side in ("BUY", "SELL"):
                        side = raw_side
                    else:
                        side = "SELL" if value < 0 else "BUY"

                    cleaned.append({
                        "asset":   token_id,
                        "title":   p.get("title", "Unknown Market"),
                        "outcome": p.get("outcome", "YES"),
                        "side":    side,
                        "value":   abs(value),
                        "shares":  abs(shares),
                    })
                return cleaned
        except Exception as e:
            logging.error(f"Fetch positions error ({wallet_addr[:10]}…): {e}")
            return None

    async def get_orderbook(
        self, session: aiohttp.ClientSession, token_id: str
    ) -> tuple[float, float]:
        try:
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    bb = float(bids[0]["price"]) if bids else 0.0
                    ba = float(asks[0]["price"]) if asks else 0.0
                    return bb, ba
        except Exception as e:
            logging.warning(f"get_orderbook error for {token_id[:12]}…: {e}")
        return 0.0, 0.0

    # -------- source wallet tracking --------

    async def _get_source_position(
        self,
        session: aiohttp.ClientSession,
        wallet_addr: str,
        token_id: str,
    ) -> Optional[dict]:
        try:
            url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=100"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    logging.warning(f"_get_source_position: API {r.status} for {wallet_addr[:10]}…")
                    return {"_network_error": True}
                data = await r.json()
                positions = data if isinstance(data, list) else []
                match = next((p for p in positions if p.get("asset") == token_id), None)
                if match is None:
                    return None
                value = float(match.get("currentValue") or match.get("value") or match.get("size") or 0)
                shares = float(match.get("size") or match.get("shares") or 0)
                raw_side = (match.get("side") or "").upper()
                side = raw_side if raw_side in ("BUY", "SELL") else ("SELL" if value < 0 else "BUY")
                return {
                    "asset":   token_id,
                    "title":   match.get("title", ""),
                    "outcome": match.get("outcome", "YES"),
                    "side":    side,
                    "value":   abs(value),
                    "shares":  abs(shares),
                }
        except Exception as e:
            logging.warning(f"_get_source_position error ({wallet_addr[:10]}…): {e}")
            return {"_network_error": True}

    # ========== REAL-TIME PnL UPDATE (WebSocket driven) ==========
    
    async def update_positions_pnl_realtime(self):
        """
        Real-time PnL is handled by WebSocket callbacks.
        This method ensures all positions are subscribed and initializes PnL.
        """
        if not self.positions:
            return
        
        open_positions = [p for p in self.positions.values() if p.status == "open"]
        
        for pos in open_positions:
            # Subscribe to real-time order book updates
            await self.ws_manager.subscribe(pos.token_id)
            
            # Register callback for this position
            self.ws_manager.register_callback(pos.token_id, self.on_price_update)
            
            # Initialize with current best bid/ask if available
            best_bid = self.ws_manager.get_best_bid(pos.token_id)
            best_ask = self.ws_manager.get_best_ask(pos.token_id)
            
            if best_bid > 0 and best_ask > 0:
                pos.calculate_unrealized_pnl(best_bid, best_ask)
                logging.info(f"📊 Initialized real-time PnL for {pos.question[:35]}: ${pos.unrealized_pnl:+.2f}")

    # -------- execute + refresh --------

    async def _execute_and_refresh(
        self,
        session: aiohttp.ClientSession,
        action: str,
        token_id: str,
        shares: float,
        size_usd: float,
        price: float,
        best_ask: float = 0.0,
    ) -> tuple[bool, str, float]:
        if action == "BUY":
            ask = best_ask if best_ask > 0 else price
            ok, oid, exec_price = await self.executor.place_limit_buy(token_id, size_usd, ask)
        else:
            ok, oid, exec_price = await self.executor.place_sell(token_id, shares, price)

        if ok:
            delta = -size_usd if action == "BUY" else size_usd
            self.balance.adjust(delta)
            asyncio.ensure_future(self.balance.get(session, force=True))

        return ok, oid, exec_price

    # -------- exit scanner --------

    async def scan_for_exits(self, session: aiohttp.ClientSession):
        wallet_snapshot: dict[str, dict] = {}
        wallet_error: set[str] = set()

        for wallet_addr in set(p.source_wallet for p in self.positions.values() if p.status == "open"):
            try:
                url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=500"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        logging.warning(f"scan_for_exits: API {r.status} for {wallet_addr[:10]}… — holding all")
                        wallet_error.add(wallet_addr)
                        continue
                    data = await r.json()
                    positions = data if isinstance(data, list) else []
                    wallet_snapshot[wallet_addr] = {p.get("asset"): p for p in positions if p.get("asset")}
                    logging.info(f"EXIT-SCAN fetched {len(wallet_snapshot[wallet_addr])} positions for {wallet_addr[:10]}…")
            except Exception as e:
                logging.warning(f"scan_for_exits fetch error ({wallet_addr[:10]}…): {e} — holding all")
                wallet_error.add(wallet_addr)

        to_close: list[tuple[str, Position, float, str]] = []

        for pos_key, pos in list(self.positions.items()):
            if pos.status != "open":
                continue
            
            # Also check positions marked "closing" from real-time risk checks
            if pos.status == "closing":
                to_close.append((pos_key, pos, pos.current_price, "real_time_risk"))
                continue

            if pos.source_wallet in wallet_error:
                continue

            snapshot = wallet_snapshot.get(pos.source_wallet, {})
            source_raw = snapshot.get(pos.token_id)

            # Use real-time WebSocket prices
            current_bid = self.ws_manager.get_best_bid(pos.token_id)
            if current_bid <= 0:
                current_bid, _ = await self.get_orderbook(session, pos.token_id)

            if current_bid > 0:
                pos.current_best_bid = current_bid
                pos.current_price = current_bid
                if pos.peak_price <= 0:
                    pos.peak_price = pos.entry_price
                if current_bid > pos.peak_price:
                    pos.peak_price = current_bid
                    
                # Update PnL
                pos.calculate_unrealized_pnl(current_bid, self.ws_manager.get_best_ask(pos.token_id))

            src_val = float(source_raw.get("currentValue") or source_raw.get("value") or 0) if source_raw else 0
            logging.info(
                f"  CHECK {pos.question[:45]} | "
                f"bid={current_bid:.3f} entry={pos.entry_price:.3f} peak={pos.peak_price:.3f} "
                f"PnL=${pos.unrealized_pnl:+.2f} source={'$'+str(round(src_val,2)) if source_raw else 'EXITED'}"
            )

            reason = None

            if source_raw is None:
                reason = "source_exited"
            elif current_bid > 0 and current_bid <= pos.entry_price * (1 - STOP_LOSS):
                reason = f"stop_loss_50% (entry={pos.entry_price:.3f} now={current_bid:.3f})"
            elif current_bid > 0 and pos.peak_price > 0 and current_bid <= pos.peak_price * (1 - TRAIL_STOP):
                reason = f"trail_stop_25% (peak={pos.peak_price:.3f} now={current_bid:.3f})"

            if reason:
                to_close.append((pos_key, pos, current_bid or pos.entry_price, reason))

        for pos_key, pos, exit_price, reason in to_close:
            ok, _, exec_price = await self._execute_and_refresh(
                session, "SELL", pos.token_id, pos.shares, pos.size_usd, exit_price
            )
            if ok:
                # Unsubscribe from WebSocket before closing
                await self.ws_manager.unsubscribe(pos.token_id)
                
                # Update realized PnL
                pos.update_realized_pnl(pos.shares, exec_price)
                pos.status = "closed"
                pos.exit_price = exec_price
                pos.pnl = pos.realized_pnl
                
                logging.info(
                    f"CLOSED [{reason}] {pos.question[:50]} | "
                    f"entry={pos.entry_price:.3f} WAP exit={pos.exit_price:.3f} "
                    f"realized_pnl=${pos.realized_pnl:+.2f}"
                )
                del self.positions[pos_key]

    # -------- main scan loop --------

    async def scan_and_copy(self):
        global bot_paused_until, peak_bankroll

        if bot_paused_until and datetime.now() < bot_paused_until:
            remaining = bot_paused_until - datetime.now()
            logging.info(f"Bot paused — {remaining} remaining")
            return

        async with aiohttp.ClientSession() as session:
            # Start WebSocket manager if not already running
            if not self.ws_manager._running:
                await self.ws_manager.start(session)
            
            bankroll = await self.balance.get(session, force=True)
            if bankroll < 1.0:
                logging.warning(f"Bankroll too low (${bankroll:.4f}) — skipping cycle")
                return

            if bankroll > peak_bankroll:
                peak_bankroll = bankroll

            if self._check_drawdown(bankroll):
                return

            open_count = sum(1 for p in self.positions.values() if p.status == "open")
            logging.info(
                f"Scanning | bankroll=${bankroll:.4f} | peak=${peak_bankroll:.4f} | "
                f"open={open_count}/{MAX_POSITIONS} | "
                f"exposure=${self._total_exposure():.2f} | "
                f"total_pnl=${self.total_pnl():+.2f}"
            )

            # Update real-time PnL subscriptions
            await self.update_positions_pnl_realtime()
            
            # Check daily loss limit
            self.check_daily_loss()
            
            await self.scan_for_exits(session)

            for wallet_addr, config in WALLETS.items():
                raw = await self.get_positions(session, wallet_addr)
                if not raw:
                    continue

                for pos in raw:
                    token_id = pos["asset"]
                    question = pos["title"]
                    side = pos["side"]
                    pos_key = f"{wallet_addr}_{token_id}_{side}"

                    if pos_key in self.positions:
                        continue

                    if pos["value"] < MIN_SOURCE_SIZE:
                        continue

                    # Get real-time prices from WebSocket cache
                    best_bid = self.ws_manager.get_best_bid(token_id)
                    best_ask = self.ws_manager.get_best_ask(token_id)
                    
                    if best_bid <= 0 or best_ask <= 0:
                        best_bid, best_ask = await self.get_orderbook(session, token_id)
                    
                    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask

                    if mid_price <= 0.01:
                        logging.debug(f"Skipping {token_id[:12]}… — no price")
                        continue

                    my_size = self._trade_size(bankroll, wallet_addr, mid_price)

                    if my_size <= 0:
                        available = bankroll * MAX_EXPOSURE - self._total_exposure()
                        logging.info(
                            f"Exposure cap — skipping {question[:50]} "
                            f"(size=${my_size:.2f} headroom=${available:.2f})"
                        )
                        continue

                    shares = round(my_size / mid_price, 4)

                    ok, order_id, exec_price = await self._execute_and_refresh(
                        session, side, token_id, shares, my_size, mid_price,
                        best_ask=best_ask,
                    )

                    if ok:
                        new_position = Position(
                            market_id="",
                            question=question,
                            outcome=pos["outcome"],
                            side=side,
                            token_id=token_id,
                            entry_price=exec_price,
                            size_usd=my_size,
                            shares=shares,
                            source_wallet=wallet_addr,
                            source_name=config["name"],
                            order_type="LIMIT",
                            peak_price=exec_price,
                            current_price=exec_price,
                            current_best_bid=best_bid,
                            current_best_ask=best_ask,
                            total_shares_filled=shares,
                            total_cost_usd=my_size,
                        )
                        self.positions[pos_key] = new_position
                        
                        # Subscribe to real-time WebSocket feed
                        await self.ws_manager.subscribe(token_id)
                        self.ws_manager.register_callback(token_id, self.on_price_update)
                        
                        logging.info(
                            f"COPIED [{config['name']}] LIMIT {side} ${my_size:.2f} "
                            f"@ {exec_price:.3f} ({shares:.4f} shares) → {question[:50]}"
                        )

    async def run(self):
        logging.info(f"🚀 Bot started | dry_run={self.dry_run} | wallets={len(WALLETS)}")
        logging.info(f"🔌 Using REAL-TIME WebSocket price feed (millisecond latency)")
        logging.info(f"📐 PnL formula: (current_best_bid - WAP) × shares")
        logging.info(f"📊 WAP tracking enabled for DCA/ladder strategies")
        logging.info(f"🛑 Daily loss limit: ${DAILY_LOSS_LIMIT}")
        logging.info(f"🎯 Stop loss: {STOP_LOSS*100}% | Trailing stop: {TRAIL_STOP*100}%")
        try:
            while True:
                try:
                    await self.scan_and_copy()
                except Exception as e:
                    logging.error(f"Loop error: {e}", exc_info=True)
                await asyncio.sleep(POLL_INTERVAL)
        finally:
            await self.ws_manager.stop()


# ==================== DASHBOARD SERVER ====================
def run_dashboard():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            if self.path == "/":
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                try:
                    bankroll = bot.balance.cached_balance
                    open_positions = [p for p in bot.positions.values() if p.status == "open"]
                    closed_positions = [p for p in bot.positions.values() if p.status == "closed"]
                    
                    total_pnl = bot.total_pnl()
                    total_open_pnl = bot.total_unrealized_pnl()
                    total_closed_pnl = bot.total_realized_pnl()
                    wallet_stats = bot.get_wallet_stats()
                    
                    def get_pnl_color(pnl):
                        if pnl > 0:
                            return "#4ade80"
                        elif pnl < 0:
                            return "#f87171"
                        return "#e0e0e0"
                    
                    def format_pnl(pnl):
                        return f"${pnl:+.2f}"
                    
                    open_rows = ""
                    for p in open_positions:
                        pnl_color = get_pnl_color(p.unrealized_pnl)
                        current_display = f"{p.current_best_bid:.4f}" if p.current_best_bid > 0 else f"{p.current_price:.4f}"
                        
                        open_rows += f"""
                        <tr>
                            <td style="font-family:monospace">{p.source_name}</td>
                            <td style="max-width:300px; overflow:hidden; text-overflow:ellipsis;">{p.question[:60]}</td>
                            <td style="color: {'#4ade80' if p.side == 'BUY' else '#f87171'}">{p.side}</td>
                            <td style="font-family:monospace">{p.outcome}</td>
                            <td style="font-family:monospace">${p.size_usd:.2f}<br><span style="font-size:10px;color:#888">{p.shares:.4f} sh</span></td>
                            <td style="font-family:monospace">{p.entry_price:.4f}<br><span style="font-size:10px;color:#888">WAP</span></td>
                            <td style="font-family:monospace">{current_display}</td>
                            <td style="color: {pnl_color}; font-weight: bold;">{format_pnl(p.unrealized_pnl)}<br><span style="font-size:10px">{p.pnl_pct():+.1f}%</span></td>
                        </tr>
                        """
                    
                    closed_rows = ""
                    for p in closed_positions:
                        pnl_color = get_pnl_color(p.pnl)
                        closed_rows += f"""
                        <tr>
                            <td style="font-family:monospace">{p.source_name}</td>
                            <td style="max-width:300px; overflow:hidden; text-overflow:ellipsis;">{p.question[:60]}</td>
                            <td style="color: {'#4ade80' if p.side == 'BUY' else '#f87171'}">{p.side}</td>
                            <td style="font-family:monospace">{p.outcome}</td>
                            <td style="font-family:monospace">${p.size_usd:.2f}<br><span style="font-size:10px;color:#888">{p.shares:.4f} sh</span></td>
                            <td style="font-family:monospace">{p.entry_price:.4f}</td>
                            <td style="font-family:monospace">{p.exit_price:.4f}</td>
                            <td style="color: {pnl_color}; font-weight: bold;">{format_pnl(p.pnl)}</td>
                        </tr>
                        """
                    
                    leaderboard_rows = ""
                    sorted_wallets = sorted(
                        wallet_stats.items(),
                        key=lambda x: (x[1]["profit_trades"], x[1]["total_pnl"]),
                        reverse=True
                    )
                    for rank, (wallet, stats) in enumerate(sorted_wallets, 1):
                        short_wallet = f"{wallet[:8]}..."
                        win_rate_display = f"{stats['win_rate']:.1f}%" if stats['win_rate'] > 0 else "—"
                        leaderboard_rows += f"""
                        <tr>
                            <td style="font-family:monospace">#{rank}</td>
                            <td style="font-family:monospace">{short_wallet}</td>
                            <td style="font-family:monospace">{stats['profit_trades']}</td>
                            <td style="font-family:monospace">{stats['loss_trades']}</td>
                            <td style="font-family:monospace">{win_rate_display}</td>
                            <td style="font-family:monospace; color:{'#4ade80' if stats['total_pnl'] >= 0 else '#f87171'}">${stats['total_pnl']:+.2f}</td>
                        </tr>
                        """
                    
                    pause_str = (
                        f"Paused until {bot_paused_until.strftime('%H:%M %d-%b')}"
                        if bot_paused_until and datetime.now() < bot_paused_until
                        else "Active"
                    )
                    
                    drawdown_pct = 0
                    if peak_bankroll > 0:
                        drawdown_pct = ((peak_bankroll - bankroll) / peak_bankroll) * 100
                    
                    html = f"""<!DOCTYPE html>
                    <html>
                    <head>
                        <meta charset="utf-8">
                        <title>CopyTrader - Real-Time PnL</title>
                        <meta http-equiv="refresh" content="30">
                        <style>
                            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 20px; background: #0a0a0a; color: #e0e0e0; }}
                            .container {{ max-width: 1400px; margin: 0 auto; }}
                            h1 {{ color: #ffffff; margin-bottom: 20px; font-size: 28px; border-left: 4px solid #6366f1; padding-left: 15px; }}
                            .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px; }}
                            .stat-card {{ background: #1a1a1a; padding: 15px 20px; border-radius: 8px; border: 1px solid #2a2a2a; }}
                            .stat-label {{ font-size: 12px; text-transform: uppercase; color: #888; letter-spacing: 1px; margin-bottom: 8px; }}
                            .stat-value {{ font-size: 28px; font-weight: bold; }}
                            .stat-value.positive {{ color: #4ade80; }}
                            .stat-value.negative {{ color: #f87171; }}
                            .stat-value.warning {{ color: #fbbf24; }}
                            .section {{ background: #0f0f0f; border-radius: 8px; margin-bottom: 25px; border: 1px solid #1f1f1f; overflow: hidden; }}
                            .section-header {{ background: #1a1a1a; padding: 12px 20px; border-bottom: 1px solid #2a2a2a; font-weight: bold; font-size: 18px; }}
                            .section-header span {{ color: #6366f1; }}
                            table {{ width: 100%; border-collapse: collapse; }}
                            th {{ background: #141414; padding: 12px; text-align: left; font-size: 13px; font-weight: 600; color: #aaa; border-bottom: 1px solid #2a2a2a; }}
                            td {{ padding: 10px 12px; border-bottom: 1px solid #1f1f1f; font-size: 13px; }}
                            tr:hover {{ background: #151515; }}
                            .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
                            .badge-active {{ background: #064e3b; color: #4ade80; }}
                            .badge-paused {{ background: #7c2d12; color: #f97316; }}
                            .status-open {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; background: #1e3a5f; color: #60a5fa; }}
                            .status-closed {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; background: #3f3f46; color: #a1a1aa; }}
                            .footer {{ margin-top: 20px; text-align: center; color: #666; font-size: 12px; }}
                            hr {{ border: none; border-top: 1px solid #2a2a2a; margin: 20px 0; }}
                            .live-badge {{ background: #dc2626; color: white; padding: 2px 8px; border-radius: 4px; font-size: 10px; margin-left: 10px; animation: pulse 1.5s infinite; }}
                            @keyframes pulse {{ 0% {{ opacity: 1; }} 50% {{ opacity: 0.5; }} 100% {{ opacity: 1; }} }}
                        </style>
                    </head>
                    <body>
                        <div class="container">
                            <h1>📊 Multi-Wallet Copy Trader <span class="live-badge">🔴 LIVE WebSocket</span></h1>
                            
                            <div class="stats">
                                <div class="stat-card">
                                    <div class="stat-label">Mode</div>
                                    <div class="stat-value" style="font-size: 20px;">
                                        <span class="badge {'badge-active' if not bot.dry_run else ''}" style="background: {'#064e3b' if not bot.dry_run else '#3f3f46'}; color: {'#4ade80' if not bot.dry_run else '#a1a1aa'}">
                                            {'🔴 LIVE' if not bot.dry_run else '🟡 DRY RUN'}
                                        </span>
                                    </div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Bot Status</div>
                                    <div class="stat-value" style="font-size: 20px;">
                                        <span class="badge {'badge-active' if pause_str == 'Active' else 'badge-paused'}">
                                            {pause_str}
                                        </span>
                                    </div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Bankroll</div>
                                    <div class="stat-value">${bankroll:.2f}</div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Peak Bankroll</div>
                                    <div class="stat-value">${peak_bankroll:.2f}</div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Drawdown</div>
                                    <div class="stat-value {'warning' if drawdown_pct > 10 else ''}">{drawdown_pct:.1f}%</div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Open Positions</div>
                                    <div class="stat-value">{len(open_positions)}</div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Total PnL</div>
                                    <div class="stat-value {'positive' if total_pnl > 0 else 'negative' if total_pnl < 0 else ''}">
                                        {format_pnl(total_pnl)}
                                    </div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Unrealized PnL</div>
                                    <div class="stat-value {'positive' if total_open_pnl > 0 else 'negative' if total_open_pnl < 0 else ''}">
                                        {format_pnl(total_open_pnl)}
                                    </div>
                                </div>
                                <div class="stat-card">
                                    <div class="stat-label">Realized PnL</div>
                                    <div class="stat-value {'positive' if total_closed_pnl > 0 else 'negative' if total_closed_pnl < 0 else ''}">
                                        {format_pnl(total_closed_pnl)}
                                    </div>
                                </div>
                            </div>
                            
                            <div class="section">
                                <div class="section-header">
                                    🏆 Wallet Leaderboard <span>(by wins → total P&L)</span>
                                </div>
                                <div style="overflow-x: auto;">
                                    <table>
                                        <thead>
                                            <tr><th>Rank</th><th>Wallet</th><th>Wins</th><th>Losses</th><th>Win Rate</th><th>Total P&amp;L</th></tr>
                                        </thead>
                                        <tbody>
                                            {leaderboard_rows if leaderboard_rows else '<tr><td colspan="6" style="text-align:center; padding:40px;">📭 No trades yet</td</tr>'}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                            
                            <div class="section">
                                <div class="section-header">
                                    📈 OPEN POSITIONS <span>(Real-Time PnL)</span>
                                </div>
                                <div style="overflow-x: auto;">
                                    <table>
                                        <thead>
                                            <tr><th>Source</th><th>Market</th><th>Side</th><th>Outcome</th><th>Size (USD/Shares)</th><th>Entry (WAP)</th><th>Best Bid</th><th>Unrealized PnL</th></tr>
                                        </thead>
                                        <tbody>
                                            {open_rows if open_rows else '<tr><td colspan="8" style="text-align:center; padding:40px;">📭 No open positions</td</tr>'}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                            
                            <div class="section">
                                <div class="section-header">
                                    📉 CLOSED POSITIONS
                                </div>
                                <div style="overflow-x: auto;">
                                    <table>
                                        <thead>
                                            <tr><th>Source</th><th>Market</th><th>Side</th><th>Outcome</th><th>Size (USD/Shares)</th><th>Entry</th><th>Exit</th><th>Realized PnL</th></tr>
                                        </thead>
                                        <tbody>
                                            {closed_rows if closed_rows else '<tr><td colspan="8" style="text-align:center; padding:40px;">📭 No closed positions</td</tr>'}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                            
                            <div class="footer">
                                <hr>
                                <p>⚡ Real-time WebSocket price feed | 📐 PnL = (best_bid - WAP) × shares | 🔄 Auto-refresh every 30s</p>
                                <p>📍 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                            </div>
                        </div>
                    </body>
                    </html>"""
                    
                    self.wfile.write(html.encode('utf-8'))
                except Exception as exc:
                    self.wfile.write(f"<html><body><h2>Error</h2><pre>{exc}</pre></body></html>".encode('utf-8'))
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
    logging.info(f"Dashboard on http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()


# ==================== ENTRY POINT ====================
async def main():
    global bot
    threading.Thread(target=run_dashboard, daemon=True).start()
    bot = CopyTrader(dry_run=DRY_RUN)
    await bot.run()


if __name__ == "__main__":
    bot = None  # type: ignore
    asyncio.run(main())
