"""
Trade copier that processes activities from queue and copies trades proportionally
"""
import asyncio
import logging

from wallet_tracker import WalletTracker
from blockchain_client import BlockchainClient
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderType, MarketOrderArgs, OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL
from config import CLOB_HOST, CHAIN_ID, PRIVATE_KEY, POLYMARKET_PROXY_ADDRESS, SIGNATURE_TYPE

logger = logging.getLogger(__name__)


class TradeCopier:
    """Copies trades from target trader proportionally"""
    
    def __init__(self, queue: asyncio.Queue, copier_address: str, copy_settings: dict):
        """
        Initialize trade copier

        Args:
            queue: Queue containing activities from the watcher
            copier_address: Address of the copier wallet
            copy_settings: Dict with copy trading settings from bot.py
        """
        self.queue = queue
        self.copier_address = copier_address
        self.copy_settings = copy_settings

        # Extract settings
        self.balance_percentage = copy_settings.get("balance_percentage", 1.0)
        self.max_trade_size_usdc = copy_settings.get("max_trade_size_usdc", None)
        self.min_trade_size_usdc = copy_settings.get("min_trade_size_usdc", 0.01)
        self.slippage_tolerance = copy_settings.get("slippage_tolerance", 0.02)

        self.copier_wallet_tracker = WalletTracker(copier_address)
        # Track multiple target wallets
        self.target_wallet_trackers = {}

        # Initialize CLOB client
        self.client = ClobClient(host=CLOB_HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID, funder=POLYMARKET_PROXY_ADDRESS, signature_type=SIGNATURE_TYPE)
        self.client.set_api_creds(self.client.create_or_derive_api_creds())
        # Initialize blockchain client
        self.blockchain_client = BlockchainClient()
        # Stats
        self._copied_trades = 0
        self._skipped_trades = 0
        self._failed_trades = 0
        self._running = False

    def _get_or_create_target_tracker(self, target_address: str) -> WalletTracker:
        """Get or create a WalletTracker for a target address"""
        if target_address not in self.target_wallet_trackers:
            logger.info("Creating wallet tracker for new target: %s", target_address)
            self.target_wallet_trackers[target_address] = WalletTracker(target_address)
        return self.target_wallet_trackers[target_address]

    async def start(self) -> None:
        """Start the copier loop"""
        if self._running:
            logger.warning("Copier is already running")
            return
        
        self._running = True
        logger.info("Starting trade copier...")
        
        # Initialize copier wallet tracker
        await self.copier_wallet_tracker.start(refresh_interval=1)

        while self._running:
            try:
                try:
                    activity = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    await self._process_activity(activity)
                except asyncio.TimeoutError:
                    continue
            
            except asyncio.CancelledError:
                logger.info("Trade copier cancelled")
                self._running = False
                break
            except Exception as e:
                logger.error(f"Error in copier loop: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    def _get_trading_ratio(self, target_tracker: WalletTracker, condition_id: str, token_id: str) -> float:
        """
        Calculate the proportional ratio between copier and trader wallets,
        adjusted by balance_percentage from copy_settings.
        """
        copier_amount = self.copier_wallet_tracker.get_position(condition_id, token_id)
        trader_amount = target_tracker.get_position(condition_id, token_id)
        
        if trader_amount == 0:
            logger.warning("No trader balance for %s in %s; ratio fallback to 0", token_id, condition_id)
            return 0

        ratio = (copier_amount / trader_amount) * self.balance_percentage
        logger.debug("Ratio: %.4f (copier=%s trader=%s balance_pct=%s)", ratio, copier_amount, trader_amount, self.balance_percentage)
        return ratio

    def _get_proportional_amount(self, original_amount: float, target_tracker: WalletTracker, condition_id: str, token_id: str) -> float:
        ratio = self._get_trading_ratio(target_tracker, condition_id, token_id)
        return original_amount * ratio

    def _apply_trade_size_limits(self, usdc_amount: float) -> float:
        """Apply min/max trade size caps from copy_settings"""
        if self.min_trade_size_usdc and usdc_amount < self.min_trade_size_usdc:
            logger.info("⏩ Trade size $%.4f below min $%.4f, skipping", usdc_amount, self.min_trade_size_usdc)
            return 0
        if self.max_trade_size_usdc and usdc_amount > self.max_trade_size_usdc:
            logger.info("✂️ Capping trade size from $%.4f to max $%.4f", usdc_amount, self.max_trade_size_usdc)
            usdc_amount = self.max_trade_size_usdc
        return usdc_amount

    async def _process_activity(self, activity: dict) -> None:
        activity_type = activity.get("type")  # TRADE, SPLIT, MERGE, REDEEM, REWARD, CONVERSION
        side = activity.get("side")           # BUY or SELL
        condition_id = activity.get("conditionId")
        token_id = activity.get("asset")
        size = float(activity.get("size"))
        usdc_size = float(activity.get("usdcSize"))
        target_address = activity.get("trader", "").lower()

        signed_order, tx, action = None, None, None
        orderType = OrderType.FOK

        # Get or create tracker for this target
        target_tracker = self._get_or_create_target_tracker(target_address)

        # 1. Create copy trade
        if activity_type == "TRADE":

            if side == BUY:
                existing_position = self.copier_wallet_tracker.get_position(condition_id, token_id)
                if existing_position > 0:
                    logger.info("⏩ Already holding %.4f shares of %s, skipping BUY", existing_position, activity.get('eventSlug'))
                    return

                raw_usdc = self._get_proportional_amount(usdc_size, target_tracker, "USDC", "USDC")
                usdc_amount = self._apply_trade_size_limits(raw_usdc)

                if usdc_amount > 0:
                    if usdc_amount > self.copier_wallet_tracker.get_position("USDC", "USDC"):
                        logger.info("⏩ Not enough USDC for BUY order, skipping BUY")
                    elif usdc_amount > 1:
                        # Market order for orders > $1
                        order_args = MarketOrderArgs(
                            token_id=token_id,
                            amount=usdc_amount,
                            side=BUY,
                        )
                        signed_order = self.client.create_market_order(order_args)
                        logger.info(f"📈 BUY ${usdc_amount} of {activity.get('eventSlug')}")
                    else:
                        # Hack limit order for orders <= $1
                        share_amount = self._get_proportional_amount(size, target_tracker, "USDC", "USDC")
                        share_amount = max(share_amount, 1.02)
                        # Apply slippage tolerance to price
                        limit_price = min(0.99, 1.0 - self.slippage_tolerance)
                        order_args = OrderArgs(
                            price=limit_price,
                            size=share_amount,
                            side=BUY,
                            token_id=token_id,
                        )
                        signed_order = self.client.create_order(order_args)
                        orderType = OrderType.GTC
                        logger.info(f"📈 BUY {share_amount} shares of {activity.get('eventSlug')}")
                else:
                    logger.info("⏩ Calculated buy amount is 0, skipping BUY")

            elif side == SELL:
                copier_available = self.copier_wallet_tracker.get_position(condition_id, token_id)
                trader_available = target_tracker.get_position(condition_id, token_id)
                proportional_amount = self._get_proportional_amount(size, target_tracker, condition_id, token_id)

                # Check if trader is selling all his bag or not enough tokens
                if size >= trader_available or proportional_amount > copier_available:
                    amount = copier_available
                else:
                    amount = proportional_amount

                # Apply min/max limits on SELL too (convert shares to usdc approx)
                if amount > 0:
                    order_args = MarketOrderArgs(
                        token_id=token_id,
                        amount=amount,
                        side=SELL,
                    )
                    signed_order = self.client.create_market_order(order_args)
                    logger.info(f"📉 SELL {amount} shares of {activity.get('eventSlug')}")
                else:
                    logger.info("⏩ Calculated sell amount is 0, skipping SELL")
            else:
                logger.warning("Unknown trade side in activity: %s", side)

        elif activity_type == "SPLIT":
            raw_usdc = self._get_proportional_amount(usdc_size, target_tracker, "USDC", "USDC")
            usdc_amount = self._apply_trade_size_limits(raw_usdc)
            if usdc_amount > 0:
                tx, action = self.blockchain_client.split(condition_id, usdc_amount)

        elif activity_type == "MERGE":
            target_mergeable = target_tracker.get_mergeable_amount(condition_id)
            copier_mergeable = self.copier_wallet_tracker.get_mergeable_amount(condition_id)
            if target_mergeable == 0 or copier_mergeable == 0:
                logger.info("⏩ No mergeable positions, skipping MERGE")
            else:
                if size >= target_mergeable:
                    amount = copier_mergeable
                else:
                    ratio = copier_mergeable / target_mergeable
                    amount = size * ratio
                if amount > 0:
                    tx, action = self.blockchain_client.merge(condition_id, amount)
                else:
                    logger.info("⏩ Calculated merge amount is 0, skipping MERGE")
            
        elif activity_type == "REDEEM":
            if target_tracker.is_redeemable(condition_id):
                tx, action = self.blockchain_client.redeem(condition_id)
            else:
                logger.debug("⏩ No redeemable positions, skipping REDEEM")
    
        else:
            logger.debug("⏩ Skipping unsupported activity type: %s", activity_type)
            return

        # 2. Update target positions
        target_tracker.update_positions(activity_type, side, condition_id, token_id, size)

        # 3. Execute copy trade
        if signed_order:            
            resp = self.client.post_order(signed_order, orderType=orderType)
            if not resp or not resp.get("success", False):
                logger.error("CLOB error response: %s", resp)
        if tx and action:
            self.blockchain_client.execute_transaction(tx, action)

    async def stop(self) -> None:
        logger.info("Stopping trade copier")
        self._running = False
    
    def is_running(self) -> bool:
        return self._running
    
    def get_stats(self) -> dict:
        return {
            "running": self._running,
            "copied_trades": self._copied_trades,
            "skipped_trades": self._skipped_trades,
            "failed_trades": self._failed_trades,
            "copier_wallet_tracker": self.copier_wallet_tracker.get_stats(),
            "target_wallet_trackers": {
                addr: tracker.get_stats()
                for addr, tracker in self.target_wallet_trackers.items()
            }
        }
