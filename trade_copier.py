import asyncio
import logging

logger = logging.getLogger(__name__)

class TradeCopier:
    """Copies trades using 1% of balance logic"""
    
    def __init__(self, queue: asyncio.Queue, copier_address: str, copy_settings: dict):
        self.queue = queue
        self.copier_address = copier_address
        self.copy_settings = copy_settings
        self.running = False
        self.stats = {"trades_copied": 0, "total_volume": 0.0}

    async def start(self):
        logger.info("TradeCopier started - Waiting for events...")
        self.running = True
        
        while self.running:
            try:
                event = await self.queue.get()
                
                await self._process_trade(event)
                
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Copier error: {e}")
                await asyncio.sleep(5)

    async def _process_trade(self, event):
        """Calculate 1% of balance and copy trade"""
        try:
            percentage = self.copy_settings.get("balance_percentage", 1.0)
            max_size = self.copy_settings.get("max_trade_size_usdc", 2)
            min_size = self.copy_settings.get("min_trade_size_usdc", 0.01)

            # Simulate getting balance (replace with real balance fetch later)
            simulated_balance = 1000.0  # TODO: Fetch real USDC balance
            trade_amount = (simulated_balance * percentage / 100)

            # Apply min/max
            trade_amount = max(min_size, min(trade_amount, max_size))

            logger.info(f"📊 Copying trade | Amount: ${trade_amount:.2f} | "
                       f"1% of balance | Target: {event.get('trader','Unknown')[:8]}...")

            # TODO: Add real Polymarket trade execution here

            self.stats["trades_copied"] += 1
            self.stats["total_volume"] += trade_amount

        except Exception as e:
            logger.error(f"Failed to copy trade: {e}")

    async def stop(self):
        self.running = False
        logger.info("TradeCopier stopped")

    def get_stats(self):
        return self.stats
