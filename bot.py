"""
Polymarket Copy Trading Bot - Ready for Render
"""
import asyncio
import logging
import signal
import sys
import os

# ==================== CONFIG ====================
TARGET_WALLETS = [
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e",
    "0xa1795199a227f8d68134f30bf26314a9918c9629"
]

COPY_SETTINGS = {
    "copy_mode": "balance_percentage",
    "balance_percentage": 1.0,
    "max_trade_size_usdc": 2,
    "min_trade_size_usdc": 0.01,
    "slippage_tolerance": 0.02,
}

POLYMARKET_PROXY_ADDRESS = os.getenv("POLYMARKET_PROXY_ADDRESS")
if not POLYMARKET_PROXY_ADDRESS:
    raise ValueError("❌ POLYMARKET_PROXY_ADDRESS environment variable is missing!")

# ====================================================

from activity_watcher import ActivityWatcher
from trade_copier import TradeCopier

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


class CopyTradingBot:
    def __init__(self):
        logger.info("Initializing Copy Trading Bot")
        self.copier_address = POLYMARKET_PROXY_ADDRESS
        self.target_addresses = [addr.lower() for addr in TARGET_WALLETS]

        self.queue = asyncio.Queue(maxsize=1000)
        
        self.watcher = ActivityWatcher(self.queue, self.target_addresses)
        self.copier = TradeCopier(
            queue=self.queue,
            copier_address=self.copier_address,
            copy_settings=COPY_SETTINGS
        )
        
        self._shutdown_event = asyncio.Event()

    async def start(self):
        logger.info("🚀 Polymarket Copy Bot Started on Render (1% of balance) 🚀")

        watcher_task = asyncio.create_task(self.watcher.start(), name="Watcher")
        copier_task = asyncio.create_task(self.copier.start(), name="Copier")

        try:
            await self._shutdown_event.wait()
        finally:
            logger.info("Shutting down...")
            await self.watcher.stop()
            await self.copier.stop()
            watcher_task.cancel()
            copier_task.cancel()
            await asyncio.gather(watcher_task, copier_task, return_exceptions=True)
            logger.info("Bot stopped.")


async def main():
    bot = CopyTradingBot()

    def signal_handler(sig, frame):
        logger.info(f"Signal {sig} received")
        bot._shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
