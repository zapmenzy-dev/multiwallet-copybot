"""
Main entry point for Polymarket Copy Trading Bot - Optimized for Render
"""
import asyncio
import logging
import signal
import sys
import os
from config import LOG_LEVEL, LOG_FILE

# ==================== CONFIG FROM ENVIRONMENT VARIABLES ====================
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
    raise ValueError("POLYMARKET_PROXY_ADDRESS environment variable is required!")

# ====================================================

from activity_watcher import ActivityWatcher
from trade_copier import TradeCopier

# Logging
handlers = [logging.StreamHandler(sys.stdout)]
if LOG_FILE:
    handlers.append(logging.FileHandler(LOG_FILE, encoding='utf-8'))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=handlers,
)

logger = logging.getLogger(__name__)
logger.info(f"Bot starting on Render | PID: {os.getpid()}")


class CopyTradingBot:
    def __init__(self):
        logger.info("Initializing copy-trading bot")
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
        logger.info("🚀 Starting Polymarket Copy Bot on Render (1% balance mode) 🚀")

        watcher_task = asyncio.create_task(self.watcher.start(), name="Watcher")
        copier_task = asyncio.create_task(self.copier.start(), name="Copier")

        try:
            await self._shutdown_event.wait()
        finally:
            logger.info("Shutdown signal received...")
            await self.watcher.stop()
            await self.copier.stop()
            watcher_task.cancel()
            copier_task.cancel()
            await asyncio.gather(watcher_task, copier_task, return_exceptions=True)
            logger.info("Bot stopped gracefully")


async def main():
    bot = CopyTradingBot()

    def signal_handler(sig, frame):
        logger.info(f"Signal {sig} received - shutting down")
        bot.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
