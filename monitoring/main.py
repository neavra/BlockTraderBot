# monitoring_layer/main.py
import asyncio
import logging
import argparse
from config.config_loader import load_config
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from monitoring.monitoring_service import MonitoringService

from execution.exchange.hyperliquid import HyperliquidExchange

logger = logging.getLogger(__name__)

async def run_service(exchange, consumer_queue, producer_queue, cache_service, config):
    try:
        monitoring_service = MonitoringService(
            exchange=exchange,
            consumer_queue=consumer_queue,
            producer_queue=producer_queue,
            cache_service=cache_service,
            config=config
        )
        
        # Start the service (calls async start())
        await monitoring_service.start()

        try:
            # This could be replaced with more sophisticated service management
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Service shutdown initiated")
        finally:
            # Graceful shutdown
            await monitoring_service.stop()
    except Exception as e:
        logger.error(f"Fatal error in main application: {e}", exc_info=True)

async def main():
    config = load_config()
    # Initialize dependencies
    exchange = HyperliquidExchange(config)
    consumer_queue = QueueService()
    producer_queue = QueueService()
    
    cache_service = CacheService()

    await run_service(
            exchange=exchange,
            consumer_queue=consumer_queue,
            producer_queue=producer_queue,
            cache_service=cache_service,
            config=config
        )

if __name__ == "__main__":
    asyncio.run(main())