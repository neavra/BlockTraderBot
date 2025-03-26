import asyncio
import time
import logging
from datetime import datetime

from config.config_loader import load_config
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges, Queues, RoutingKeys
from shared.domain.dto.alert import Alert, AlertType
from execution.exchange.hyperliquid import HyperliquidExchange

from monitoring.main import run_service

logger = logging.getLogger(__name__)

async def run_test(producer_queue):
    """
    Test function that sends an order event to the Orders queue.
    This simulates an order being created in the execution layer.
    """
    logger.info("Starting test - waiting 3 seconds for setup to complete...")
    await asyncio.sleep(3)  # Give time for the monitoring service to initialize
    
    # Create a test order event
    order_event = {
        "type": "new_order",
        "order_id": "test_order_123",
        "symbol": "BTC-USD",
        "side": "buy",
        "price": 65000.00,
        "size": 0.5,
        "status": "open",
        "timestamp": datetime.now().isoformat()
    }
    
    # Publish the order event to the ORDERS queue
    logger.info(f"Publishing test order event: {order_event}")
    producer_queue.publish(
        Exchanges.EXECUTION,
        RoutingKeys.ORDER_NEW,
        order_event
    )
    
    logger.info("Test order event published. Check for alerts.")
    
    # Keep the test running for a bit to see the results
    await asyncio.sleep(10)
    logger.info("Test completed")

async def main():
    config = load_config()
    # Initialize dependencies
    exchange = HyperliquidExchange(config)
    consumer_queue = QueueService()
    producer_queue = QueueService()
    cache_service = CacheService()

    # Run the monitoring service in a background task
    service_task = asyncio.create_task(
        run_service(
            exchange=exchange,
            consumer_queue=consumer_queue,
            producer_queue=producer_queue,
            cache_service=cache_service,
            config=config
        )
    )
    
    # Run the test in a separate task
    test_task = asyncio.create_task(run_test(producer_queue))
    
    # Wait for the test to complete
    await test_task
    
    # Allow some time for processing before ending
    logger.info("Test finished. Press Ctrl+C to exit.")
    
    # Keep the main task running
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        # Cancel the service task when we're done
        service_task.cancel()
        try:
            await service_task
        except asyncio.CancelledError:
            pass
        logger.info("Monitoring service stopped")

if __name__ == "__main__":
    asyncio.run(main())