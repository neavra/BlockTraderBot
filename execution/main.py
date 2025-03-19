import asyncio
import logging
import os
from typing import Dict, Any

# Import necessary services and exchange
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService

from exchange.hyperliquid import HyperliquidExchange
from exchange.exchange_interface import ExchangeInterface
from exchange_service import ExchangeService

from config.config_loader import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_service(
    exchange: ExchangeInterface, 
    consumer_queue: QueueService, 
    producer_queue: QueueService,
    cache_service: CacheService,
    config: Dict[str, Any]
):
    """
    Initialize and run the exchange service.
    
    Args:
        exchange: Hyperliquid exchange interface
        consumer_queue: Queue service for consuming signals
        producer_queue: Queue service for producing order events
        cache_service: Cache service for storing order information
        config: Configuration dictionary
    """
    try:
        # Create exchange service
        exchange_service = ExchangeService(
            exchange=exchange,
            consumer_queue=consumer_queue,
            producer_queue=producer_queue,
            cache_service=cache_service,
            config=config
        )
        
        # Start the service
        await exchange_service.start()
        
        # Keep the service running
        try:
            # This could be replaced with more sophisticated service management
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Service shutdown initiated")
        finally:
            # Graceful shutdown
            await exchange_service.stop()
    
    except Exception as e:
        logger.error(f"Error running exchange service: {e}", exc_info=True)

async def main():
    """
    Main entry point for the application.
    Initializes all required services and runs the exchange service.
    """
    try:
        # Load configuration
        config = load_config()
        
        # Initialize Hyperliquid exchange
        exchange = HyperliquidExchange(config)
        
        # Initialize RabbitMQ queue services
        consumer_queue = QueueService(host='localhost')
        producer_queue = QueueService(host='localhost')
        
        # Initialize Redis cache service
        cache_service = CacheService()
        
        # Run the service
        await run_service(
            exchange=exchange,
            consumer_queue=consumer_queue,
            producer_queue=producer_queue,
            cache_service=cache_service,
            config=config
        )
    
    except Exception as e:
        logger.error(f"Fatal error in main application: {e}", exc_info=True)

if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())