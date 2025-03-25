# strategy/main.py
import asyncio
import logging
from typing import Dict, Any

from config.config_loader import load_config
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService

from strategy.strategy_service import StrategyService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_service(
    consumer_queue: QueueService, 
    producer_queue: QueueService,
    cache_service: CacheService,
    config: Dict[str, Any]
):
    """
    Initialize and run the strategy service.
    
    Args:
        consumer_queue: Queue for consuming market data events
        producer_queue: Queue for producing signal events
        cache_service: Cache service for market data
        config: Configuration dictionary
    """
    try:
        # Create strategy service
        strategy_service = StrategyService(
            consumer_queue=consumer_queue,
            producer_queue=producer_queue,
            cache_service=cache_service,
            config=config
        )
        
        # Start the service
        await strategy_service.start()
        
        # Keep the service running
        try:
            # This could be replaced with more sophisticated service management
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Service shutdown initiated")
        finally:
            # Graceful shutdown
            await strategy_service.stop()
    
    except Exception as e:
        logger.error(f"Error running strategy service: {e}", exc_info=True)

async def main():
    """
    Main entry point for the application.
    Initializes all required services and runs the strategy service.
    """
    try:
        # Load configuration
        config = load_config()
        
        # Initialize RabbitMQ queue services
        consumer_queue = QueueService(host='localhost')
        producer_queue = QueueService(host='localhost')
        
        # Initialize Redis cache service
        cache_service = CacheService()
        
        # Run the service
        await run_service(
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