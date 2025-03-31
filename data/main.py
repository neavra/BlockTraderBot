import asyncio
import logging
import sys
from typing import Dict, Any

# Import necessary services and components
from data.logs.logging import setup_logging
from config.config_loader import load_config
from database.db import Database
from managers.candle_manager import CandleManager
from data.data_service import DataService

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)

async def run_service(
    config: Dict[str, Any]
):
    """
    Initialize and run the data service.
    
    Args:
        database: Database connection instance
        candle_manager: Manager for handling candle data
        config: Configuration dictionary
    """
    try:
        # Create data service
        data_service = DataService(
            config=config
        )
        
        # Start the service
        success = await data_service.start()
        if not success:
            logger.error("Failed to start data service")
            return
        
        # Keep the service running
        try:
            # This could be replaced with more sophisticated service management
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Service shutdown initiated")
        finally:
            # Graceful shutdown
            await data_service.stop()
    
    except Exception as e:
        logger.error(f"Fatal error in data service: {e}", exc_info=True)

async def main():
    """
    Main entry point for the application.
    Initializes all required services and runs the data service.
    """
    try:
        # Load configuration
        config = load_config()
        # Run the service
        await run_service(
            config=config
        )
    
    except Exception as e:
        logger.error(f"Fatal error in main application: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())