import asyncio
import logging
import sys
from typing import Dict, Any
from datetime import datetime, timezone

# Import necessary services and components
from config.config_loader import load_config
from back_testing_engine import BackTestingEngine, BackTestConfiguration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_service(
    config: Dict[str, Any],
    backtest_config: BackTestConfiguration
):
    """
    Initialize and run the backtesting service.
    
    Args:
        config: Global configuration dictionary
        backtest_config: Backtesting-specific configuration
    """
    try:
        # Create backtesting engine
        backtesting_engine = BackTestingEngine(
            config=config,
            backtest_config=backtest_config
        )
        
        # Start the service (includes initialization)
        await backtesting_engine.start()
        
        # Run the backtest
        results = await backtesting_engine.run_backtest()
        
        # Log results summary
        logger.info("=" * 60)
        logger.info("BACKTESTING COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Results: {results}")
        logger.info("=" * 60)
        
        return results
        
    except Exception as e:
        logger.error(f"Fatal error in backtesting service: {e}", exc_info=True)
        raise
    finally:
        # Graceful shutdown
        if 'backtesting_engine' in locals():
            await backtesting_engine.stop()

async def main():
    """
    Main entry point for the backtesting application.
    Initializes all required services and runs the backtesting engine.
    """
    try:
        # Load configuration
        config = load_config()
        
        # Create backtest configuration from config file or command line args
        # TODO: These parameters should come from config file or CLI arguments
        backtest_config = BackTestConfiguration(
            symbol=config.get('backtest', {}).get('symbol', 'BTCUSDT'),
            timeframe=config.get('backtest', {}).get('timeframe', '1h'),
            exchange=config.get('backtest', {}).get('exchange', 'binance'),
            start_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2023, 12, 31, tzinfo=timezone.utc),
            initial_capital=config.get('backtest', {}).get('initial_capital', 100000.0),
            **config.get('backtest', {})
        )
        
        logger.info("Starting Backtesting Application...")
        logger.info(f"Symbol: {backtest_config.symbol}")
        logger.info(f"Timeframe: {backtest_config.timeframe}")
        logger.info(f"Exchange: {backtest_config.exchange}")
        logger.info(f"Period: {backtest_config.start_time} to {backtest_config.end_time}")
        logger.info(f"Initial Capital: ${backtest_config.initial_capital:,.2f}")
        
        # Run the backtesting service
        results = await run_service(
            config=config,
            backtest_config=backtest_config
        )
        
        logger.info("Backtesting application completed successfully")
        return results
    
    except Exception as e:
        logger.error(f"Fatal error in main application: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())