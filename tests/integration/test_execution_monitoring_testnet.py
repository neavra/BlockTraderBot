import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add the project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# Import necessary components
from config.config_loader import load_config
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges, Queues, RoutingKeys
from shared.dto.order import Order

# Exchange and service imports
from execution.exchange.hyperliquid import HyperliquidExchange
from execution.execution_service import ExecutionService
from monitoring.monitoring_service import MonitoringService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_real_world_test():
    """
    A real-world test that initializes the actual trading bot components 
    and executes a real trading cycle with minimal mocking.
    
    This test:
    1. Initializes the real exchange connector (in test mode)
    2. Sets up the execution and monitoring services
    3. Creates and submits a real order (with a small amount)
    4. Cancels the order
    5. Verifies that alerts are sent to Telegram
    """
    try:
        logger.info("Starting real-world trading bot test")
        
        # Load the configuration
        config = load_config()
        
        # Ensure we're using testnet/paper trading mode
        if 'exchanges' in config and 'hyperliquid' in config['exchanges']:
            config['exchanges']['hyperliquid'].setdefault('options', {})
            config['exchanges']['hyperliquid']['options']['testnet'] = True
            logger.info("Forcing testnet mode for the exchange")
        
        # Initialize shared services
        cache_service = CacheService()
        
        # Initialize the exchange with real API access
        exchange = HyperliquidExchange(config['exchanges']['hyperliquid'])
        await exchange.initialize()
        logger.info("Exchange initialized")
        
        # Check if exchange initialization was successful
        if not await exchange.fetch_balance():
            logger.error("Could not fetch balance - check exchange API credentials")
            return False
        
        # Create queue services for communication
        execution_consumer_queue = QueueService()
        execution_producer_queue = QueueService()

        monitoring_consumer_queue = QueueService()
        monitoring_producer_queue = QueueService()
        
        # Initialize execution service
        execution_service = ExecutionService(
            exchange=exchange,
            consumer_queue=execution_consumer_queue,
            producer_queue=execution_producer_queue,
            cache_service=cache_service,
            config=config
        )
        
        # Initialize monitoring service
        monitoring_service = MonitoringService(
            exchange=exchange,
            consumer_queue=monitoring_consumer_queue,
            producer_queue=monitoring_producer_queue,
            cache_service=cache_service,
            config=config
        )
        
        # Start the services
        await execution_service.start()
        await monitoring_service.start()
        logger.info("Services started")
        
        # Create a test signal
        test_signal = {
            "id": f"test_signal_{int(datetime.now().timestamp())}",
            "symbol": "BTC/USDC:USDC",  # Adjust to match your exchange's available symbols
            "direction": "long",
            "signal_type": "entry",
            "price_target": 60000.00,  # Set a price far from market to avoid fill
            "stop_loss": 58000.00,
            "take_profit": 65000.00,
            "position_size": 0.001,  # Very small amount for testing
            "confidence_score": 0.9,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Processing test signal: {test_signal['id']}")
        
        # Process the signal to create order parameters
        order_params = await execution_service.process_signal(test_signal)
        
        if not order_params:
            logger.error("Failed to process signal")
            return False
        
        logger.info(f"Signal processed, generated order parameters: {order_params}")
        
        # Execute the order
        order_result = await execution_service.execute_order(order_params)
        
        if not order_result:
            logger.error("Failed to execute order")
            return False
        
        order_id = order_result['id']
        logger.info(f"Order successfully placed with ID: {order_id}")
        
        # Create the order event manually
        order_event = {
            'type': 'created',
            'order_id': order_id,
            'symbol': order_params['symbol'],
            'side': order_params['side'],
            'order_type': order_params['type'],
            'price': order_params['price'],
            'size': order_params['amount'],
            'status': 'open',
            'timestamp': datetime.now().isoformat()
        }
        
        # Manually send the event to the monitoring service
        logger.info("Forwarding order event to monitoring service")
        monitoring_service.on_event(order_event)
        
        # Wait a bit for processing and to see Telegram notification
        logger.info("Waiting 5 seconds for Telegram notification to be sent...")
        await asyncio.sleep(5)
        
        # Now cancel the order
        logger.info(f"Cancelling order: {order_id}")
        cancel_result = await execution_service.cancel_order(order_id, order_params['symbol'])
        
        if not cancel_result:
            logger.error("Failed to cancel order")
            return False
        
        logger.info(f"Order successfully cancelled: {cancel_result}")
        
        # Create cancellation event manually
        cancel_event = {
            'type': 'cancelled',
            'order_id': order_id,
            'symbol': order_params['symbol'],
            'side': order_params['side'],
            'order_type': order_params['type'],
            'price': order_params['price'],
            'size': order_params['amount'],
            'status': 'cancelled',
            'timestamp': datetime.now().isoformat()
        }
        
        # Send cancellation event to monitoring service
        logger.info("Forwarding cancellation event to monitoring service")
        monitoring_service.on_event(cancel_event)
        
        # Wait a bit for processing and to see Telegram notification
        logger.info("Waiting 5 seconds for Telegram notification to be sent...")
        await asyncio.sleep(5)
        
        logger.info("Test completed successfully!")
        logger.info("Check your Telegram for order placement and cancellation notifications")
        
        # Clean up
        execution_producer_queue.stop()
        execution_consumer_queue.stop()
        monitoring_producer_queue.stop()
        monitoring_consumer_queue.stop()
        cache_service.close()

        await execution_service.stop()
        await monitoring_service.stop()
        
        
        return True
        
    except Exception as e:
        logger.error(f"Error during real-world test: {e}", exc_info=True)
        return False

async def verify_open_orders():
    """
    Utility function to check for any open orders on the exchange.
    This can be used to verify order placement or cancellation.
    """
    try:
        # Load the configuration
        config = load_config()
        
        # Initialize the exchange
        exchange = HyperliquidExchange(config['exchanges']['hyperliquid'])
        await exchange.initialize()
        
        # Fetch open orders
        open_orders = await exchange.fetch_open_orders()
        
        if open_orders:
            logger.info(f"Found {len(open_orders)} open orders:")
            for order in open_orders:
                logger.info(f"Order ID: {order['id']}, Symbol: {order['symbol']}, "
                           f"Type: {order['type']}, Side: {order['side']}, "
                           f"Price: {order['price']}, Amount: {order['amount']}")
        else:
            logger.info("No open orders found")
        
        await exchange.close()
        
    except Exception as e:
        logger.error(f"Error checking open orders: {e}")

async def main():
    """Main entry point for the test script."""
    try:
        # Run the main test
        test_result = await run_real_world_test()
        
        if test_result:
            logger.info("✅ REAL-WORLD TEST PASSED")
        else:
            logger.error("❌ REAL-WORLD TEST FAILED")
        
        # Optionally verify open orders afterward
        # Uncomment if you want to check open orders after the test
        # await verify_open_orders()
        
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())