import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any
from config.config_loader import load_config
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges

from execution.exchange.hyperliquid import HyperliquidExchange
from execution.execution_service import ExecutionService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class QueueMessageCollector:
    """Helper class to collect and verify messages published to a queue."""
    
    def __init__(self):
        self.messages = []
        
    def collect_message(self, message):
        """Callback to collect messages from a queue."""
        logger.info(f"Collected message: {message}")
        self.messages.append(message)
        
    def clear(self):
        """Clear collected messages."""
        self.messages = []
        
    def get_messages_by_type(self, message_type: str) -> List[Dict[str, Any]]:
        """Filter messages by type."""
        return [msg for msg in self.messages if msg.get('type') == message_type]
    
    def get_messages_by_order_id(self, order_id: str) -> List[Dict[str, Any]]:
        """Filter messages by order ID."""
        return [msg for msg in self.messages if msg.get('order_id') == order_id]
    
    def print_messages(self):
        """Print all collected messages."""
        for i, msg in enumerate(self.messages):
            logger.info(f"Message {i+1}: {msg}")

async def run_execution_test():
    """
    Test the execution layer by simulating:
    1. Processing a signal into an order
    2. Creating the order
    3. Canceling the order
    
    Verify that appropriate events are published to the queue.
    """
    try:
        # Load configuration
        config = load_config()
        
        # Initialize dependencies
        exchange = HyperliquidExchange(config)
        consumer_queue = QueueService()
        producer_queue = QueueService()
        listener_queue = QueueService()
        
        # Set up a listener on the producer_queue to capture published messages
        message_collector = QueueMessageCollector()
        
        # Initialize cache service
        cache_service = CacheService()
        
        # Initialize execution service
        execution_service = ExecutionService(
            exchange=exchange,
            consumer_queue=consumer_queue,
            producer_queue=producer_queue,
            cache_service=cache_service,
            config=config
        )
        
        # Start the execution service
        await execution_service.start()
        
        # Set up queue to listen for order events
        listener_queue.declare_exchange(Exchanges.EXECUTION)
        listener_queue.declare_queue("test_orders_capture")
        
        # Bind to capture all order events
        listener_queue.bind_queue(
            Exchanges.EXECUTION,
            "test_orders_capture",
            "order.#"  # Listen for all order-related events
        )
        
        # Subscribe to the queue to collect messages
        listener_queue.subscribe(
            "test_orders_capture",
            message_collector.collect_message
        )
        
        logger.info("Starting execution layer test...")
        
        # Step 1: Create a mock signal
        signal_id = f"test_signal_{int(datetime.now().timestamp())}"
        mock_signal = {
            "id": signal_id,
            "symbol": "BTC-USD",
            "direction": "long",
            "signal_type": "entry",
            "price_target": 65000.00,
            "stop_loss": 63000.00,
            "take_profit": 70000.00,
            "position_size": 0.01,
            "confidence_score": 0.9,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Created mock signal: {signal_id}")
        
        # Step 2: Process the signal to get order parameters
        logger.info("Processing signal into order parameters...")
        order_params = await execution_service.process_signal(mock_signal)
        
        if not order_params:
            logger.error("Failed to process signal into order parameters")
            return False
        
        logger.info(f"Signal processed successfully: {order_params}")
        
        # Create a test order with a known ID (for easier tracking)
        # We'll override the exchange's create_order method to return a predictable result
        order_id = f"test_order_{int(datetime.now().timestamp())}"
        
        # Save the original method
        original_create_order = exchange.create_order
        
        # Override create_order to return a controlled result
        async def mock_create_order(*args, **kwargs):
            return {
                "id": order_id,
                "symbol": kwargs["symbol"],
                "side": kwargs["side"],
                "type": kwargs["order_type"],
                "amount": kwargs["amount"],
                "price": kwargs["price"],
                "status": "open",
                "info": {}
            }
        
        # Replace the method
        exchange.create_order = mock_create_order
        
        # Step 3: Execute the order
        logger.info(f"Executing order with parameters: {order_params}")
        order_result = await execution_service.execute_order(order_params)
        
        if not order_result:
            logger.error("Failed to execute order")
            return False
        
        logger.info(f"Order executed successfully: {order_result}")
        
        # Wait a bit for message processing
        await asyncio.sleep(2)
        
        # Check if the order creation event was published
        creation_messages = message_collector.get_messages_by_type("created")
        if not creation_messages:
            logger.error("No order creation events were published")
            return False
        
        order_messages = message_collector.get_messages_by_order_id(order_id)
        if not order_messages:
            logger.error(f"No events for order {order_id} were published")
            return False
        
        logger.info(f"Order creation event published successfully: {order_messages[0]}")
        
        # Clear collected messages before next test
        message_collector.clear()
        
        # Step 4: Cancel the order
        logger.info(f"Cancelling order: {order_id}")
        
        # Override cancel_order to return a controlled result
        async def mock_cancel_order(*args, **kwargs):
            return {
                "id": kwargs["id"],
                "symbol": kwargs["symbol"],
                "status": "cancelled"
            }
        
        # Replace the method
        exchange.cancel_order = mock_cancel_order
        
        # Cancel the order
        cancel_result = await execution_service.cancel_order(order_id, "BTC-USD")
        
        if not cancel_result:
            logger.error("Failed to cancel order")
            return False
        
        logger.info(f"Order cancelled successfully: {cancel_result}")
        
        # Wait a bit for message processing
        await asyncio.sleep(2)
        
        # Check if the order cancellation event was published
        cancellation_messages = message_collector.get_messages_by_type("cancelled")
        if not cancellation_messages:
            logger.error("No order cancellation events were published")
            return False
        
        logger.info(f"Order cancellation event published successfully: {cancellation_messages[0]}")
        
        # Print all collected messages for review
        logger.info("All collected messages:")
        message_collector.print_messages()
        
        # Restore original methods
        exchange.create_order = original_create_order
        
        # Test passed
        logger.info("Execution layer test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error during execution test: {e}", exc_info=True)
        return False
    finally:
        # Clean up resources
        if 'execution_service' in locals():
            await execution_service.stop()
        
        if 'producer_queue' in locals():
            producer_queue.stop()
        
        if 'consumer_queue' in locals():
            consumer_queue.stop()
        
        if 'cache_service' in locals():
            cache_service.close()

async def main():
    """Main entry point for the test script."""
    result = await run_execution_test()
    if result:
        logger.info("✅ TEST PASSED: Execution layer is functioning correctly")
    else:
        logger.error("❌ TEST FAILED: Execution layer is not functioning correctly")

if __name__ == "__main__":
    asyncio.run(main())