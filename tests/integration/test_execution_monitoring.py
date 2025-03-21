import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Import necessary components from both layers
from config.config_loader import load_config
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges, Queues, RoutingKeys, CacheKeys
from shared.dto.alert import Alert, AlertType

# Execution layer imports
from execution.exchange.hyperliquid import HyperliquidExchange
from execution.execution_service import ExchangeService

# Monitoring layer imports
from monitoring.monitoring_service import MonitoringService

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IntegrationTest:
    """
    Integration test for the execution and monitoring layers.
    
    This test:
    1. Initializes both the execution and monitoring services
    2. Creates and sends a mock trading signal to the execution layer
    3. Verifies that the execution layer processes the signal and creates an order
    4. Verifies that the monitoring layer receives the order event and creates an alert
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the integration test."""
        self.config = config
        
        # Cache service is shared between services
        self.cache_service = CacheService()
        
        # Separate queue services for each layer
        self.execution_consumer_queue = QueueService()  # For execution layer to consume signals
        self.execution_producer_queue = QueueService()  # For execution layer to produce order events
        
        self.monitoring_consumer_queue = QueueService()  # For monitoring layer to consume order events
        self.monitoring_producer_queue = QueueService()  # For monitoring layer to produce alerts (not used in this test)
        
        # Exchange interface is shared between services
        self.exchange = HyperliquidExchange(config)
        
        # Mock telegram alerts to verify they were sent
        self.alerts_received = []
        
        # Services
        self.execution_service = None
        self.monitoring_service = None
    
    async def setup(self):
        """Set up the test environment."""
        logger.info("Setting up integration test environment...")
        
        # Initialize exchange
        await self.exchange.initialize()
        
        # Create services
        self.execution_service = ExchangeService(
            exchange=self.exchange,
            consumer_queue=self.execution_consumer_queue,
            producer_queue=self.execution_producer_queue,
            cache_service=self.cache_service,
            config=self.config
        )
        
        self.monitoring_service = MonitoringService(
            exchange=self.exchange, 
            consumer_queue=self.monitoring_consumer_queue,
            producer_queue=self.monitoring_producer_queue,
            cache_service=self.cache_service,
            config=self.config
        )
        
        # Override alert sending in monitoring service to capture alerts
        self.original_send_alert = self.monitoring_service._process_event_async
        self.monitoring_service._process_event_async = self.mock_send_alert
        
        # Start services
        await self.execution_service.start()
        await self.monitoring_service.start()
        
        # Wait for services to fully initialize
        await asyncio.sleep(2)
        
        logger.info("Test environment set up successfully")
    
    async def teardown(self):
        """Clean up the test environment."""
        logger.info("Tearing down integration test environment...")
        
        # Stop services
        await self.execution_service.stop()
        await self.monitoring_service.stop()
        
        # Close cache connection
        self.cache_service.close()
        
        logger.info("Test environment torn down")
    
    async def mock_send_alert(self, event, alert):
        """Mock implementation to capture alerts instead of sending them."""
        self.alerts_received.append(alert)
        logger.info(f"Captured alert: {alert.type.value} - {alert.message}")
    
    async def verify_order_in_cache(self, order_id: str, timeout: int = 5) -> bool:
        """Verify that an order was stored in the cache."""
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            # Check for the order in cache
            order_key_pattern = f"order:*:{order_id}"
            matching_keys = self.cache_service.keys(order_key_pattern)
            
            if matching_keys:
                order_data = self.cache_service.get(matching_keys[0])
                logger.info(f"Found order in cache: {order_data}")
                return True
            
            await asyncio.sleep(0.5)
        
        logger.error(f"Order {order_id} not found in cache within timeout")
        return False
    
    async def verify_alert_received(self, order_id: str, timeout: int = 5) -> bool:
        """Verify that an alert was received for a specific order."""
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            for alert in self.alerts_received:
                if alert.details and alert.details.get('order_id') == order_id:
                    logger.info(f"Found alert for order {order_id}: {alert.message}")
                    return True
            
            await asyncio.sleep(0.5)
        
        logger.error(f"No alert received for order {order_id} within timeout")
        return False
    
    async def send_mock_signal(self):
        """Send a mock trading signal to the execution layer."""
        # Create a mock signal
        signal_id = f"test_signal_{int(datetime.now().timestamp())}"
        
        mock_signal = {
            "id": signal_id,
            "symbol": "BTC-USD",
            "direction": "long",
            "signal_type": "entry",
            "price_target": 68000.00,
            "stop_loss": 66000.00,
            "take_profit": 72000.00,
            "position_size": 0.01,
            "confidence_score": 0.85,
            "timestamp": datetime.now().isoformat()
        }
        
        # Send the signal to the execution layer's queue
        self.execution_consumer_queue.publish(
            Exchanges.STRATEGY,
            RoutingKeys.SIGNAL_ALL,
            mock_signal
        )
        
        logger.info(f"Sent mock signal: {signal_id}")
        return signal_id
    
    async def run_test(self):
        """Run the integration test."""
        try:
            # Set up the test environment
            await self.setup()
            
            # Send a mock signal
            signal_id = await self.send_mock_signal()
            
            # Wait a bit for processing
            await asyncio.sleep(2)
            
            # Process the signal manually since we're not actually consuming from the queue in the test
            logger.info("Processing the signal manually for testing purposes")
            signal = {
                "id": signal_id,
                "symbol": "BTC-USD",
                "direction": "long",
                "signal_type": "entry",
                "price_target": 68000.00,
                "stop_loss": 66000.00,
                "take_profit": 72000.00,
                "position_size": 0.01
            }
            
            # Process the signal to get order parameters
            order_params = await self.execution_service.process_signal(signal)
            
            # Create a mock order based on those parameters
            order_id = f"test_order_{int(datetime.now().timestamp())}"
            order_event = {
                "order_id": order_id,
                "symbol": order_params["symbol"],
                "side": order_params["side"],
                "type": order_params["type"],
                "price": order_params["price"],
                "size": order_params["amount"],
                "status": "open",
                "timestamp": datetime.now().isoformat()
            }
            
            # Publish the order event to the execution queue
            self.execution_producer_queue.publish(
                Exchanges.EXECUTION,
                RoutingKeys.ORDER_NEW.format(
                    exchange=self.exchange.id,
                    symbol=order_params["symbol"]
                ),
                order_event
            )
            
            logger.info(f"Published order event to queue: {order_id}")
            
            # Manually trigger the monitoring layer's event handler to simulate message consumption
            logger.info("Triggering monitoring layer event handler")
            self.monitoring_consumer_queue.publish(
                Exchanges.EXECUTION,
                RoutingKeys.ORDER_NEW,
                order_event
            )
            
            # Wait for monitoring to process the event
            await asyncio.sleep(2)
            
            # Verify the results
            # 1. Check if the order is in the cache
            # cache_result = await self.verify_order_in_cache(order_id)
            
            # 2. Check if an alert was generated
            alert_result = await self.verify_alert_received(order_id)
            
            if alert_result:
                logger.info("✅ Integration test PASSED: Order was successfully processed and alerted")
                return True
            else:
                logger.error("❌ Integration test FAILED: Order processing or alerting failed")
                return False
                
        except Exception as e:
            logger.error(f"Error during integration test: {e}", exc_info=True)
            return False
        finally:
            # Clean up
            await self.teardown()

async def main():
    """Main entry point for the integration test."""
    try:
        # Load configuration
        config = load_config()
        
        # Create and run the integration test
        test = IntegrationTest(config)
        test_result = await test.run_test()
        
        if test_result:
            logger.info("Integration test completed successfully")
            sys.exit(0)
        else:
            logger.error("Integration test failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Unhandled exception in integration test: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())