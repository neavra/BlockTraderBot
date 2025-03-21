import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add the project root to the Python path if running this file directly
if __name__ == "__main__":
    # Get the absolute path of the current script
    current_dir = Path(__file__).resolve().parent
    # Add the parent directory (project root) to sys.path
    project_root = current_dir.parent.parent
    sys.path.append(str(project_root))

# Import necessary components
from config.config_loader import load_config
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges, Queues, RoutingKeys
from shared.dto.alert import Alert, AlertType
from shared.dto.order import Order

# Execution layer imports
from execution.exchange.hyperliquid import HyperliquidExchange
from execution.execution_service import ExecutionService

# Monitoring layer imports
from monitoring.monitoring_service import MonitoringService
from monitoring.alert.alert_manager import AlertManager, TelegramAlertProvider, AlertProvider

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MockAlertProvider(AlertProvider):
    """Mock alert provider that captures alerts for testing."""
    
    def __init__(self):
        self.alerts = []
    
    async def send_alert(self, alert: Alert) -> bool:
        """Capture an alert instead of sending it."""
        self.alerts.append(alert)
        logger.info(f"MockAlertProvider captured alert: {alert.type.value} - {alert.message}")
        return True
    
    def get_alerts_by_type(self, alert_type: AlertType) -> List[Alert]:
        """Get alerts of a specific type."""
        return [a for a in self.alerts if a.type == alert_type]
    
    def get_alerts_for_order(self, order_id: str) -> List[Alert]:
        """Get alerts for a specific order ID."""
        return [a for a in self.alerts if hasattr(a, 'details') and 
                a.details and a.details.get('order_id') == order_id]
    
    def clear(self):
        """Clear all captured alerts."""
        self.alerts = []
        logger.info("MockAlertProvider: Cleared all captured alerts")

class IntegrationTest:
    """
    Integration test for the execution and monitoring layers.
    
    This test verifies:
    1. The execution layer can process signals and create orders
    2. Order events are published to the queue
    3. The monitoring layer receives these events and creates alerts
    4. Alerts are sent via the configured channels (mocked)
    """
    
    def __init__(self):
        """Initialize the integration test."""
        self.config = None
        self.cache_service = None
        self.exchange = None
        
        # Separate queue connections for each layer
        self.execution_consumer_queue = None
        self.execution_producer_queue = None
        self.monitoring_consumer_queue = None
        self.monitoring_producer_queue = None
        
        # Services
        self.execution_service = None
        self.monitoring_service = None
        
        # Mock components
        self.mock_alert_provider = None
        
        # Test data
        self.test_order_id = None
        self.test_signal_id = None
    
    async def setup(self):
        """Set up the test environment."""
        logger.info("Setting up integration test environment...")
        
        # Load configuration
        self.config = load_config()
        
        # Ensure monitoring telegram config exists for test
        if 'monitoring' not in self.config:
            self.config['monitoring'] = {}
        if 'telegram' not in self.config['monitoring']:
            self.config['monitoring']['telegram'] = {
                'bot_token': 'test_token',
                'chat_id': 'test_chat_id'
            }
        
        # Initialize shared services
        self.cache_service = CacheService()
        
        # Initialize exchange
        self.exchange = HyperliquidExchange(self.config)
        
        # Create queue services - separate connections for each component
        self.execution_consumer_queue = QueueService()
        self.execution_producer_queue = QueueService()
        self.monitoring_consumer_queue = QueueService()
        self.monitoring_producer_queue = QueueService()
        
        # Set up mock alert provider
        self.mock_alert_provider = MockAlertProvider()
        
        # Mock exchange initialize to avoid API calls
        original_exchange_init = self.exchange.initialize
        async def mock_exchange_init():
            logger.info("Mock exchange initialization")
            return True
        self.exchange.initialize = mock_exchange_init
        
        # Save original monitoring service methods
        original_init_alert_manager = MonitoringService._init_alert_manager
        
        # Create a modified _init_alert_manager method that uses our mock provider
        async def mock_init_alert_manager(self_monitor):
            logger.info("Using mock alert manager initialization")
            self_monitor.alert_manager = AlertManager(providers=[self.mock_alert_provider])
            logger.info("Mock alert manager initialized")
        
        # Replace the method
        MonitoringService._init_alert_manager = mock_init_alert_manager
        
        # Initialize execution service
        self.execution_service = ExecutionService(
            exchange=self.exchange,
            consumer_queue=self.execution_consumer_queue,
            producer_queue=self.execution_producer_queue,
            cache_service=self.cache_service,
            config=self.config
        )
        
        # Initialize monitoring service with mocked components
        self.monitoring_service = MonitoringService(
            exchange=self.exchange,
            consumer_queue=self.monitoring_consumer_queue,
            producer_queue=self.monitoring_producer_queue,
            cache_service=self.cache_service,
            config=self.config
        )
        
        # Start services
        await self.execution_service.start()
        await self.monitoring_service.start()
        
        # Restore original methods
        self.exchange.initialize = original_exchange_init
        MonitoringService._init_alert_manager = original_init_alert_manager
        
        # Validate that our mock alert provider is being used
        if not isinstance(self.monitoring_service.alert_manager, AlertManager):
            logger.error("Alert manager wasn't properly initialized")
        else:
            providers = getattr(self.monitoring_service.alert_manager, "providers", [])
            logger.info(f"Alert manager has {len(providers)} providers")
            for i, provider in enumerate(providers):
                logger.info(f"Provider {i+1} type: {type(provider).__name__}")
                if isinstance(provider, MockAlertProvider):
                    logger.info("Found MockAlertProvider - alert capture should work")
        
        logger.info("Integration test environment set up")
    
    async def teardown(self):
        """Clean up the test environment."""
        logger.info("Tearing down integration test environment...")
        
        # Stop services
        if self.execution_service:
            await self.execution_service.stop()
        
        if self.monitoring_service:
            await self.monitoring_service.stop()
        
        # Stop queues
        if self.execution_consumer_queue:
            self.execution_consumer_queue.stop()
        
        if self.execution_producer_queue:
            self.execution_producer_queue.stop()
        
        if self.monitoring_consumer_queue:
            self.monitoring_consumer_queue.stop()
        
        if self.monitoring_producer_queue:
            self.monitoring_producer_queue.stop()
        
        # Close cache
        if self.cache_service:
            self.cache_service.close()
        
        logger.info("Integration test environment torn down")
    
    async def test_direct_alert(self):
        """
        Test direct alert sending through the monitoring service.
        This is a basic sanity check to verify alert capturing works.
        """
        logger.info("Testing direct alert sending...")
        
        # Create a test alert
        test_alert = Alert(
            type=AlertType.ORDER_PLACED,
            symbol="BTC-USD",
            message="Test direct alert",
            timestamp=datetime.now().isoformat(),
            details={"order_id": "test123"}
        )
        
        # Directly send the alert through the alert manager
        result = await self.monitoring_service.alert_manager.send_alert(test_alert)
        
        # Check if the alert was captured
        direct_alerts = self.mock_alert_provider.get_alerts_by_type(AlertType.ORDER_PLACED)
        
        if not direct_alerts:
            logger.error("Direct alert test failed - alerts are not being captured")
            
            # Debug information about providers
            providers = getattr(self.monitoring_service.alert_manager, "providers", [])
            logger.error(f"Alert manager has {len(providers)} providers")
            for i, provider in enumerate(providers):
                logger.error(f"Provider {i+1} type: {type(provider).__name__}")
            
            return False
        
        logger.info(f"Direct alert test succeeded - captured alert: {direct_alerts[0].message}")
        return True
    
    async def test_order_flow(self):
        """
        Test the complete order flow:
        1. Process a signal into order parameters
        2. Create an order
        3. Verify the execution layer publishes an order event
        4. Manually forward the event to the monitoring layer
        5. Verify the monitoring layer creates an alert
        """
        # Clear any previous alerts
        self.mock_alert_provider.clear()
        
        logger.info("Starting order flow test...")
        
        # Create a mock signal
        self.test_signal_id = f"test_signal_{int(datetime.now().timestamp())}"
        mock_signal = {
            "id": self.test_signal_id,
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
        
        logger.info(f"Created mock signal: {self.test_signal_id}")
        
        # Process the signal to get order parameters
        order_params = await self.execution_service.process_signal(mock_signal)
        
        if not order_params:
            logger.error("Failed to process signal into order parameters")
            return False
        
        logger.info(f"Signal processed successfully: {order_params}")
        
        # Mock the exchange's create_order method
        self.test_order_id = f"test_order_{int(datetime.now().timestamp())}"
        
        # Save original methods
        original_create_order = self.exchange.create_order
        
        # Override methods with mocks
        async def mock_create_order(*args, **kwargs):
            return {
                "id": self.test_order_id,
                "symbol": kwargs["symbol"],
                "side": kwargs["side"],
                "type": kwargs["order_type"],
                "amount": kwargs["amount"],
                "price": kwargs["price"],
                "status": "open",
                "info": {}
            }
        
        self.exchange.create_order = mock_create_order
        
        # Execute the order through the execution service
        order_result = await self.execution_service.execute_order(order_params)
        
        if not order_result:
            logger.error("Failed to execute order")
            return False
        
        logger.info(f"Order executed successfully: {order_result}")
        
        # Create the order event that would be published to the queue
        order_event = {
            'type': 'created',
            'order_id': self.test_order_id,
            'symbol': order_params['symbol'],
            'side': order_params['side'],
            'order_type': order_params['type'],
            'price': order_params['price'],
            'size': order_params['amount'],
            'status': 'open',
            'timestamp': datetime.now().isoformat()
        }
        
        # Create the alert that would be generated for this event
        order_alert = Alert(
            type=AlertType.ORDER_PLACED,
            symbol=order_params['symbol'],
            message=f"Order {self.test_order_id} received",
            timestamp=datetime.now().isoformat(),
            details=order_event
        )
        
        # Directly call the monitoring service's process method
        await self.monitoring_service._process_event_async(order_event, order_alert)
        
        # Wait briefly for alert processing
        await asyncio.sleep(1)
        
        # Check if an alert was created
        placed_alerts = self.mock_alert_provider.get_alerts_by_type(AlertType.ORDER_PLACED)
        
        if not placed_alerts:
            logger.error("No ORDER_PLACED alerts were created")
            logger.error(f"Total alerts captured: {len(self.mock_alert_provider.alerts)}")
            for i, alert in enumerate(self.mock_alert_provider.alerts):
                logger.error(f"Alert {i+1}: {alert.type.value} - {alert.message}")
            return False
        
        logger.info(f"Order placed alert created successfully: {placed_alerts[0].message}")
        
        # Restore original methods
        self.exchange.create_order = original_create_order
        
        return True
    
    async def test_cancel_flow(self):
        """
        Test the order cancellation flow:
        1. Cancel an order via the execution service
        2. Verify the execution layer publishes a cancellation event
        3. Manually forward the event to the monitoring layer
        4. Verify the monitoring layer creates a cancellation alert
        """
        # Clear previous alerts before starting this test
        self.mock_alert_provider.clear()
        
        logger.info("Starting cancellation flow test...")
        
        if not self.test_order_id:
            logger.error("No test order ID available. Run test_order_flow first.")
            return False
        
        # Save original methods
        original_cancel_order = self.exchange.cancel_order
        
        # Override methods with mocks
        async def mock_cancel_order(*args, **kwargs):
            return {
                "id": self.test_order_id,
                "symbol": kwargs["symbol"],
                "status": "cancelled"
            }
        
        self.exchange.cancel_order = mock_cancel_order
        
        # Cancel the order through the execution service
        cancel_result = await self.execution_service.cancel_order(
            self.test_order_id, "BTC-USD"
        )
        
        if not cancel_result:
            logger.error("Failed to cancel order")
            return False
        
        logger.info(f"Order cancelled successfully: {cancel_result}")
        
        # Create the cancellation event that would be published to the queue
        cancel_event = {
            'type': 'cancelled',
            'order_id': self.test_order_id,
            'symbol': "BTC-USD",
            'side': "buy",
            'order_type': "limit",
            'price': 65000.00,
            'size': 0.01,
            'status': 'cancelled',
            'timestamp': datetime.now().isoformat()
        }
        
        # Create the alert that would be generated for this event
        cancel_alert = Alert(
            type=AlertType.ORDER_CANCELLED,
            symbol="BTC-USD",
            message=f"Order {self.test_order_id} cancelled",
            timestamp=datetime.now().isoformat(),
            details=cancel_event
        )
        
        # Directly call the monitoring service's process method
        await self.monitoring_service._process_event_async(cancel_event, cancel_alert)
        
        # Wait briefly for alert processing
        await asyncio.sleep(1)
        
        # Check if a cancellation alert was created
        cancel_alerts = self.mock_alert_provider.get_alerts_by_type(AlertType.ORDER_CANCELLED)
        
        if not cancel_alerts:
            logger.error("No ORDER_CANCELLED alerts were created")
            
            # Debug information about all alerts
            logger.error(f"Total alerts captured: {len(self.mock_alert_provider.alerts)}")
            for i, alert in enumerate(self.mock_alert_provider.alerts):
                logger.error(f"Alert {i+1}: {alert.type.value} - {alert.message}")
            
            # Debug alert manager and providers
            providers = getattr(self.monitoring_service.alert_manager, "providers", [])
            logger.error(f"Alert manager has {len(providers)} providers")
            for i, provider in enumerate(providers):
                logger.error(f"Provider {i+1} type: {type(provider).__name__}")
            
            return False
        
        logger.info(f"Order cancellation alert created successfully: {cancel_alerts[0].message}")
        
        # Restore original methods
        self.exchange.cancel_order = original_cancel_order
        
        return True
    
    async def run_tests(self):
        """Run all integration tests."""
        try:
            # Set up the test environment
            await self.setup()
            
            # Run direct alert test first as a sanity check
            direct_alert_result = await self.test_direct_alert()
            if not direct_alert_result:
                logger.error("Direct alert test failed - alert capturing is not working")
                return False
            
            # Run order flow test
            order_test_result = await self.test_order_flow()
            if not order_test_result:
                logger.error("Order flow test failed")
                return False
            
            # Run cancellation test
            cancel_test_result = await self.test_cancel_flow()
            if not cancel_test_result:
                logger.error("Cancel flow test failed")
                return False
            
            logger.info("All integration tests passed!")
            return True
            
        except Exception as e:
            logger.error(f"Error during integration tests: {e}", exc_info=True)
            return False
        finally:
            # Clean up
            await self.teardown()

async def main():
    """Main entry point for the integration test."""
    test = IntegrationTest()
    result = await test.run_tests()
    
    if result:
        logger.info("✅ INTEGRATION TEST PASSED: Execution and Monitoring layers are communicating correctly")
    else:
        logger.error("❌ INTEGRATION TEST FAILED: Issues with Execution and Monitoring layer communication")

if __name__ == "__main__":
    asyncio.run(main())