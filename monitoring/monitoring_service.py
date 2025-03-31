import asyncio
import logging
import queue
import threading
from datetime import datetime
from typing import Dict, Any, Optional

from monitoring.tele.tele_bot import TeleBot
from monitoring.order.order_manager import OrderManager
from monitoring.position.position_manager import PositionManager
from monitoring.alert.alert_manager import AlertManager, TelegramAlertProvider
from shared.domain.dto.alert_dto import AlertDto, AlertType
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges, Queues, RoutingKeys
from shared.domain.dto.order_dto import OrderDto

from execution.exchange.exchange_interface import ExchangeInterface

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class MonitoringService:
    """
    Service that monitors trading system events and forwards alerts to the Telegram bot.
    
    This service:
    1. Consumes events from a queue
    2. Processes events into alerts
    3. Sends alerts via Telegram
    4. Monitors existing Orders
    5. Interface with Orders and Positions to get data
    """
    
    """
    When initialising the service, __init__() has to be called to set up the basic object structure
    and store dependencies
    Then start() is called to use async operations to initialize components
    and assign values to the null fields. This is because init cannot be async in python
    """
    def __init__(
        self, 
        exchange: ExchangeInterface,
        consumer_queue: QueueService, 
        producer_queue: QueueService,
        cache_service: CacheService,
        config: Dict[str, Any]
    ):
        """
        Initialize the monitoring service.
        
        Args:
            consumer_queue: Service for consuming order messages
            producer_queue: Service for producing Signal Messages
            cache_service: Service for cache interactions
            config: Configuration dictionary for the monitoring service
        """
        self.consumer_queue = consumer_queue
        self.producer_queue = producer_queue
        self.cache_service = cache_service
        self.exchange = exchange

        self.config = config or {}
        
        self.running = False # Not active yet
        
        self.order_manager = None
        self.position_manager = None
        self.alert_manager = None
        self.telegram_bot = None
        self.main_loop = None
        
        # Store background tasks
        # self.tasks = []

    async def start(self):
        """Initialize and start the monitoring service components."""
        if self.running:
            logger.warning("Monitoring service is already running")
            return
        
        logger.info("Starting monitoring service...")

        self.main_loop = asyncio.get_running_loop()
        
        # Initialize the exchange connector
        await self._init_exchange()
        
        # Initialize managers
        await self._init_order_manager()
        await self._init_position_manager()
        
        # Initialize alert system
        await self._init_alert_manager()
        
        # Initialize and start the order consumer
        await self._init_order_consumer()

        await self._init_signal_producer()
        
        # Start background monitoring tasks
        # self.tasks.append(asyncio.create_task(self._monitor_positions()))
        
        self.running = True
        logger.info("Monitoring service started successfully")
    
    async def stop(self):
        """Gracefully stop the monitoring service and all its components."""
        if not self.running:
            logger.warning("Monitoring service is not running")
            return
        
        logger.info("Stopping monitoring service...")
            
        # Wait for tasks to complete
        # if self.tasks:
        #     await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Stop components in reverse order
        if self.consumer_queue:
            self.consumer_queue.stop()
        
        if self.producer_queue:
            self.producer_queue.stop()

        # Close connections
        if self.exchange:
            await self.exchange.close()
        
        self.running = False
        logger.info("Monitoring service stopped")
    
    async def _init_exchange(self):
        """Initialize the exchange connector for market data and order status queries."""
        logger.info("Initializing exchange...")
        await self.exchange.initialize()
        logger.info("Exchange connector initialized")
    
    async def _init_order_manager(self):
        """Initialize the order manager for tracking order status and history."""
        logger.info("Initializing order manager...")
        
        # order_repository = OrderRepository()

        # self.order_manager = OrderManager(order_repository)
        self.order_manager = OrderManager()
        logger.info("Order manager initialized")
    
    async def _init_position_manager(self):
        """Initialize the position manager for tracking active positions."""
        logger.info("Initializing position manager...")
        
        # position_repository = PositionRepository()

        # self.position_manager = PositionManager(position_repository)
        self.position_manager = PositionManager()
        logger.info("Position manager initialized")
    
    async def _init_alert_manager(self):
        """Initialize the alert manager with a Telegram alert provider."""
        logger.info("Initializing alert manager...")
        
        # Get Telegram configuration from the service config
        telegram_config = self.config['monitoring']['telegram']
        
        if telegram_config:
            # Initialize the Telegram bot using your implementation
            self.telegram_bot = TeleBot(
                token=telegram_config.get('bot_token'),
                chat_id=telegram_config.get('chat_id'),
            )

            self.telegram_bot.set_data_providers(
                    self.get_all_orders,
                    self.get_all_positions
                )
            await self._run_telegram_bot()

            # Create the alert provider with your Telegram bot
            telegram_provider = TelegramAlertProvider(self.telegram_bot)
            
            # Initialize the alert manager with the provider
            self.alert_manager = AlertManager(providers=[telegram_provider])
            
            # Start the Telegram bot if needed (consider running in background)
            # We're not calling telegram_bot.start() here because that would block
            # Instead, we might want to start it in a background thread if needed
            
            logger.info("Alert manager initialized with Telegram provider")
        else:
            # Initialize without providers if Telegram is disabled
            self.alert_manager = AlertManager()
            logger.info("Alert manager initialized without providers (Telegram disabled)")
    
    async def _run_telegram_bot(self):
        """Run the Telegram bot in the background."""
        logger.info("Starting Telegram bot...")
        try:
            await self.telegram_bot.start_async()
            logger.info("Telegram bot started")
        except Exception as e:
            logger.error(f"Error starting Telegram bot: {str(e)}")
    
    
    async def _init_order_consumer(self):
        """Initialize the order event consumer to process order updates."""
        logger.info("Initializing order event consumer...")
        
        # Ensure queue exchange and queue exist
        self.consumer_queue.declare_exchange(Exchanges.EXECUTION)
        self.consumer_queue.declare_queue(Queues.ORDERS)
        self.consumer_queue.bind_queue(
            Exchanges.EXECUTION,
            Queues.ORDERS,
            RoutingKeys.ORDER_ALL
        )

        # Subscribe to the queue with a callback
        self.consumer_queue.subscribe(
            Queues.ORDERS,
            self.on_event
        )
        
        logger.info("Order event consumer initialized")

    async def _init_signal_producer(self):
        """Initialize the signal event producer to produce events when orders are executed."""
        logger.info("Initializing signal event producer...")
        
        # Ensure queue exchange and queue exist
        self.producer_queue.declare_exchange(Exchanges.STRATEGY)
        self.producer_queue.declare_queue(Queues.SIGNALS)
        self.producer_queue.bind_queue(
            Exchanges.STRATEGY,
            Queues.SIGNALS,
            RoutingKeys.ORDER_EXECUTED
        )
        
        logger.info("Signal event producer initialized")
    
    # This method is called by the telegram bot, which wraps the call to the position manager
    def get_all_positions(self):
        return self.position_manager.get_all_positions()
    
    # This method is called by the telegram bot, which wraps the call to the order manager
    def get_all_orders(self):
        return self.order_manager.get_all_orders()
    
    # This method is binded to the queue, and the callback is registered here when receiving an event
    # On event, it should get the data from the cache, and check_order_status persistently
    # It should also create an alert that a new order is created
    def on_event(self, event):
        """Synchronous callback that schedules async work"""
        # Log receipt of the event synchronously
        logger.info(f"Received order event: {event}")
        
        try:
            # Create an alert object
            alert = AlertDto(
                type=AlertType.ORDER_PLACED,
                symbol=event.get("symbol", "unknown"),
                message=f"Order {event.get('order_id', 'unknown')} received",
                timestamp=datetime.now(),
                details=event
            )
            
            # Use run_coroutine_threadsafe to schedule the async task from this thread
            # This requires having a reference to the main event loop
            asyncio.run_coroutine_threadsafe(
                self._process_event_async(event, alert), 
                self.main_loop  # You need to store the main loop as an instance variable
            )
            
        except Exception as e:
            logger.error(f"Error scheduling event processing: {str(e)}")

    async def _process_event_async(self, event, alert):
        """Async method that handles the actual processing"""
        try:
            # Do any async processing here
            if self.alert_manager:
                await self.alert_manager.send_alert(alert)
                logger.info(f"Alert sent for order {event.get('order_id', 'unknown')}")
        except Exception as e:
            logger.error(f"Async processing error: {str(e)}")

    # This method is called when the order status is executed, this should also create an alert
    # After create, it needs to publish a signal to the signal queue
    def on_executed():
        logger.info("Order Executed")
    
    def build_alert():
        logger.info("Building alert object")

    def send_alert():
        logger.info("Sending Alert")

    # This method is a task that keeps checking on the status of the orders in the cache until something changes
    # Uses the exchange connector to quert the status, gets the order id from the cache
    def check_order_status():
        logger.info("Checking order status")
