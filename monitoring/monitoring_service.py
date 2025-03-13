import asyncio
import logging
import queue
import threading
from datetime import datetime
from typing import Dict, Any, Optional

from alert.telegram_bot import TelegramBot
from data.alert import Alert, AlertType

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
    """
    
    def __init__(self, telegram_bot: TelegramBot, event_queue: queue.Queue):
        """
        Initialize the monitoring service.
        
        Args:
            telegram_bot: The Telegram bot instance to use for sending alerts
            event_queue: The queue to consume events from
        """
        self.telegram_bot = telegram_bot
        self.event_queue = event_queue
        self.running = False
        self.event_thread = None
        self.event_loop = None
        logger.info("Monitoring service initialized")
    
    def _map_event_to_alert_type(self, event_type: str) -> AlertType:
        """Map event types to AlertType enum values."""
        event_type_map = {
            "ORDER_PLACED": AlertType.ORDER_PLACED,
            "ORDER_FILLED": AlertType.ORDER_FILLED,
            "ORDER_CANCELLED": AlertType.ORDER_CANCELLED,
            "ORDER_REJECTED": AlertType.ORDER_REJECTED,
            "POSITION_OPENED": AlertType.POSITION_OPENED,
            "POSITION_CLOSED": AlertType.POSITION_CLOSED,
            "TAKE_PROFIT_HIT": AlertType.TAKE_PROFIT_HIT,
            "STOP_LOSS_HIT": AlertType.STOP_LOSS_HIT,
            "ORDER_BLOCK_DETECTED": AlertType.ORDER_BLOCK_DETECTED,
            "SIGNAL_GENERATED": AlertType.SIGNAL_GENERATED,
            "ERROR": AlertType.ERROR,
            "WARNING": AlertType.WARNING,
            "INFO": AlertType.INFO
        }
        return event_type_map.get(event_type, AlertType.INFO)
    
    def _process_event(self, event: Dict[str, Any]) -> Optional[Alert]:
        """
        Process an event and convert it into an Alert.
        
        Args:
            event: The event to process
            
        Returns:
            Alert object or None if the event could not be processed
        """
        try:
            # Extract required fields with defaults
            event_type = event.get("type", "INFO")
            symbol = event.get("symbol", "Unknown")
            message = event.get("message", "No message provided")
            
            # Format timestamp
            timestamp = event.get("timestamp")
            if not timestamp:
                timestamp = datetime.now().isoformat()
            
            # Extract additional details
            details = event.get("details", {})
            
            # Map event type to AlertType enum
            alert_type = self._map_event_to_alert_type(event_type)
            
            # Create and return an Alert
            return Alert(
                type=alert_type,
                symbol=symbol,
                message=message,
                timestamp=timestamp,
                details=details
            )
        except Exception as e:
            logger.error(f"Failed to process event: {e}")
            logger.error(f"Event data: {event}")
            return None
    
    async def _process_queue(self):
        """Process events from the queue and send alerts."""
        logger.info("Started processing event queue")
        while self.running:
            try:
                # Check if there's an event in the queue (non-blocking)
                try:
                    event = self.event_queue.get_nowait()
                except queue.Empty:
                    # No events, sleep briefly and try again
                    await asyncio.sleep(0.1)
                    continue
                
                logger.info(f"Processing event: {event}")
                
                # Process the event into an alert
                alert = self._process_event(event)
                
                if alert:
                    # Send the alert via Telegram
                    await self.telegram_bot.send_alert(alert)
                
                # Mark the event as processed
                self.event_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in event processing loop: {e}")
                await asyncio.sleep(1)  # Brief pause before retrying
    
    def _run_event_loop(self):
        """Run the asyncio event loop in a separate thread."""
        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        
        # Create task for processing the queue
        self.event_loop.create_task(self._process_queue())
        
        # Run the event loop
        try:
            self.event_loop.run_forever()
        finally:
            logger.info("Event loop stopped")
            self.event_loop.close()
    
    def start(self):
        """Start the monitoring service."""
        if self.running:
            logger.warning("Monitoring service is already running")
            return
        
        self.running = True
        
        # Start a separate thread for the asyncio event loop
        self.event_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.event_thread.start()
        
        logger.info("Monitoring service started")
    
    def stop(self):
        """Stop the monitoring service."""
        if not self.running:
            logger.warning("Monitoring service is not running")
            return
        
        logger.info("Stopping monitoring service...")
        self.running = False
        
        # Stop the event loop
        if self.event_loop:
            self.event_loop.call_soon_threadsafe(self.event_loop.stop)
        
        # Wait for the thread to finish
        if self.event_thread:
            self.event_thread.join(timeout=5)
        
        logger.info("Monitoring service stopped")
    
    def add_event(self, event: Dict[str, Any]):
        """
        Add an event to the monitoring queue.
        
        Args:
            event: The event to add to the queue
        """
        self.event_queue.put(event)
        logger.debug(f"Added event to queue: {event}")
    
    async def fetch_and_send_positions(self):
        """Fetch current positions and send them to the Telegram chat."""
        try:
            await self.telegram_bot.fetch_positions()
            logger.info("Positions fetched and sent")
        except Exception as e:
            logger.error(f"Failed to fetch and send positions: {e}")
    
    async def fetch_and_send_orders(self):
        """Fetch current orders and send them to the Telegram chat."""
        try:
            await self.telegram_bot.fetch_orders()
            logger.info("Orders fetched and sent")
        except Exception as e:
            logger.error(f"Failed to fetch and send orders: {e}")
    
    def create_command_task(self, coroutine):
        """
        Create a task to run in the event loop.
        
        Args:
            coroutine: The coroutine to run
        """
        if self.event_loop:
            asyncio.run_coroutine_threadsafe(coroutine, self.event_loop)