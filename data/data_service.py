import asyncio
import logging
import time
from typing import Dict, List, Any, Optional

from consumer.candle_consumer import CandleConsumer
from data.logs.logging import setup_logging
from config.config_loader import load_config
from connectors.websocket.factory import WebSocketClientFactory
from connectors.rest.factory import RestClientFactory
from connectors.rest import RestClient
from database.db import Database
from managers.candle_manager import CandleManager
from shared.queue.queue_service import QueueService
from utils.concurrency import gather_with_concurrency

logger = logging.getLogger(__name__)

class DataService:
    """
    Data Layer Service for the trading bot.
    
    This service:
    1. Collects real-time and historical market data
    2. Processes and normalizes data
    3. Stores data in the database
    4. Publishes data events to the system
    """
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """
        Initialize the data service with dependencies.
        
        Args:
            database: Database connection instance
            candle_manager: Manager for handling candle data
            config: Configuration dictionary
        """
        self.database = Database(db_url=config["data"]["database"]["database_url"])
        self.consumer_candle_queue = QueueService()
        self.candle_consumer = CandleConsumer(database=self.database, consumer_candle_queue=self.consumer_candle_queue)
        self.candle_manager = CandleManager(candle_consumer=self.candle_consumer, config=config)
        self.config = config
        
        # Initialize client factories
        self.ws_factory = WebSocketClientFactory()
        self.rest_factory = RestClientFactory()
        
        # Initialize client lists
        self.websocket_clients = []
        self.rest_clients = []
        
        # Task tracking
        self.tasks = []
        self.running = False
        
        logger.info("Data service initialized")
    
    async def start(self) -> bool:
        """
        Start the data service and all its components.
        
        Returns:
            True if startup was successful, False otherwise
        """
        if self.running:
            logger.warning("Data service is already running")
            return True
        
        try:
            logger.info("Starting data service...")
            
            # Initialize database tables
            self.database.create_tables()
            
            # Start the candle manager
            await self.candle_manager.start()
            
            # Initialize market data clients
            await self._init_market_data_clients()
            
            # Start WebSocket listeners
            await self._start_websocket_listeners()
            
            # Load initial historical data
            await self._load_initial_history()
            
            self.running = True
            logger.info("Data service started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error starting data service: {e}", exc_info=True)
            return False
    
    async def stop(self) -> None:
        """
        Stop the data service and all its components gracefully.
        """
        if not self.running:
            logger.warning("Data service is not running")
            return
        
        logger.info("Stopping data service...")
        
        # First, stop the candle manager
        if self.candle_manager:
            await self.candle_manager.stop()
        
        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete cancellation with a timeout
        if self.tasks:
            try:
                # Wait with a timeout (e.g., 5 seconds)
                await asyncio.wait(self.tasks, timeout=5)
            except asyncio.CancelledError:
                logger.warning("Some tasks were cancelled during shutdown")
            except Exception as e:
                logger.error(f"Error waiting for tasks during shutdown: {e}")
        
        # Close database connection
        if self.database:
            await self.database.disconnect()
        
        self.running = False
        logger.info("Data service stopped")
    
    async def _init_market_data_clients(self) -> None:
        """
        Initialize WebSocket and REST clients for configured exchanges,
        symbols, and timeframes.
        """
        logger.info("Initializing market data clients...")
        
        for exchange_name, exchange_config in self.config['data'].items():
            # Skip if the exchange is not a dictionary or is empty
            if not isinstance(exchange_config, dict):
                continue
            
            # Check for symbols
            symbols = exchange_config.get('symbols', {})
            if not symbols:
                continue
                
            for symbol, is_active in symbols.items():
                if not is_active:
                    continue
                    
                # Check for timeframes
                timeframes = exchange_config.get('timeframes', {})
                if not timeframes:
                    continue
                    
                for timeframe, tf_is_active in timeframes.items():
                    if not tf_is_active:
                        continue
                        
                    # Create WebSocket client
                    ws_client = self.ws_factory.create(
                        exchange=exchange_name,
                        symbol=symbol,
                        interval=timeframe,
                        manager=self.candle_manager
                    )
                    self.websocket_clients.append(ws_client)
                    
                    # Create REST client (for historical data)
                    rest_client = self.rest_factory.create(
                        exchange=exchange_name,
                        symbol=symbol,
                        interval=timeframe
                    )
                    self.rest_clients.append(rest_client)
        
        logger.info(f"Initialized {len(self.websocket_clients)} websocket clients and {len(self.rest_clients)} REST clients")
    
    async def _start_websocket_listeners(self) -> None:
        """
        Start all WebSocket listeners to receive real-time market data.
        """
        logger.info("Starting WebSocket listeners...")
        
        for client in self.websocket_clients:
            task = asyncio.create_task(client.listen())
            self.tasks.append(task)
        
        logger.info(f"Started {len(self.websocket_clients)} WebSocket listeners")
    
    async def _load_initial_history(self) -> None:
        """
        Load initial historical data for all configured symbols and timeframes.
        """
        logger.info("Loading initial historical data...")
        
        historical_tasks = [
            asyncio.create_task(self._fetch_initial_history(
                rest_client=client, 
                lookback_period=self.config['data'].get('lookback', 720)
            ))
            for client in self.rest_clients
        ]
        
        # Run historical tasks with concurrency limit to avoid overwhelming exchanges
        await gather_with_concurrency(10, *historical_tasks)
        
        logger.info("Initial historical data loading completed")
    
    async def _fetch_initial_history(self, rest_client: RestClient, lookback_period: int = 720) -> None:
        """
        Fetch initial historical data for a specific symbol/timeframe.
        
        Args:
            rest_client: REST client for the exchange
            lookback_period: Number of hours to look back for historical data
        """
        try:
            logger.info(f"Fetching initial history for {rest_client.symbol}/{rest_client.interval}")
            
            end_time = int(time.time() * 1000)  # Current time in milliseconds
            start_time = end_time - (lookback_period * 60 * 60 * 1000)  # Convert hours to ms
            
            while start_time < end_time - (self._timeframe_to_seconds(rest_client.interval) * 1000):
                candles = await rest_client.fetch_candlestick_data(
                    limit=1500, 
                    startTime=start_time
                )
                
                if not candles:
                    break
                    
                normalized_candles = await self.candle_manager.handle_rest_data(
                    data_list=candles,
                    exchange=rest_client.exchange,
                    symbol=rest_client.symbol,
                    interval=rest_client.interval
                )
                
                if not normalized_candles:
                    break
                    
                last_candle_time = normalized_candles[-1].timestamp
                start_time = int(last_candle_time.timestamp() * 1000) + (self._timeframe_to_seconds(rest_client.interval) * 1000)
                
                logger.info(f"Loaded {len(normalized_candles)} historical candles for {rest_client.symbol}/{rest_client.interval}")
            
            self.candle_manager.mark_historical_complete(
                exchange=rest_client.exchange,
                symbol=rest_client.symbol,
                timeframe=rest_client.interval
            )
            logger.info(f"Completed historical data loading for {rest_client.symbol}/{rest_client.interval}")
            
        except Exception as e:
            logger.error(f"Error fetching initial history for {rest_client.symbol}/{rest_client.interval}: {e}")
    
    @staticmethod
    def _timeframe_to_seconds(timeframe: str) -> int:
        """
        Convert timeframe string (e.g., "1h", "4h", "1d") to seconds.
        
        Args:
            timeframe: Timeframe string
            
        Returns:
            Timeframe duration in seconds
        """
        multipliers = {"m": 60, "h": 3600, "d": 86400}
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        return value * multipliers.get(unit, 0)