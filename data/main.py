import asyncio
from dataclasses import asdict
import json
import logging
import signal
import sys
import os
import time
from typing import List, Dict, Any

from data.logs.logging import setup_logging
from config.config_loader import load_config
from connectors.rest.binance_rest import BinanceRestClient
from infrastructure.database.db import Database
from managers.candle_manager import CandleManager
from connectors.websocket.factory import WebSocketClientFactory
from connectors.rest.factory import RestClientFactory
from utils.concurrency import gather_with_concurrency

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

class TradingBotDataLayer:
    """
    Main application class for the trading bot data layer.
    Handles startup, shutdown, and running of all components.
    """

    def __init__(self):
        # Setup logging
        setup_logging()
        self.logger = logging.getLogger("TradingBotDataLayer")
        self.logger.info("Initializing Trading Bot Data Layer")

        # Intialise Config:
        self.config = load_config()
        self.logger.info("Configurations initialised")
        
        # Signal handling for graceful shutdown
        #self._setup_signal_handlers()
        
        # State tracking
        self.running = False
        self.tasks = []
        
        # Initialise Database
        self.database = Database(db_url=self.config["data"]["database"]["database_url"])
        
        # Initialize manager
        self.candle_manager = CandleManager(database=self.database)
        
        # Initialize handlers and consumers
        # self.market_data_handler = MarketDataHandler(event_bus=self.event_bus)
        # self.candle_consumer = CandleConsumer(queue=self.candle_queue, database=self.database)
        
        # Client factories
        self.ws_factory = WebSocketClientFactory()
        self.rest_factory = RestClientFactory()
        
        # Clients
        self.websocket_clients = []
        self.rest_clients = []

    

    async def _handle_shutdown_signal(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received shutdown signal {signum}. Initiating graceful shutdown...")
        self.running = False

    async def initialize(self):
        """Initialize all components."""
        self.logger.info("Ensuring database table exists and is created...")
        self.database.create_tables()
        
        #self.logger.info("Initializing market data handler...")
        #await self.market_data_handler.initialize()
        
        #self.logger.info("Initializing candle consumer...")
        #await self.candle_consumer.initialize()
        
        # Register event handlers
        # await self.register_event_handlers()
        
        # Initialize clients
        await self.initialize_clients()
        
        self.running = True
        self.logger.info("Initialization complete")

    async def register_event_handlers(self):
        """Register all event handlers."""
        self.logger.info("Registering event handlers...")
        # Register handlers with the event bus
        await self.market_data_handler.register_handlers()

    async def initialize_clients(self):
        """Initialize WebSocket and REST clients for all exchanges, symbols, and timeframes."""
        self.logger.info("Initializing market data clients...")
        
        for exchange_name, exchange_data in self.config['data'].items():
            # Skip if the exchange is not a dictionary or is empty
            if not isinstance(exchange_data, dict):
                continue
            
            # Check for symbols
            symbols = exchange_data.get('symbols', {})
            if symbols:
                for symbol, sym_is_active in symbols.items():
                    if sym_is_active:
                        # Check for timeframes
                        timeframes = exchange_data.get('timeframes', {})
                        if timeframes:
                            for timeframe, tf_is_active in timeframes.items():
                                if tf_is_active:
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

    async def start(self):
        """Start the application and all its components."""
        self.logger.info("Starting Trading Bot Data Layer")
        
        # Initialize everything
        await self.initialize()
        
        # Start Candle Manager queues:
        await self.candle_manager.start()

        # Start websocket listeners - group by exchange for better connection management
        websocket_tasks = [
            asyncio.create_task(client.listen()) 
            for client in self.websocket_clients
        ]
        self.tasks.extend(websocket_tasks)
        
        # Start the candle consumer - process incoming candles
        #consumer_task = asyncio.create_task(self.candle_consumer.run())
        #self.tasks.append(consumer_task)

        
        # Initial historical data load - fetch recent candles to start
        historical_tasks = [
            asyncio.create_task(self.fetch_initial_history(rest_client=client, lookback_period=1)) #720
            for client in self.rest_clients
        ]
        # Run historical tasks with concurrency limit to avoid overwhelming exchanges
        await gather_with_concurrency(10, *historical_tasks)
        
        self.logger.info("All components started successfully")
        
        # Keep application running until shutdown is called
        while self.running:
            await asyncio.sleep(1) #small sleep not to hog cpu
        
        # When self.running becomes False, perform shutdown
        #await self.shutdown()

    async def fetch_initial_history(self, rest_client : BinanceRestClient, lookback_period: int = 720):
        """Fetch initial historical data for a symbol/timeframe."""
        try:
            self.logger.info(f"Fetching initial history for {rest_client.symbol}/{rest_client.interval}")
            end_time = int(time.time() * 1000)  # Current time in milliseconds
            start_time = end_time - (lookback_period * 60 * 60 * 1000)  # Convert hours to ms
            while start_time < end_time - (timeframe_to_seconds(rest_client.interval) * 1000):
                candles = await rest_client.fetch_candlestick_data(limit=1500, startTime=start_time)
                
                if candles:
                    normalized_candles = await self.candle_manager.handle_rest_data(data_list=candles, exchange=rest_client.exchange, symbol=rest_client.symbol, interval=rest_client.interval)
                    last_candle_time = normalized_candles[-1].timestamp
                    start_time = int(last_candle_time.timestamp() * 1000) + (timeframe_to_seconds(rest_client.interval) * 1000)
                    self.logger.info(f"Loaded {len(normalized_candles)} historical candles for {rest_client.symbol}/{rest_client.interval} from Start Time:{start_time} to End Time:{end_time}")
                else:
                    break
            self.logger.info(f"No more historical candles to load for {rest_client.symbol}/{rest_client.interval}")
        except Exception as e:
            self.logger.error(f"Error fetching initial history: {e}")

    async def shutdown(self):
        """Gracefully shut down all components."""
        self.logger.info("Shutting down Trading Bot Data Layer...")
        
        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete cancellation
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close connections
        self.logger.info("Closing database connection...")
        await self.database.disconnect()
        
        self.logger.info("Shutdown complete")
        sys.exit(0)


async def main():
    """Main entry point for the application."""
    app = TradingBotDataLayer()
    await app.start()

def timeframe_to_seconds(timeframe: str) -> int:
        """
        Convert timeframe string (e.g., "1h", "4h", "1d") to seconds.

        Args:
            timeframe (str): Timeframe string.

        Returns:
            int: Timeframe duration in seconds.
        """
        multipliers = {"m": 60, "h": 3600, "d": 86400}
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        return value * multipliers.get(unit, 0)

if __name__ == "__main__":
    asyncio.run(main())