import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from dataclasses import asdict

from adapters.normalizer.base import Normalizer
from utils.helper import DateTimeEncoder
from infrastructure.database.db import Database
from services.consumer.candle_consumer import CandleConsumer
from shared.constants import Exchanges, Queues, RoutingKeys
from shared.queue.queue_service import QueueService

from .base import BaseManager
from domain.models.candle import CandleData
from adapters.normalizer.factory import NormalizerFactory
from domain.events.market_events import CandleClosedEvent, CandleUpdatedEvent

class CandleManager(BaseManager):
    """
    Manager for candle data processing.
    Handles the flow of data from connectors to consumers.
    """
    def __init__(self, database: Database):
        """
        Initialize the candle manager.
        
        Args:
            database : Database Instance
        """
        self.producer_queue = QueueService()
        self.consumer_queue = QueueService()
        self.candle_consumer = CandleConsumer(database=database)
        #self.cache = cache
        #self.event_bus = event_bus
        self.logger = logging.getLogger("CandleManager")
        
        # Keep track of normalizers by exchange
        self.websocket_normalizers = {}
        self.rest_normalizers = {}

        self.running = False # Not active yet
        self.main_loop = None
    

    async def start(self):

        if self.running:
            self.logger.warning("Candle Manager is already running")
            return

        self.main_loop = asyncio.get_running_loop()

        # Initialise the producer and consumers for the Candle Queue:
        await self._init_candle_producer()
        await self._init_candle_consumer()

        # Intialise Candle Consumer:
        await self.candle_consumer.initialize()

        self.running = True

        self.logger.info("Candle Manager started successfully")

    async def _init_candle_producer(self):
        self.producer_queue.declare_exchange(Exchanges.MARKET_DATA)
        self.producer_queue.declare_queue(Queues.CANDLES)
        self.producer_queue.bind_queue(
            exchange=Exchanges.MARKET_DATA,
            queue=Queues.CANDLES,
            routing_key=RoutingKeys.CANDLE_ALL
        )
        self.logger.info("Initialised Candle Producer Queue")
    
    async def _init_candle_consumer(self):
        self.consumer_queue.declare_exchange(Exchanges.MARKET_DATA)
        self.consumer_queue.declare_queue(Queues.CANDLES)
        self.consumer_queue.bind_queue(
            exchange=Exchanges.MARKET_DATA,
            queue=Queues.CANDLES,
            routing_key=RoutingKeys.CANDLE_ALL
        )
        self.consumer_queue.subscribe(
            queue=Queues.CANDLES,
            callback=self.on_candle
        )
        self.logger.info("Initialised Candle Consumer Queue")

    def _get_websocket_normalizer(self, exchange: str) -> Normalizer:
        """
        Get or create a WebSocket normalizer for the specified exchange.
        
        Args:
            exchange: Exchange name
            
        Returns:
            Normalizer instance
        """
        exchange = exchange.lower()
        if exchange not in self.websocket_normalizers:
            self.websocket_normalizers[exchange] = NormalizerFactory.create_websocket_normalizer(exchange)
        return self.websocket_normalizers[exchange]
    
    def _get_rest_normalizer(self, exchange: str) -> Normalizer:
        """
        Get or create a REST normalizer for the specified exchange.
        
        Args:
            exchange: Exchange name
            
        Returns:
            Normalizer instance
        """
        exchange = exchange.lower()
        if exchange not in self.rest_normalizers:
            self.rest_normalizers[exchange] = NormalizerFactory.create_rest_normalizer(exchange)
        return self.rest_normalizers[exchange]
    
    async def handle_websocket_data(self, data: Dict[str, Any], is_closed: bool) -> None:
        """
        Handle data received from a WebSocket.
        
        Args:
            data: Raw WebSocket data
            is_closed: Flag indicating if the candle is closed
        """
        #self.logger.info("Websocket data:" + str(data))
        exchange = data.get('exchange', '').lower()
        
        try:
            # Get the appropriate normalizer
            normalizer = self._get_websocket_normalizer(exchange)
            
            # Normalize the data
            normalized_data = await normalizer.normalize_websocket_data(data)
            
            self.logger.info("Normalized data:" + str(normalized_data))
            # Create a candle domain object
            #candle = CandleData(**normalized_data)
            candle = normalized_data
            # Cache key for this candle
            # cache_key = f"{exchange}:{symbol}:{interval}"
            
            if is_closed:
                # Candle is closed - publish to the queue for processing
                self.logger.info("Closed candle: {}, Symbol: {}, Timeframe: {}".format(candle["exchange"], candle["symbol"], candle["timeframe"]))
                self.producer_queue.publish(exchange=Exchanges.MARKET_DATA, routing_key=RoutingKeys.CANDLE_ALL, message=candle)
                self.logger.info("Yes pass")
                # Remove from cache
                #await self.cache.delete(cache_key)
                
                # Publish candle closed event
                #await self.event_bus.publish(CandleClosedEvent(candle=candle), routing_key="event.data.candle")
            else:
                # Candle is still open - update the cache
                self.logger.debug(f"Updated open candle: {candle['timestamp']}, Open: {candle['open']}, Close: {candle['close']}")
                #await self.cache.set(cache_key, candle)
                
                # Publish candle updated event
                # await self.event_bus.publish(CandleUpdatedEvent(candle=candle), routing_key="event.data.candle")
                
        except Exception as e:
            self.logger.error(f"Error handling WebSocket data: {e}")
            raise
    
    async def handle_rest_data(self, data_list: List[Dict[str, Any]], exchange: str, symbol: str, interval: str) -> None:
        """
        Handle data received from a REST API.
        Process historical candle data.
        
        Args:
            data_list: List of raw REST API data
            exchange: Exchange string
            symbol: Symbol string
            interval: Interval string
        """
        if not data_list:
            self.logger.warning("Received empty REST data list")
            return
        
        try:
            # Get the appropriate normalizer
            normalizer = self._get_rest_normalizer(exchange)
            
            normalized_data_list = []
            # Process each candle in the list
            for data in data_list:
                # Normalize the data
                normalized_candle : CandleData = await normalizer.normalize_rest_data(data=data, exchange=exchange, symbol=symbol, interval=interval)
                
                normalized_candle_json = json.dumps(asdict(normalized_candle), cls=DateTimeEncoder, default=str)
                
                # Historical candles are always closed - publish to the queue
                self.logger.info(f"Historical candle: {normalized_candle.timestamp}, Symbol: {normalized_candle.symbol}, Timeframe: {normalized_candle.timeframe}")

                self.producer_queue.publish(exchange=Exchanges.MARKET_DATA, routing_key=RoutingKeys.CANDLE_ALL, message=normalized_candle_json)
                self.logger.info("Published Candle!")
                
                # Publish candle closed event
                #await self.event_bus.publish(CandleClosedEvent(candle=normalized_candle), routing_key="event.data.candle")

                normalized_data_list.append(normalized_candle)
            return normalized_data_list
                
        except Exception as e:
            self.logger.error(f"Error handling REST data: {e}")
    
    def on_candle(self, candle):
        """Synchronous callback that schedules async work"""
        # Log receipt of the event synchronously
        self.logger.info(f"Received event: {candle}")
        
        try:
            # Use run_coroutine_threadsafe to schedule the async task from this thread
            # This requires having a reference to the main event loop
            asyncio.run_coroutine_threadsafe(
                self._process_event_async(candle), 
                self.main_loop  # You need to store the main loop as an instance variable
            )
        except Exception as e:
            self.logger.error(f"Error scheduling candle processing: {str(e)}")
    
    async def _process_event_async(self, candle):
        """Async method that handles the actual processing"""
        try: 
            self.logger.info(f"Received candle: {candle}")
            if self.candle_consumer:
                candledata = CandleData(**candle)
                await self.candle_consumer.process_item(candle=candledata)
                self.logger.info(f"Candle consumed by consumer: {candledata}")

        except Exception as e:
            self.logger.error(f"Async processing error: {str(e)}")
