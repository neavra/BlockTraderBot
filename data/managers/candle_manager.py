import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from dataclasses import asdict

from normalizer.base import Normalizer
from shared.cache.cache_service import CacheService
from database.db import Database
from consumer.candle_consumer import CandleConsumer
from shared.constants import Exchanges, Queues, RoutingKeys
from shared.queue.queue_service import QueueService

from .base import BaseManager
from shared.domain.dto.candle import CandleData
from normalizer.factory import NormalizerFactory
from shared.domain.events.market_events import CandleClosedEvent, CandleUpdatedEvent

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
        self.producer_candle_queue = QueueService()
        self.producer_event_queue = QueueService()
        self.consumer_candle_queue = QueueService()
        self.candle_cache = CacheService()
        self.candle_consumer = CandleConsumer(database=database)
        self.logger = logging.getLogger("CandleManager")
        
        # Keep track of normalizers by exchange
        self.websocket_normalizers = {}
        self.rest_normalizers = {}

        self.event_tasks = set()
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

    async def stop(self):
        """Stop the candle manager and clean up resources."""
        if not self.running:
            self.logger.warning("Candle Manager is not running")
            return
            
        self.logger.info("Stopping Candle Manager...")
        self.running = False
        
        # Cancel all pending tasks
        tasks_to_cancel = list(self.event_tasks)
        for task in tasks_to_cancel:
            if not task.done() and not task.cancelled():
                task.cancel()
        
        # Wait for tasks with a timeout
        if tasks_to_cancel:
            try:
                # Wait up to 3 seconds for tasks to finish
                await asyncio.sleep(3)
                
                # Log any tasks that didn't complete
                for task in self.event_tasks:
                    if not task.done():
                        self.logger.warning(f"Task didn't complete during shutdown: {task}")
            except Exception as e:
                self.logger.error(f"Error waiting for candle manager tasks to complete: {e}")
        
        # Close any client resources
        if self.producer_candle_queue:
            self.producer_candle_queue.stop()
        
        if self.producer_event_queue:
            self.producer_event_queue.stop()
        
        if self.consumer_candle_queue:
            self.consumer_candle_queue.stop()
        
        if self.candle_cache:
            self.candle_cache.close()
        
        # Stop candle consumer if it has a stop method
        if hasattr(self.candle_consumer, 'stop'):
            await self.candle_consumer.stop()
        
        self.logger.info(f"Candle manager stopped, cleaned up {len(tasks_to_cancel)} tasks")

    async def _init_candle_producer(self):
        self.producer_candle_queue.declare_exchange(Exchanges.MARKET_DATA)
        self.producer_candle_queue.declare_queue(Queues.CANDLES)
        self.producer_candle_queue.bind_queue(
            exchange=Exchanges.MARKET_DATA,
            queue=Queues.CANDLES,
            routing_key=RoutingKeys.CANDLE_ALL
        )
        self.logger.info("Initialised Candle Producer Queue")
    
    async def _init_candle_consumer(self):
        self.consumer_candle_queue.declare_exchange(Exchanges.MARKET_DATA)
        self.consumer_candle_queue.declare_queue(Queues.CANDLES)
        self.consumer_candle_queue.bind_queue(
            exchange=Exchanges.MARKET_DATA,
            queue=Queues.CANDLES,
            routing_key=RoutingKeys.CANDLE_ALL
        )
        self.consumer_candle_queue.subscribe(
            queue=Queues.CANDLES,
            callback=self.on_candle
        )
        self.logger.info("Initialised Candle Consumer Queue")

    async def _init_event_producer(self):
        self.producer_event_queue.declare_exchange(Exchanges.MARKET_DATA)
        self.producer_event_queue.declare_queue(Queues.EVENTS)
        self.producer_event_queue.bind_queue(
            exchange=Exchanges.MARKET_DATA,
            queue=Queues.EVENTS,
            routing_key=RoutingKeys.DATA_EVENT_HANDLE
        )
        self.logger.info("Initialised Event Producer Queue")

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
            normalized_candle = await normalizer.normalize_websocket_data(data)
            
            self.logger.info("Normalized Candle:" + str(normalized_candle))
            
            # Normalize the dataclass CandleData to JSON string
            normalized_candle_json = normalizer.to_json(normalized_candle)
            # Cache key for this candle
            cache_key = f"{normalized_candle.exchange}:{normalized_candle.symbol}:{normalized_candle.timeframe}:{normalized_candle.timestamp}"
            
            if is_closed:
                # Candle is closed - publish to the queue for processing
                self.logger.info("Closed candle: {}, Symbol: {}, Timeframe: {}".format(normalized_candle.exchange, normalized_candle.symbol, normalized_candle.timeframe))
                self.producer_candle_queue.publish(exchange=Exchanges.MARKET_DATA, routing_key=RoutingKeys.CANDLE_ALL, message=normalized_candle_json)
                self.logger.debug("Successfully published candle to candle producer queue")
                
                # Add to cache
                self.candle_cache.set(cache_key, normalized_candle_json)
                self.logger.debug("Successfully stored closed candle to cache")

                # Normalize the dataclass event to JSON string
                candle_event = normalizer.to_json(CandleClosedEvent(candle=normalized_candle))
                
                # Publish candle closed event
                self.producer_event_queue.publish(exchange=Exchanges.MARKET_DATA, routing_key=RoutingKeys.DATA_EVENT_HANDLE, message=candle_event)

                self.logger.debug("Successfully published candle closed event to candle event queue")
            else:
                # Candle is still open 
                self.logger.debug(f"Updated open candle: {normalized_candle.timestamp}, Open: {normalized_candle.open}, Close: {normalized_candle.close}")
                
                # self.candle_cache.set(cache_key, normalized_candle_json)
                # self.logger.debug("Successfully added open candle in cache")
                
                # Normalize the dataclass event to JSON string
                candle_event = normalizer.to_json(CandleUpdatedEvent(candle=normalized_candle))
                
                # Publish candle updated event
                self.producer_event_queue.publish(exchange=Exchanges.MARKET_DATA, routing_key=RoutingKeys.DATA_EVENT_HANDLE, message=candle_event)
                
                self.logger.debug("Successfully published candle open event to candle event queue")
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
                
                # Convert to json
                normalized_candle_json = normalizer.to_json(normalized_candle)

                # Cache key for this candle
                cache_key = f"{normalized_candle.exchange}:{normalized_candle.symbol}:{normalized_candle.timeframe}:{normalized_candle.timestamp}"
                
                # Store historical candle in the cache publish to the queue
                self.logger.info(f"Historical candle: {normalized_candle.timestamp}, Symbol: {normalized_candle.symbol}, Timeframe: {normalized_candle.timeframe}")

                self.candle_cache.set(cache_key, normalized_candle_json)
                self.logger.debug("Successfully stored historical candle to cache")

                self.producer_candle_queue.publish(exchange=Exchanges.MARKET_DATA, routing_key=RoutingKeys.CANDLE_ALL, message=normalized_candle_json)
                self.logger.info("Published Candle!")
                
                # Publish candle closed event
                #await self.producer_event_queue.publish(CandleClosedEvent(candle=normalized_candle), routing_key="event.data.candle")

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
            task = asyncio.run_coroutine_threadsafe(
                self._process_event_async(candle), 
                self.main_loop  # You need to store the main loop as an instance variable
            )

            # Track the task future
            self.event_tasks.add(task)

            def _remove_task(future):
                self.event_tasks.discard(future)
            task.add_done_callback(_remove_task)
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

        except asyncio.CancelledError:
            # Handle cancellation gracefully
            self.logger.debug("Candle processing was cancelled")
            raise  # Re-raise to properly propagate cancellation
        except Exception as e:
            self.logger.error(f"Async processing error: {str(e)}")
