import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from dataclasses import asdict
from datetime import datetime

from data.utils.helper import DateTimeEncoder
from data.normalizer.base import Normalizer
from shared.cache.cache_service import CacheService
from data.database.db import Database
from data.consumer.candle_consumer import CandleConsumer
from shared.constants import Exchanges, Queues, RoutingKeys, CacheKeys, CacheTTL
from shared.queue.queue_service import QueueService

from .base import BaseManager
from shared.domain.dto.candle_dto import CandleDto
from data.normalizer.factory import NormalizerFactory
from shared.domain.events.candle_closed_event import CandleClosedEvent

from data.aggregators.candle_aggregator import CandleAggregator
from .state_manager import StateManager
from data.utils.timeframe_utils import get_custom_timeframes_for_base, is_timeframe_enabled

class CandleManager(BaseManager):
    """
    Manager for candle data processing.
    Handles the flow of data from connectors to consumers.
    """
    def __init__(self, database: Database, config: Dict[str, Any]):
        """
        Initialize the candle manager.
        
        Args:
            candle_consumer : Candle Consumer Instance
            config: Global Configuration dictionary
        """
        self.producer_candle_queue = QueueService()
        self.producer_event_queue = QueueService()
        self.candle_cache = CacheService()
        self.candle_consumer = CandleConsumer(database=database)
        self.logger = logging.getLogger("CandleManager")

        self.config = config
        
        # Initialize custom timeframe components
        self.custom_timeframes_enabled = is_timeframe_enabled(config)
        if self.custom_timeframes_enabled:
            self.state_manager = StateManager(cache_service=self.candle_cache)
            self.candle_aggregator = CandleAggregator(
                state_manager=self.state_manager,
                queue_service=self.producer_candle_queue,
                config=config
            )
        
        self.historical_complete = {}
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

        # Intialise Candle Consumer:
        await self.candle_consumer.initialize()

        self.running = True

        # Initialize custom timeframe components if enabled
        if self.custom_timeframes_enabled:
            self.logger.info("Custom timeframe processing is enabled")
        else:
            self.logger.info("Custom timeframe processing is disabled")

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
        # Only process closed candles
        if not is_closed:
            return
        
        exchange = data.get('exchange', '').lower()
        
        try:
            # Get the appropriate normalizer
            normalizer = self._get_websocket_normalizer(exchange)
            
            # Normalize the data
            normalized_candle: CandleDto = await normalizer.normalize_websocket_data(data)
            
            self.logger.info(f"Normalized Candle: {normalized_candle}")
            
            # Process standard timeframe candle
            market_key = f"{normalized_candle.exchange}:{normalized_candle.symbol}:{normalized_candle.timeframe}"
            historical_done = self.historical_complete.get(market_key, False)
            await self._process_standard_candle(normalized_candle, normalizer, "live", historical_done)
            
            # Process custom timeframes if enabled
            # if self.custom_timeframes_enabled:
            #     await self._process_custom_timeframes(normalized_candle)
                
        except Exception as e:
            self.logger.error(f"Error handling WebSocket data: {e}")
            raise
    
    async def handle_rest_data(self, data_list: List[Dict[str, Any]], exchange: str, symbol: str, interval: str) -> List[CandleDto]:
        """
        Handle data received from a REST API.
        Process historical candle data.
        
        Args:
            data_list: List of raw REST API data
            exchange: Exchange string
            symbol: Symbol string
            interval: Interval string
            
        Returns:
            List of normalized candle DTOs
        """
        if not data_list:
            self.logger.warning("Received empty REST data list")
            return []
        
        try:
            # Get the appropriate normalizer
            normalizer = self._get_rest_normalizer(exchange)
            
            normalized_data_list = []
            # Process each candle in the list
            for data in data_list:
                # Normalize the data
                normalized_candle: CandleDto = await normalizer.normalize_rest_data(
                    data=data, exchange=exchange, symbol=symbol, interval=interval
                )
                
                # Process standard timeframe candle only if closed, ignore opened candles
                if normalized_candle.is_closed:
                    await self._process_standard_candle(normalized_candle, normalizer, "historical")
                    
                
                # Process custom timeframes if enabled
                # if self.custom_timeframes_enabled:
                #     await self._process_custom_timeframes(normalized_candle)
                
                normalized_data_list.append(normalized_candle)
                
            return normalized_data_list
                
        except Exception as e:
            self.logger.error(f"Error handling REST data: {e}")
            return []
    
    async def _cache_candle(self, candle: CandleDto, cache_key: str):
        score = candle.timestamp.timestamp() if isinstance(candle.timestamp, datetime) else float(candle.timestamp)
        normalized_candle_json = json.dumps(asdict(candle), cls=DateTimeEncoder)
                    
        # Add to cache as a sorted set. Cache Key contains a sorted set of candles, sorted by timestamp
        self.candle_cache.add_to_sorted_set(
            name=cache_key,
            value=normalized_candle_json,
            score=score,
            ex=CacheTTL.CANDLE_DATA
        )
        self.logger.debug("Successfully stored closed candle to cache")

    async def _process_standard_candle(self, normalized_candle: CandleDto, normalizer: Normalizer, source: str, historical_done: bool = False) -> None:
        """
        Process a standard timeframe candle.
        Store it in the database and publish events.
        
        Args:
            candle: Normalized candle data
            is_closed: Flag indicating if the candle is closed
            normalizer: Normalizer instance for converting to JSON
        """
        # Convert to JSON for storage/publishing
        normalized_candle_json = normalizer.to_json(normalized_candle)

        # Candle is closed - publish to the queue to insert into the database
        # self.logger.info(f"Closed candle: {candle.exchange}, Symbol: {candle.symbol}, Timeframe: {candle.timeframe}")
        
        # self.logger.debug("Successfully published candle to candle producer queue")
        self.producer_candle_queue.publish(
                exchange=Exchanges.MARKET_DATA, 
                routing_key=RoutingKeys.CANDLE_ALL, 
                message=normalized_candle_json
            )
        if source == 'historical':
            candle_event = CandleClosedEvent.to_event(normalized_candle, source)
            self.producer_event_queue.publish(
                exchange=Exchanges.MARKET_DATA, 
                routing_key=RoutingKeys.DATA_EVENT_HANDLE, 
                message=json.dumps(asdict(candle_event), cls=DateTimeEncoder)
            )

            cache_key = CacheKeys.CANDLE_HISTORY_REST_API_DATA.format(
                exchange=normalized_candle.exchange,
                symbol=normalized_candle.symbol,
                timeframe=normalized_candle.timeframe
            )
        # No matter what need to cache both live and historical.But only publish live events once historical data is caught up
        elif source == 'live':
            cache_key = CacheKeys.CANDLE_LIVE_WEBSOCKET_DATA.format(
                    exchange=normalized_candle.exchange,
                    symbol=normalized_candle.symbol,
                    timeframe=normalized_candle.timeframe
                )
            if historical_done:
                candle_event = CandleClosedEvent.to_event(normalized_candle, source)
                self.producer_event_queue.publish(
                    exchange=Exchanges.MARKET_DATA, 
                    routing_key=RoutingKeys.DATA_EVENT_HANDLE, 
                    message=json.dumps(asdict(candle_event), cls=DateTimeEncoder)
                )
                # self.logger.debug("Successfully published candle closed event to candle event queue")
        else:
            self.logger.error("CandleManage process_standard_candle, invalid source type")

        await self._cache_candle(candle=normalized_candle,cache_key=cache_key)

        await self._prune_candle_cache(normalized_candle.timeframe, cache_key)
    
    async def _prune_candle_cache(self, timeframe: str, cache_key: str):
        """
        Prune the candle cache for a specific key to maintain a size limit.
        
        Args:
            exchange: Exchange name
            symbol: Trading pair symbol 
            timeframe: Candle timeframe
            cache_key: Redis key for the sorted set
        """
        try:
            # Determine max candles to keep based on timeframe
            max_candles = self._get_max_candles_for_timeframe(timeframe)
            
            # Get current count
            current_count = self.candle_cache.sorted_set_count(cache_key)
            
            # Prune if needed
            if current_count > max_candles:
                to_remove = current_count - max_candles
                removed = self.candle_cache.sorted_set_remove_range_by_rank(
                    cache_key, 
                    0,  # Start index (oldest)
                    to_remove - 1  # End index
                )
                
                if removed:
                    self.logger.debug(f"Pruned {removed} candles from {cache_key} to maintain limit of {max_candles}")
        except Exception as e:
            self.logger.warning(f"Error pruning candle cache: {e}")

    def _get_max_candles_for_timeframe(self, timeframe: str) -> int:
        """
        Determine maximum number of candles to keep based on timeframe.
        
        Args:
            timeframe: Candle timeframe string
            
        Returns:
            Maximum number of candles to keep in cache
        """
        # Convert timeframe to seconds
        seconds = self._timeframe_to_seconds(timeframe)
        
        # Scale max candles inversely with timeframe duration
        if seconds <= 60:  # 1m
            return 10000  # More for lower timeframes
        elif seconds <= 300:  # 5m
            return 5000
        elif seconds <= 900:  # 15m
            return 3000
        elif seconds <= 3600:  # 1h
            return 1500
        elif seconds <= 14400:  # 4h
            return 750
        elif seconds <= 86400:  # 1d
            return 365  # About a year of daily candles
        else:  # Weekly or above
            return 200
    async def _process_custom_timeframes(self, standard_candle: CandleDto) -> None:
        """
        Process custom timeframes for a standard candle.
        
        Args:
            standard_candle: Standard timeframe candle
        """
        # Skip if custom timeframes are disabled
        if not self.custom_timeframes_enabled:
            return
        
        try:
            # Get mapped custom timeframes for this standard timeframe
            custom_timeframes = get_custom_timeframes_for_base(
                standard_candle.timeframe, self.config
            )
            
            if not custom_timeframes:
                self.logger.debug(
                    f"No Custom Timeframes for "
                    f"{standard_candle.exchange}:{standard_candle.symbol}:{standard_candle.timeframe}"
                                  )
                return
            
            self.logger.debug(
                f"Processing {len(custom_timeframes)} custom timeframes for "
                f"{standard_candle.exchange}:{standard_candle.symbol}:{standard_candle.timeframe}"
            )
            
            # Process each custom timeframe
            for custom_tf in custom_timeframes:
                await self.candle_aggregator.process_candle(standard_candle, custom_tf)
                
        except Exception as e:
            self.logger.error(f"Error processing custom timeframes: {e}")

    def mark_historical_complete(self, exchange: str, symbol: str, timeframe: str) -> None:
        """
        Mark historical data loading as complete for a specific market.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timeframe: Candle timeframe
        """
        key = f"{exchange}:{symbol}:{timeframe}"
        self.historical_complete[key] = True
        self.logger.info(f"Historical data marked complete for {exchange}:{symbol}:{timeframe}")
        

    def get_configured_markets(self) -> list:
        """
        Get all configured markets (exchange, symbol, timeframe combinations)
        from the config.
        
        Returns:
            List of tuples (exchange, symbol, timeframe)
        """
        markets = []
        
        for exchange_name, exchange_config in self.config.get('data', {}).items():
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
                        
                    markets.append((exchange_name, symbol, timeframe))
        
        return markets

    def is_any_historical_loading(self) -> bool:
        """Check if any symbol/timeframe is still loading historical data"""
        # If any configured market isn't in the historical_complete dict or is False
        for exchange, symbol, timeframe in self.get_configured_markets():
            key = f"{exchange}:{symbol}:{timeframe}"
            if not self.historical_complete.get(key, False):
                return True
        return False
    
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