import asyncio
import logging
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

from strategy.engine.indicator_dag import IndicatorDAG
from strategy.strategies.base import Strategy
from strategy.context.context_engine import ContextEngine
from strategy.engine.mitigation_service import MitigationService
from shared.domain.dto.signal_dto import SignalDto
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.domain.dto.candle_dto import CandleDto
from shared.domain.types.source_type_enum import SourceTypeEnum
from strategy.indicators.base import Indicator
from shared.constants import Exchanges, Queues, RoutingKeys, CacheKeys, CacheTTL
from strategy.domain.models.market_context import MarketContext
from data.database.db import Database
from data.database.repository.signal_repository import SignalRepository

logger = logging.getLogger(__name__)

class StrategyRunner:
    """
    Main runner for strategy execution
    Handles:
    - Data retrieval from cache
    - Strategy execution
    - Signal publishing
    - Event-based strategy execution
    """
    
    def __init__(
        self, 
        strategies: List[Strategy], 
        cache_service: CacheService,
        producer_queue: QueueService,
        consumer_queue: QueueService,
        context_engine: ContextEngine,
        database: Database,
        config: Dict[str, Any]
    ):
        """
        Initialize the strategy runner
        
        Args:
            strategies: List of strategies to run
            cache_service: Cache service for data retrieval
            producer_queue: Queue service for publishing signals
            consumer_queue: Queue service for consuming candle events
            config: Configuration dictionary
        """
        self.strategies = strategies
        self.database = database
        session = self.database.get_session()
        self.signal_repository = SignalRepository(session=session)
        self.context_engine =context_engine
        # Holds Candle Data
        self.cache_service = cache_service
        # Produces Signals generated from the data
        self.producer_queue = producer_queue
        # Consumes the candle data from the data layer
        self.consumer_queue = consumer_queue
        self.config = config
        self.running = False
        self.execution_task = None
        self.main_loop = None  # Will store the event loop for callbacks
        
        self.all_indicators : Dict[str, Indicator] = {} 
        # Create indicator DAG
        self.indicator_dag = IndicatorDAG()
        # Initialize mitigation service
        self.mitigation_service = MitigationService()
    
    async def start(self):
        """Start the strategy runner"""
        if self.running:
            logger.warning("Strategy runner is already running")
            return
        
        self.running = True
        logger.info("Starting strategy runner...")
        
        # Store the event loop for callbacks
        self.main_loop = asyncio.get_running_loop()
        
        # Initialize the signal exchange
        self.producer_queue.declare_exchange(Exchanges.STRATEGY)
        
        # Initialize the event consumer
        await self._init_event_consumer()
        
        # Initialize the indicator DAG with registered indicators
        await self._init_indicator_dag()

        # Register indicators for mitigation processing
        await self._init_mitigation_service()
        
        logger.info("Strategy runner started")
    
    async def stop(self):
        """Stop the strategy runner"""
        logger.info("Stopping strategy runner...")
        self.running = False
        
        if self.execution_task:
            try:
                self.execution_task.cancel()
                await self.execution_task
            except asyncio.CancelledError:
                pass
            
        logger.info("Strategy runner stopped")
    
    async def _init_event_consumer(self):
        """Initialize the candle event consumer."""
        logger.info("Initializing candle event consumer...")
        
        try:
            # Set up exchange and queue
            self.consumer_queue.declare_exchange(Exchanges.MARKET_DATA)
            self.consumer_queue.declare_queue(Queues.CANDLES)
            
            # Bind to candle events - we want to receive all candle events
            self.consumer_queue.bind_queue(
                Exchanges.MARKET_DATA,
                Queues.CANDLES,
                RoutingKeys.CANDLE_ALL
            )
            
            # Subscribe to the queue
            self.consumer_queue.subscribe(
                Queues.CANDLES,
                self._on_candle_event
            )
            
            logger.info("Candle event consumer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize candle event consumer: {e}")
            raise
    
    async def _init_indicator_dag(self):
        """Initialize the indicator DAG by registering indicators and their dependencies"""
        logger.info("Initializing indicator DAG...")
        
        
        # First, gather all indicators from strategies
        for strategy in self.strategies:
            for name, indicator in strategy.indicators.items():
                self.all_indicators[name] = indicator
        
        # Now register each indicator with its dependencies
        for name, indicator in self.all_indicators.items():
            # Get requirements from indicator
            requirements = indicator.get_requirements()
            
            # Extract indicator dependencies
            dependencies = requirements.get('indicators', [])
            
            # Register in the DAG
            self.indicator_dag.register_indicator(name, indicator, dependencies)
        
        # Compute initial execution order
        execution_order = self.indicator_dag.compute_execution_order()
        logger.info(f"Indicator execution order established: {execution_order}")

    async def _init_mitigation_service(self):
        """Register indicators that require mitigation with the mitigation service."""
        logger.info("Registering indicators for mitigation processing...")
        
        try:
            # Get all indicator types that require mitigation
            
            # Register each one with the mitigation service
            for name, indicator in self.all_indicators.items():                
                if indicator:
                    # Register with the mitigation service
                    self.mitigation_service.register_indicator(
                        indicator_type=name,
                        indicator=indicator,
                    )
                    
                    logger.info(f"Registered {name} for mitigation processing")
                else:
                    logger.warning(f"Could not register {name} for mitigation (missing indicator or repository)")
            
            logger.info(f"Registered {len(self.mitigation_service.indicators)} indicators for mitigation processing")
            
        except Exception as e:
            logger.error(f"Error registering indicators for mitigation: {e}", exc_info=True)

    def _on_candle_event(self, event: Dict[str, Any]):
        """
        Callback for handling candle events from the queue.
        This is called when a new candle is received.
        
        Args:
            event: Candle event data
        """
        try:
            logger.debug(f"Received candle event: {event.get('symbol')} {event.get('timeframe')}")
            
            # Determine the event source, default to live
            event_source = event.get('source', SourceTypeEnum.LIVE)
            
            # Schedule strategy execution in the background
            # This allows us to react to new market data immediately
            if self.main_loop and self.running:
                asyncio.run_coroutine_threadsafe(
                    self._execute_on_event(event, event_source),
                    self.main_loop
                )
        except Exception as e:
            logger.error(f"Error processing candle event: {e}")
    
    async def _execute_on_event(self, event: Dict[str, Any], source: SourceTypeEnum):
        """
        Execute strategies based on a candle event.

        Args:
            event: Candle event data
            source: Event source ('historical' or 'live')
        """
        try:
            exchange = event.get('exchange')
            symbol = event.get('symbol')
            timeframe = event.get('timeframe')

            if not symbol or not timeframe:
                logger.warning(f"Missing required fields in candle event: {event}")
                return

            # Get market data from the appropriate source
            candle_data: List[CandleDto] = await self._get_market_data_by_source(symbol, timeframe, source)

            if not candle_data:
                logger.warning(f"No market data available for {symbol} {timeframe} from {source}")
                return

            # 1. First update current context
            await self.context_engine.update_context(symbol, timeframe, candle_data, exchange)

            # 2. Then try to get the MTF set
            market_contexts: List[MarketContext] = await self.context_engine.get_multi_timeframe_contexts(symbol, timeframe, exchange)

            if not market_contexts:
                logger.info(f"Incomplete MTF context for {symbol} {timeframe}. Skipping strategy execution.")
                return

            # Execute all applicable strategies with this data
            await self.execute_strategies(candle_data, market_contexts, source)
            
            # Process mitigation after strategy execution
            await self.execute_mitigation(candle_data)

            # Update the last_updated timestamp after all processing is complete
            last_updated_key = CacheKeys.CANDLE_LAST_UPDATED.format(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe
            )
            
            # Get the latest timestamp from the event or use current time
            event_timestamp = event.get('timestamp')
            current_time = datetime.now().isoformat()
            latest_timestamp = event_timestamp if event_timestamp else current_time
            
            # Update the last updated info in cache
            self.cache_service.set(last_updated_key, {
                'timestamp': latest_timestamp,
                'source': source,
                'processed_at': current_time
            })
            
            logger.debug(f"Updated last processed timestamp for {symbol} {timeframe} to {latest_timestamp}")
        except Exception as e:
            logger.error(f"Error in event-based strategy execution: {e}", exc_info=True)

    async def execute_mitigation(self, candles: List[CandleDto]):
        """
        Execute mitigation processing for indicators.
        
        Args:
            candles: Recent candle data
        """
        try:
            self.mitigation_service.process_mitigation(candles)
        except Exception as e:
            logger.error(f"Error executing mitigation: {e}", exc_info=True)

    async def execute_strategies(self, candle_data: List[CandleDto], market_contexts: List[MarketContext], source: SourceTypeEnum):
        """
        Execute all applicable strategies with the provided market data
        
        Args:
            data: Market data dictionary including candles, symbol, timeframe, etc.
        """
        try:
            # Collect all required indicators across strategies
            required_indicators = set()
            for strategy in self.strategies:
                # Check if strategy is applicable for this symbol and timeframe
                requirements = strategy.get_requirements()
                timeframes = requirements.get('timeframes', [])
                # Skip timeframe checks for now
                # if not timeframes or candle_data[0].timeframe in timeframes:
                    # Add this strategy's indicators to the required set
                indicators_needed = requirements.get('indicators', [])
                required_indicators.update(indicators_needed)
            
            # Run indicators through the DAG
            # Run indicators should return a dictionary where the keys are the indicator names 
            # and values is a list of indicator values
            indicator_results = await self.indicator_dag.run_indicators(
                candle_data,
                market_contexts, 
                requested_indicators=list(required_indicators)
            )
            
            # Execute each applicable strategy
            for strategy in self.strategies:
                try:
                    # Get strategy requirements
                    # requirements = strategy.get_requirements()
                    # timeframes = requirements.get('timeframes', [])
                    
                    # # Skip if this timeframe is not supported by the strategy
                    # if timeframes and candle_data.get('timeframe') not in timeframes:
                    #     continue
                        
                    # Execute the strategy with enhanced data
                    signals = await strategy.analyze(indicator_results)
                    
                    # If a signal was generated, publish it and save to database
                    if signals:
                        # Publish signal to message queue
                        if source == SourceTypeEnum.LIVE:
                            for signal in signals:
                                # Publish each signal to message queue
                                await self._publish_signal(signal)
                        else:
                            logger.info(f"Skip signal generation, currently handling historical data, source = {source}")
                        # Prepare signals for database operations
                        signal_dict = [signal.to_dict() for signal in signals]
                        # Save signal to database
                        saved_signal = await self.signal_repository.bulk_create_signals(signal_dict)
                        
                        if saved_signal:
                            logger.info(f"Saved signal to database: ID {saved_signal.get('id')}")
                        else:
                            logger.warning(f"Failed to save signal to database")                                                  
                except Exception as e:
                    logger.error(f"Error executing strategy {strategy.name}: {e}", exc_info=True)
        
        except Exception as e:
            logger.error(f"Error in strategy execution: {e}", exc_info=True)

    async def _get_market_data_by_source(self, exchange:str, symbol: str, timeframe: str, source: SourceTypeEnum) -> Optional[List[CandleDto]]:
        """
        Retrieve market data based on the source type.
        Uses the last updated timestamp to get candles after that time from the appropriate
        source-specific sorted set. Optimized to fetch only new candles from Redis.
        
        Args:
            symbol: Trading pair
            timeframe: Candle timeframe
            source: Data source ('historical' or 'live')
            
        Returns:
            Dictionary with market data or None if data not available
        """
        try:
            # Build the cache keys based on source
            if source == SourceTypeEnum.HISTORICAL:
                # For historical data, use the historical candle set
                candles_sorted_set_key = CacheKeys.CANDLE_HISTORY_REST_API_DATA.format(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe
                )
            else:
                # For live data, use the live candle set
                candles_sorted_set_key = CacheKeys.CANDLE_LIVE_WEBSOCKET_DATA.format(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe
                )
            
            # Get the last updated timestamp key
            last_updated_key = CacheKeys.CANDLE_LAST_UPDATED.format(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe
                )
            last_updated_info = self.cache_service.get(last_updated_key)
            
            # Determine the minimum score (timestamp) to retrieve candles
            min_score = '-inf'  # Default to get all candles if no last update
            if last_updated_info and isinstance(last_updated_info, dict):
                timestamp = last_updated_info.get('timestamp')
                if timestamp and isinstance(timestamp, str):
                    try:
                        # Convert ISO format to timestamp if needed
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        min_score = dt.timestamp() * 1000
                        logger.debug(f"Retrieving candles after timestamp {min_score}")
                    except ValueError:
                        logger.warning(f"Invalid timestamp format in last_updated_info: {timestamp}")
            
            # Get all candles from the sorted set with scores (timestamps) higher than min_score
            # This way we only retrieve candles newer than the last processed one
            candles = self.cache_service.get_from_sorted_set_by_score(
                candles_sorted_set_key,
                min_score=min_score,
                max_score='+inf',
                with_scores=True
            )
            
            if not candles:
                logger.debug(f"No new candles found for {symbol} {timeframe} from {source}")
                return None
            
            # Convert the candles from JSON strings to CandleDto objects
            candle_dtos: List[CandleDto] = []
            for candle_data in candles:
                # Unpack the candle data and score if with_scores is True
                if isinstance(candle_data, (list, tuple)) and len(candle_data) == 2:
                    candle_json, score = candle_data
                else:
                    candle_json = candle_data
                
                try:
                    # Parse the JSON if it's a string
                    candle_dict: Dict = json.loads(candle_json) if isinstance(candle_json, str) else candle_json
                    
                    # Convert dictionary to CandleDto object
                    candle_dto = CandleDto(
                        symbol=candle_dict.get('symbol'),
                        exchange=candle_dict.get('exchange'),
                        timeframe=candle_dict.get('timeframe'),
                        timestamp=candle_dict.get('timestamp'),
                        open=float(candle_dict.get('open')),
                        high=float(candle_dict.get('high')),
                        low=float(candle_dict.get('low')),
                        close=float(candle_dict.get('close')),
                        volume=float(candle_dict.get('volume')),
                        is_closed=candle_dict.get('is_closed', True),
                        raw_data=candle_dict.get('raw_data'),
                        id=candle_dict.get('id')
                    )
                    
                    candle_dtos.append(candle_dto)
                except (json.JSONDecodeError, TypeError, ValueError) as e:
                    logger.warning(f"Failed to decode or convert candle data: {e} - Data: {candle_json}")
                    continue

            # Sort candles by timestamp (ascending)
            # We need to handle datetime or string timestamps
            candle_dtos.sort(key=lambda x: x.timestamp if isinstance(x.timestamp, (int, float)) 
                            else datetime.fromisoformat(x.timestamp.replace('Z', '+00:00')).timestamp() 
                            if isinstance(x.timestamp, str) else 0)
            
            # Get the latest candle (last in the sorted list)
            latest_candle = candle_dtos[-1] if candle_dtos else None
            
            if not latest_candle:
                logger.debug(f"No latest candle available for {symbol} {timeframe} from {source}")
                return None
            
            # Get market state
            market_state_key = CacheKeys.MARKET_STATE.format(
                exchange=self.config.get('exchange', 'default'),
                symbol=symbol
            )
            market_state = self.cache_service.get(market_state_key) or {}
            
            # Determine how many historical candles we need for strategy lookback
            max_lookback = 0
            for strategy in self.strategies:
                requirements = strategy.get_requirements()
                strategy_lookback = requirements.get('lookback_period', 0)
                max_lookback = max(max_lookback, strategy_lookback)
            
            if max_lookback > candle_dtos:
                if source == SourceTypeEnum.LIVE:
                    # If it's live data and we don't have enough candles,
                    # try to get historical candles to supplement
                    logger.info(f"Not enough live candles for {symbol} {timeframe}. Found: {len(candle_dtos)}, getting historical data")
                    original_live_candles = candle_dtos.copy()
                    # Get the historical candle set key
                    historical_candles_key = CacheKeys.CANDLE_HISTORY_REST_API_DATA.format(
                        exchange=exchange,
                        symbol=symbol,
                        timeframe=timeframe
                    )
                    
                    # Fetch the needed number of historical candles
                    additional_candles_needed = max_lookback - len(candle_dtos)
                    logger.debug(f"Fetching {additional_candles_needed} additional historical candles")
                    
                    # Get the most recent historical candles
                    historical_candles = self.cache_service.get_from_sorted_set_by_score(
                        historical_candles_key,
                        min_score='-inf',
                        max_score='+inf',
                        with_scores=True,
                        limit=additional_candles_needed,
                        descending=True
                    )

                    if not historical_candles:
                        logger.debug(f"Failed to fetch historical candles for {symbol} {timeframe} from {source}")
                        return None
                    
                    # Process historical candles and add them to historical_candle_dtos
                    historical_candle_dtos: List[CandleDto] = []
                    for candle_json in historical_candles:
                        try:
                            # Parse the JSON if it's a string
                            candle_dict = json.loads(candle_json) if isinstance(candle_json, str) else candle_json
                            
                            # Convert dictionary to CandleDto object
                            candle_dto = CandleDto(
                                symbol=candle_dict.get('symbol'),
                                exchange=candle_dict.get('exchange'),
                                timeframe=candle_dict.get('timeframe'),
                                timestamp=candle_dict.get('timestamp'),
                                open=float(candle_dict.get('open')),
                                high=float(candle_dict.get('high')),
                                low=float(candle_dict.get('low')),
                                close=float(candle_dict.get('close')),
                                volume=float(candle_dict.get('volume')),
                                is_closed=candle_dict.get('is_closed', True),
                                raw_data=candle_dict.get('raw_data'),
                                id=candle_dict.get('id')
                            )
                            
                            historical_candle_dtos.append(candle_dto)
                        except (json.JSONDecodeError, TypeError, ValueError) as e:
                            logger.warning(f"Failed to decode historical candle JSON: {e}")
                            continue
                    
                    # Sort historical candles (newest first since we retrieved them in descending order)
                    historical_candle_dtos.sort(key=lambda x: x.timestamp if isinstance(x.timestamp, (int, float)) 
                                else datetime.fromisoformat(x.timestamp.replace('Z', '+00:00')).timestamp() 
                                if isinstance(x.timestamp, str) else 0, reverse=True)
                    
                    # Sort live candles (oldest first)
                    original_live_candles.sort(key=lambda x: x.timestamp if isinstance(x.timestamp, (int, float)) 
                                else datetime.fromisoformat(x.timestamp.replace('Z', '+00:00')).timestamp() 
                                if isinstance(x.timestamp, str) else 0)
                        
                    # Check for gap between latest historical and earliest live candle
                    latest_historical = historical_candle_dtos[0]  # First is latest since we sorted in reverse
                    earliest_live = original_live_candles[0]  # First is earliest since we sorted normally
                    
                    # Get timestamps
                    hist_time = latest_historical.timestamp
                    live_time = earliest_live.timestamp
                    
                    # Convert to float timestamp if it's a datetime
                    if isinstance(hist_time, datetime):
                        hist_time = hist_time.timestamp()
                    if isinstance(live_time, datetime):
                        live_time = live_time.timestamp()
                            
                    # Calculate timeframe in seconds
                    timeframe_seconds = self._timeframe_to_seconds(timeframe)
                    
                    # Check for gap: should be less than 2x the timeframe
                    if (live_time - hist_time) > (2 * timeframe_seconds):
                        logger.warning(f"Gap detected between historical and live candles for {symbol} {timeframe}. " 
                                    f"Gap: {live_time - hist_time} seconds, expected less than {2 * timeframe_seconds} seconds")
                        return None
                        
                    #     # If gap check passes, add the needed historical candles to our main list
                    needed_count = min(additional_candles_needed, len(historical_candle_dtos))
                    candle_dtos.extend(historical_candle_dtos[:needed_count])

                    # Check again if we have enough candles after adding historical data
                    if len(candle_dtos) < max_lookback:
                        logger.warning(f"Still not enough candles after adding historical data for {symbol} {timeframe}. Found: {len(candle_dtos)}")
                        return None
                    
                    # Sort candles by timestamp to ensure they're in order
                    candle_dtos.sort(key=lambda x: x.timestamp if isinstance(x.timestamp, (int, float)) 
                                else datetime.fromisoformat(x.timestamp.replace('Z', '+00:00')).timestamp() 
                                if isinstance(x.timestamp, str) else 0)
                else:
                    # If it's historical data and we don't have enough candles, just return None
                    logger.warning(f"Not enough historical candles for {symbol} {timeframe}. Found: {len(candle_dtos)}, minimum required: {max_lookback}")
                    return None
            
            # Build market data dictionary
            data = {
                'symbol': symbol,
                'timeframe': timeframe,
                'exchange': self.config.get('exchange', 'default'),
                'timestamp': datetime.now().isoformat(),
                'candles': candle_dtos,
                'latest_candle': latest_candle,
                'current_price': latest_candle.close,
                'market_state': market_state,
                'source': source,
                'last_updated': last_updated_info
            }
            
            # Update the last updated timestamp to the latest candle's timestamp
            latest_timestamp = latest_candle.timestamp
            self.cache_service.set(last_updated_key, {
                'timestamp': latest_timestamp,
                'source': source
            })
            
            return data
            
        except Exception as e:
            logger.error(f"Error retrieving {source} market data for {symbol} {timeframe}: {e}", exc_info=True)
            return None
    
    async def _publish_signal(self, signal: SignalDto) -> bool:
        """
        Publish a signal to the queue and cache
        
        Args:
            signal: Signal to publish
            
        Returns:
            True if successfully published, False otherwise
        """
        try:
            # Ensure signal has all required fields
            if not signal.symbol:
                logger.error("Cannot publish signal: missing symbol")
                return False
            
            # Convert signal to dictionary
            signal_dict = signal.to_dict()
            
            # Create routing key
            routing_key = RoutingKeys.ORDER_BLOCK_DETECTED.format(
                exchange=signal.exchange,
                symbol=signal.symbol,
                timeframe=signal.timeframe or "default"
            )
            
            # Publish to the strategy exchange
            self.producer_queue.publish(
                Exchanges.STRATEGY,
                routing_key,
                signal_dict
            )
            
            # Also cache the signal
            signal_key = CacheKeys.SIGNAL.format(
                exchange=signal.exchange,
                symbol=signal.symbol,
                id=signal.id
            )
            
            self.cache_service.set(
                signal_key,
                signal_dict,
                expiry=CacheTTL.SIGNAL_DATA
            )
            
            # Update active signals hash
            active_signals_key = CacheKeys.ACTIVE_SIGNALS_HASH.format(
                exchange=signal.exchange,
                symbol=signal.symbol
            )
            
            self.cache_service.hash_set(
                active_signals_key,
                signal.id,
                signal_dict
            )
            
            logger.info(f"Published signal: {signal.id} ({signal.strategy} for {signal.symbol})")
            return True
            
        except Exception as e:
            logger.error(f"Error publishing signal: {e}", exc_info=True)
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