# strategy/engine/runner.py
import asyncio
import logging
import json
from typing import Dict, Any, List
from datetime import datetime

from strategy.strategies.base import Strategy
from shared.dto.signal import Signal
from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges, Queues, RoutingKeys, CacheKeys, CacheTTL

logger = logging.getLogger(__name__)

class StrategyRunner:
    """
    Main runner for strategy execution
    Handles:
    - Data retrieval from cache
    - Strategy execution
    - Signal publishing
    """
    
    def __init__(
        self, 
        strategies: List[Strategy], 
        cache_service: CacheService,
        producer_queue: QueueService,
        config: Dict[str, Any]
    ):
        """
        Initialize the strategy runner
        
        Args:
            strategies: List of strategies to run
            cache_service: Cache service for data retrieval
            producer_queue: Queue service for publishing signals
            config: Configuration dictionary
        """
        self.strategies = strategies
        self.cache_service = cache_service
        self.producer_queue = producer_queue
        self.config = config
        self.running = False
        self.execution_task = None
    
    async def start(self):
        """Start the strategy runner"""
        if self.running:
            logger.warning("Strategy runner is already running")
            return
        
        self.running = True
        logger.info("Starting strategy runner...")
        
        # Initialize the signal exchange
        self.producer_queue.declare_exchange(Exchanges.STRATEGY)
        
        # Start the execution loop in a background task
        self.execution_task = asyncio.create_task(self._run_execution_loop())
        
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
    
    async def _run_execution_loop(self):
        """Main execution loop for the strategy runner"""
        try:
            execution_interval = self.config.get('execution_interval_seconds', 1)
            
            while self.running:
                try:
                    await self.execute_strategies()
                except Exception as e:
                    logger.error(f"Error during strategy execution: {e}", exc_info=True)
                
                # Wait for the next execution cycle
                await asyncio.sleep(execution_interval)
                
        except asyncio.CancelledError:
            logger.info("Strategy execution loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in strategy execution loop: {e}", exc_info=True)
    
    async def execute_strategies(self):
        """Execute all strategies with current market data"""
        # Get symbols and timeframes to process from config
        symbols = self.config.get('symbols', [])
        timeframes = self.config.get('timeframes', [])
        
        for symbol in symbols:
            for timeframe in timeframes:
                # Get market data from cache
                data = await self._get_market_data(symbol, timeframe)
                if not data:
                    continue
                
                # Execute each strategy
                for strategy in self.strategies:
                    try:
                        # Get strategy requirements
                        requirements = strategy.get_requirements()
                        supported_timeframes = requirements.get('timeframes', [])
                        
                        # Skip if this timeframe is not supported by the strategy
                        if supported_timeframes and timeframe not in supported_timeframes:
                            continue
                            
                        # Execute the strategy
                        signal = await strategy.analyze(data)
                        
                        # If a signal was generated, publish it
                        if signal:
                            await self._publish_signal(signal)
                            logger.info(f"Generated signal from {strategy.name} for {symbol} ({timeframe})")
                    except Exception as e:
                        logger.error(f"Error executing strategy {strategy.name}: {e}", exc_info=True)
    
    async def _get_market_data(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """
        Retrieve market data from cache
        
        Args:
            symbol: Trading pair
            timeframe: Candle timeframe
            
        Returns:
            Dictionary with market data
        """
        try:
            # Get latest candle
            latest_key = CacheKeys.LATEST_CANDLE.format(
                exchange=self.config.get('exchange', 'default'),
                symbol=symbol,
                timeframe=timeframe
            )
            latest_candle = self.cache_service.get(latest_key)
            
            if not latest_candle:
                logger.debug(f"No latest candle found for {symbol} {timeframe}")
                return None
            
            # Determine max lookback needed by all strategies
            max_lookback = 100  # Default
            for strategy in self.strategies:
                requirements = strategy.get_requirements()
                strategy_lookback = requirements.get('lookback_period', 0)
                max_lookback = max(max_lookback, strategy_lookback)
            
            # Get candle history keys
            history_key = CacheKeys.CANDLE_HISTORY_SET.format(
                exchange=self.config.get('exchange', 'default'),
                symbol=symbol,
                timeframe=timeframe
            )
            
            # Get candle IDs
            candle_ids = self.cache_service.get_from_sorted_set(
                history_key, 
                0, 
                max_lookback - 1,  # -1 because we already have the latest candle
                desc=True  # Most recent first
            )
            
            # Get candles data
            candles = [latest_candle]  # Start with latest candle
            for candle_id in candle_ids:
                candle_key = CacheKeys.CANDLE_DATA.format(
                    exchange=self.config.get('exchange', 'default'),
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=candle_id
                )
                candle = self.cache_service.get(candle_key)
                if candle:
                    candles.append(candle)
            
            # Get market state
            market_state_key = CacheKeys.MARKET_STATE.format(
                exchange=self.config.get('exchange', 'default'),
                symbol=symbol
            )
            market_state = self.cache_service.get(market_state_key) or {}
            
            # Build market data dictionary
            data = {
                'symbol': symbol,
                'timeframe': timeframe,
                'exchange': self.config.get('exchange', 'default'),
                'timestamp': datetime.now().isoformat(),
                'candles': candles,
                'latest_candle': latest_candle,
                'current_price': latest_candle.get('close'),
                'market_state': market_state
            }
            
            # Update and add market structure
            if hasattr(self, 'market_structure'):
                market_context = await self.market_structure.update(data)
                data['market_context'] = market_context
            
            return data
            
        except Exception as e:
            logger.error(f"Error retrieving market data for {symbol} {timeframe}: {e}", exc_info=True)
            return None
    
    async def _publish_signal(self, signal: Signal) -> bool:
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
            routing_key = RoutingKeys.SIGNAL_GENERATED.format(
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