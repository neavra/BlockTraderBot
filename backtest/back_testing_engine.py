import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from data.managers.candle_manager import CandleManager
from data.connectors.rest.factory import RestClientFactory
from strategy.engine.strategy_runner import StrategyRunner
from shared.cache.cache_service import CacheService
from shared.queue.queue_service import QueueService
from data.database.db import Database
from shared.domain.dto.candle_dto import CandleDto
from time_manager import TimeManager
from strategy.domain.models.market_context import MarketContext
from shared.domain.dto.signal_dto import SignalDto
from shared.domain.types.source_type_enum import SourceTypeEnum


logger = logging.getLogger(__name__)

class BackTestConfiguration:
    """Configuration for backtesting parameters."""
    
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        exchange: str,
        start_time: datetime,
        end_time: datetime,
        initial_capital: float = 100000.0,
        **kwargs
    ):
        """
        Initialize backtest configuration.
        
        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            exchange: Exchange name
            start_time: Backtest start time
            end_time: Backtest end time
            initial_capital: Starting capital for backtesting
            **kwargs: Additional configuration parameters
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.exchange = exchange
        self.start_time = start_time
        self.end_time = end_time
        self.initial_capital = initial_capital
        self.config = kwargs


class BackTestingEngine:
    """
    Main orchestrator for the backtesting framework.
    Entry point that initializes all dependencies and manages the backtesting process.
    """
    
    def __init__(self, config: Dict[str, Any], backtest_config: BackTestConfiguration):
        """
        Initialize the backtesting engine.
        
        Args:
            config: Global system configuration
            backtest_config: Backtesting-specific configuration
        """
        self.config = config
        self.backtest_config = backtest_config
        self.running = False
        self.logger = logging.getLogger("BackTestingEngine")
        
        # Core components - will be initialized in start()
        self.time_manager: Optional[TimeManager] = None
        self.database: Optional[Database] = None
        self.cache_service: Optional[CacheService] = None
        self.queue_service: Optional[QueueService] = None
        self.candle_manager: Optional[CandleManager] = None
        self.strategy_service = None
        self.execution_service = None  # Placeholder for ExecutionService
        self.monitoring_service = None  # Placeholder for BackTestingMonitoringService
        
        # Data storage
        self.historical_candles: List[CandleDto] = []
        
    async def start(self):
        """Initialize and start the backtesting engine and all its components."""
        if self.running:
            self.logger.warning("BackTesting Engine is already running")
            return
        
        self.running = True
        self.logger.info("Starting BackTesting Engine...")
        
        # Step 1: Initialize all dependencies
        self.logger.info("Initializing BackTesting Engine dependencies...")
        
        # Initialize time manager
        self.time_manager = TimeManager()
        
        # Initialize database with backtesting-specific configuration
        self.database = Database(
            db_url=self.config["data"]["database"]["database_url"],
            echo=False  # Disable SQL logging for performance
        )
        
        # Initialize cache service with backtesting keys
        self.cache_service = CacheService()
        
        # Initialize queue service (disabled for backtesting)
        self.queue_service = QueueService()
        
        # Initialize candle manager with backtesting configuration
        await self._init_candle_manager()
        
        # Initialize strategy service with backtesting dependencies
        await self._init_strategy_service()
        
        # Initialize execution service (placeholder)
        await self._init_execution_service()
        
        # Initialize monitoring service (placeholder)
        await self._init_monitoring_service()
        
        # Step 2: Start all components
        await self.candle_manager.start()
        await self.strategy_service.start()
        
        # TODO: Start execution and monitoring services
        # await self.execution_service.start()
        # await self.monitoring_service.start()
        
        self.logger.info("BackTesting Engine started successfully")
    
    async def stop(self):
        """Stop the backtesting engine and clean up resources."""
        if not self.running:
            self.logger.warning("BackTesting Engine is not running")
            return
        
        self.logger.info("Stopping BackTesting Engine...")
        self.running = False
        
        # Stop all components
        # if self.candle_manager:
        #     await self.candle_manager.stop()
        # if self.strategy_service:
        #     await self.strategy_service.stop()
        
        # TODO: Stop execution and monitoring services
        # if self.execution_service:
        #     await self.execution_service.stop()
        # if self.monitoring_service:
        #     await self.monitoring_service.stop()
        
        # Close database connection
        # if self.database:
        #     await self.database.disconnect()
        
        # # Close cache service
        # if self.cache_service:
        #     self.cache_service.close()
        
        self.logger.info("BackTesting Engine stopped")
    
    async def _init_candle_manager(self):
        """Initialize the candle manager with backtesting configuration."""
        self.logger.info("Initializing CandleManager for backtesting...")
        
        # Create backtesting-specific config
        backtest_data_config = self.config.copy()
        # TODO: Modify config to use backtesting cache keys and disable queue publishing
        
        self.candle_manager = CandleManager(
            database=self.database,
            config=backtest_data_config
        )
    
    async def _init_strategy_service(self):
        """Initialize the strategy service with backtesting dependencies."""
        self.logger.info("Initializing StrategyService for backtesting...")
        
        # Import StrategyService
        from strategy.strategy_service import StrategyService
        
        # Initialize StrategyService with backtesting configuration
        # StrategyService will handle initialization of ContextEngine, MitigationService, and StrategyRunner
        self.strategy_service = StrategyService(
            cache_service=self.cache_service,
            producer_queue=self.queue_service,  # Disabled for backtesting
            consumer_queue=self.queue_service,  # Disabled for backtesting
            config=self.config,
            is_backtest=True,
        )
    
    async def _init_execution_service(self):
        """Initialize the execution service with backtesting configuration."""
        self.logger.info("Initializing ExecutionService for backtesting...")
        
        # TODO: Initialize ExecutionService with backtesting flag
        # TODO: Initialize BackTestingOrderExecutor
        
        self.execution_service = None  # Placeholder
    
    async def _init_monitoring_service(self):
        """Initialize the monitoring service for backtesting."""
        self.logger.info("Initializing BackTestingMonitoringService...")
        
        # TODO: Initialize BackTestingMonitoringService
        # TODO: Initialize portfolio state management
        
        self.monitoring_service = None  # Placeholder
    
    async def load_historical_data(
        self,
        symbol: str,
        timeframe: str,
        exchange: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[CandleDto]:
        """
        Load historical candle data for backtesting.
        
        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            exchange: Exchange name
            start_time: Start time for data
            end_time: End time for data
            
        Returns:
            List of historical candles in chronological order
        """
        self.logger.info(f"Loading historical data for {symbol} {timeframe} from {start_time} to {end_time}")
        
        try:
            # Create RestClient using RestClientFactory
            rest_client = RestClientFactory.create(
                exchange=exchange,
                symbol=symbol,
                interval=timeframe
            )
            
            # Convert datetime to milliseconds
            start_ts = int(start_time.timestamp() * 1000)
            end_ts = int(end_time.timestamp() * 1000)
            current_start = start_ts
            
            all_normalized_candles = []
            
            # Query data in chunks to handle large time ranges (similar to _fetch_initial_history)
            while current_start < end_ts - (self._timeframe_to_seconds(timeframe) * 1000):
                self.logger.info(f"Fetching chunk starting from {datetime.fromtimestamp(current_start/1000, tz=timezone.utc)}")
                
                # Fetch chunk of candles (max 1000 per request)
                raw_data = await rest_client.fetch_candlestick_data(
                    limit=1000,
                    startTime=current_start,
                    endTime=end_ts  # Use end_ts to ensure we don't go beyond our target
                )
                
                if not raw_data:
                    self.logger.warning(f"No data returned for chunk starting at {current_start}")
                    break

                normalizer = self.candle_manager._get_rest_normalizer(exchange)
            
                normalized_data_list = []
                # Process each candle in the list
                for data in raw_data:
                    # Normalize the data
                    normalized_candle: CandleDto = await normalizer.normalize_rest_data(
                        data=data, exchange=exchange, symbol=symbol, interval=timeframe
                    )
                    
                    normalized_data_list.append(normalized_candle)
                
                if not normalized_data_list:
                    self.logger.warning(f"No normalized candles returned for chunk")
                    break
                
                # Add to our collection
                all_normalized_candles.extend(normalized_data_list)
                
                # Update start time for next chunk
                last_candle_time = normalized_data_list[-1].timestamp
                if isinstance(last_candle_time, datetime):
                    current_start = int(last_candle_time.timestamp() * 1000) + (self._timeframe_to_seconds(timeframe) * 1000)
                else:
                    # Handle case where timestamp might be a string
                    last_dt = datetime.fromisoformat(last_candle_time.replace('Z', '+00:00'))
                    current_start = int(last_dt.timestamp() * 1000) + (self._timeframe_to_seconds(timeframe) * 1000)
                
                self.logger.info(f"Loaded {len(normalized_data_list)} candles for {symbol}/{timeframe} (total: {len(all_normalized_candles)})")
                
                # Break if we've reached our end time
                if current_start >= end_ts:
                    break
            
            # Sort candles chronologically to ensure proper order
            all_normalized_candles.sort(key=lambda x: x.timestamp if isinstance(x.timestamp, datetime) 
                                       else datetime.fromisoformat(x.timestamp.replace('Z', '+00:00')))
            
            #
            # Store for internal use
            self.historical_candles = all_normalized_candles
            self.time_manager.set_total_candles(len(self.historical_candles))
            
            self.logger.info(f"Successfully loaded {len(all_normalized_candles)} historical candles for {symbol} {timeframe}")
            return all_normalized_candles
            
        except Exception as e:
            self.logger.error(f"Error loading historical data for {symbol}/{timeframe}: {e}", exc_info=True)
            return []
        
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
    
    async def run_backtest(self) -> Dict[str, Any]:
        """
        Execute the main backtesting loop.
        
        Returns:
            Backtesting results and performance metrics
        """
        self.logger.info("Starting backtest execution...")
        
        try:
            symbol = self.backtest_config.symbol
            timeframe = self.backtest_config.timeframe
            exchange = self.backtest_config.exchange
            # Step 1: Load historical data
            candles = await self.load_historical_data(
                symbol=symbol,
                timeframe=timeframe,
                exchange=exchange,
                start_time=self.backtest_config.start_time,
                end_time=self.backtest_config.end_time
            )
            
            if not candles:
                raise ValueError("No historical data loaded")
            
            # # Step 2: Main execution loop
            self.logger.info(f"Processing {len(candles)} candles...")
            
            window_size = 100  # Number of candles to include in each window
            
            # Process candles with sliding window approach
            for i in range(len(candles)):
                # Create sliding window: start from max(0, i-window_size+1) to i+1
                window_start = max(0, i - window_size + 1)
                window_end = i + 1
                candle_data = candles[window_start:window_end]
                current_candle = candles[i]
                # a. Set current simulation time
                self.time_manager.set_current_time(current_candle.timestamp)
                                
                # b. Execute strategies
                signals = []
                await self.strategy_service.strategy_runner.context_engine.update_context(symbol, timeframe, candle_data, exchange)

                market_contexts: List[MarketContext] = await self.strategy_service.strategy_runner.context_engine.get_multi_timeframe_contexts(symbol, timeframe, exchange)
                if not market_contexts:
                    logger.info(f"Incomplete MTF context for {symbol} {timeframe}. Skipping strategy execution.")
                    continue

                result : List[SignalDto] = await self.strategy_service.strategy_runner.execute_strategies(candle_data, market_contexts, SourceTypeEnum.HISTORICAL)
                if not result:
                    logger.info("NO signals generated")
                    continue
                signals.extend(result)

                # c. Process signals through execution layer
                orders = []  # TODO: ExecutionService.process_signals(signals)
                
                # d. Update portfolio state
                # TODO: BackTestingMonitoringService.update_portfolio_state()
                
                # e. Advance to next candle
                self.time_manager.advance_to_next_candle()
                
                # Log progress periodically
                if self.time_manager.candle_index % 1000 == 0:
                    progress = self.time_manager.get_progress()
                    self.logger.info(f"Backtest progress: {progress:.1f}%")
            
            # # Step 3: Generate final report
            # # TODO: results = BackTestingMonitoringService.generate_final_report()
            results = {}  # Placeholder
            self.logger.info(f"Backtest execution completed successfully, signals generated: {signals}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error during backtest execution: {e}", exc_info=True)
            raise