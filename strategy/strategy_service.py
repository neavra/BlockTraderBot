# strategy/service.py
import logging
import asyncio
from typing import Dict, List, Any, Optional

from shared.queue.queue_service import QueueService
from shared.cache.cache_service import CacheService
from shared.constants import Exchanges, Queues, RoutingKeys

from strategy.indicators.base import Indicator
from strategy.strategies.base import Strategy
from strategy.engine.strategy_runner import StrategyRunner
from strategy.strategies.strategy_factory import StrategyFactory
from strategy.indicators.indicator_factory import IndicatorFactory
from strategy.context.context_engine import ContextEngine
from data.database.db import Database
from strategy.domain.types.indicator_type_enum import IndicatorType


logger = logging.getLogger(__name__)

class StrategyService:
    """
    Main service class for the Strategy Layer.
    Encapsulates all strategy layer functionality.
    """
    
    def __init__(
        self, 
        consumer_queue: QueueService, 
        producer_queue: QueueService,
        cache_service: CacheService,
        config: Dict[str, Any]
    ):
        """
        Initialize the strategy service.
        
        Args:
            consumer_queue: Queue service for consuming data events
            producer_queue: Queue service for producing signal events
            cache_service: Cache service for retrieving market data
            config: Configuration dictionary
        """
        # TODO Refactor the definition of queues properly
        self.database = Database(db_url=config["data"]["database"]["database_url"])
        self.consumer_queue = consumer_queue
        self.producer_queue = producer_queue
        self.cache_service = cache_service
        self.config = config
        self.running = False
        
        # Components to be initialized in start()
        self.indicators = {}
        self.strategies = []
        self.context_engine = None
        self.strategy_runner = None
        self.main_loop = None
    
    async def start(self):
        """Initialize and start the strategy service."""
        if self.running:
            logger.warning("Strategy service is already running")
            return
        
        logger.info("Starting strategy service...")
        
        # Store the event loop for callbacks
        self.main_loop = asyncio.get_running_loop()
        
        # Initialize market context
        await self._init_market_context()

        # Initialize indicators
        await self._init_indicators()
        
        # Initialize strategies
        await self._init_strategies()
        
        # Initialize data event consumer
        # await self._init_data_consumer()
        
        # Initialize the strategy runner
        await self._init_strategy_runner()
        
        self.running = True
        logger.info("Strategy service started successfully")
    
    async def stop(self):
        """Stop the strategy service and release resources."""
        logger.info("Stopping strategy service...")
        
        if self.strategy_runner:
            await self.strategy_runner.stop()
        
        # if self.consumer_queue:
        #     self.consumer_queue.stop()
        
        if self.producer_queue:
            self.producer_queue.stop()
        
        self.running = False
        logger.info("Strategy service stopped")
    
    async def _init_market_context(self):
        """Initialize market context components."""
        logger.info("Initializing market context...")
        
        try:
            # Get market context configuration
            market_context_params = self.config.get('strategy', {}).get('market_context', {})

            # Initialize market structure with cache service
            self.context_engine = ContextEngine(
                cache_service=self.cache_service,
                database=self.database,
                config=market_context_params
            )

            await self.context_engine.start()

            # Initialize analyzers if specified in config
            if 'analyzers' in market_context_params:
                logger.info(f"Initialized context with {len(self.context_engine.analyzers)} analyzers")
            else:
                logger.info("Market structure initialized without analyzers")
        except Exception as e:
            logger.error(f"Failed to initialize market structure: {e}")
            raise
        
    async def _init_indicators(self):
        """Initialize indicators based on configuration."""
        logger.info("Initializing indicators...")
        
        # Create indicator factory
        indicator_factory = IndicatorFactory(self.database)
        
        # Get indicator configurations
        indicator_configs = self.config.get('strategy', {}).get('indicators', {})
        
        # Create each indicator
        for ind_name_str, ind_config in indicator_configs.items():
            if not ind_config.get('enabled', True):
                continue
                
            try:
                ind_enum = IndicatorType.get_by_type_name(ind_name_str)
                indicator = indicator_factory.create_indicator(
                    ind_enum, 
                    params=ind_config.get('params', {})
                )
                self.indicators[ind_enum] = indicator
                logger.info(f"Initialized indicator: {ind_name_str}")
            except Exception as e:
                logger.error(f"Failed to initialize indicator {ind_enum}: {e}")
        
        logger.info(f"Initialized {len(self.indicators)} indicators")
    
    async def _init_strategies(self):
        """Initialize strategies based on configuration."""
        logger.info("Initializing strategies...")
        
        # Create strategy factory
        strategy_factory = StrategyFactory()
        
        # Get strategy configurations
        strategy_configs = self.config.get('strategy', {}).get('strategies', {})
        
        # Create each strategy
        for strat_name, strat_config in strategy_configs.items():
            if not strat_config.get('enabled', True):
                continue
                
            try:
                logger.info(f"Initializing strategy: {strat_name}")
                strategy = strategy_factory.create_strategy(
                    strat_name,
                    indicators=self.indicators,
                    params=strat_config.get('params', {})
                )
                self.strategies.append(strategy)
                logger.info(f"Initialized strategy: {strat_name}")
            except Exception as e:
                logger.error(f"Failed to initialize strategy {strat_name}: {e}")
        
        logger.info(f"Initialized {len(self.strategies)} strategies")
    
    async def _init_strategy_runner(self):
        """Initialize the strategy runner."""
        logger.info("Initializing strategy runner...")
        
        try:
            self.strategy_runner = StrategyRunner(
                strategies=self.strategies,
                cache_service=self.cache_service,
                producer_queue=self.producer_queue,
                consumer_queue=self.consumer_queue,
                context_engine=self.context_engine,
                database=self.database,
                config=self.config.get('strategy', {})
            )
            
            # Start the runner
            await self.strategy_runner.start()
            
            logger.info("Strategy runner initialized and started")
        except Exception as e:
            logger.error(f"Failed to initialize strategy runner: {e}")
            raise
    
    # async def _init_data_consumer(self):
    #     """Initialize the data event consumer."""
    #     logger.info("Initializing data event consumer...")
        
    #     try:
    #         # Set up exchange and queue
    #         self.consumer_queue.declare_exchange(Exchanges.MARKET_DATA)
    #         self.consumer_queue.declare_queue(Queues.EVENTS)
    #         self.consumer_queue.bind_queue(
    #             exchange=Exchanges.MARKET_DATA,
    #             queue=Queues.EVENTS,
    #             routing_key=RoutingKeys.DATA_EVENT_HANDLE
    #         )
            
    #         # Subscribe to the queue
    #         self.consumer_queue.subscribe(
    #             Queues.EVENTS,
    #             self._on_candle_event
    #         )
            
    #         logger.info("Data event consumer initialized")
    #     except Exception as e:
    #         logger.error(f"Failed to initialize data event consumer: {e}")
    #         raise
    
    # def _on_candle_event(self, event: Dict[str, Any]):
    #     """
    #     Callback for handling candle events from the queue.
    #     This is called when a new candle is received.
        
    #     Args:
    #         event: Candle event data
    #     """
    #     try:
    #         logger.debug(f"Received candle event: {event.get('symbol')} {event.get('timeframe')}")
            
    #         # Schedule strategy execution in the background
    #         # This allows us to react to new market data immediately
    #         if self.main_loop and self.strategy_runner:
    #             asyncio.run_coroutine_threadsafe(
    #                 self.strategy_runner.execute_strategies(),
    #                 self.main_loop
    #             )
    #     except Exception as e:
    #         logger.error(f"Error processing candle event: {e}")