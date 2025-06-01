import logging
import asyncio
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from strategy.domain.types.time_frame_enum import TimeframeEnum, TimeframeCategoryEnum
from strategy.domain.models.market_context import MarketContext
from shared.constants import CacheKeys, CacheTTL
from strategy.context.analyzers.factory import AnalyzerFactory
from strategy.context.analyzers.base import BaseAnalyzer
from shared.cache.cache_service import CacheService
from shared.domain.dto.candle_dto import CandleDto
from data.database.db import Database
from data.database.repository.market_context_repository import MarketContextRepository
from strategy.domain.types.time_frame_enum import TIMEFRAME_HIERARCHY

logger = logging.getLogger(__name__)

class ContextEngine:
    """
    Main engine for market context analysis and maintenance.
    Analyzes candle data to create and update market contexts across timeframes.
    Uses cache for storing contexts instead of in-memory dictionary.
    """
    
    def __init__(
        self, 
        cache_service: CacheService,
        database: Database,
        config: Dict[str, Any]
    ):
        """
        Initialize the context engine.
        
        Args:
            cache_service: Cache service for storing/retrieving contexts
            database_service: Database for persistent storage of historical contexts
            config: Configuration dictionary with analyzer parameters
        """
        self.cache_service = cache_service
        self.repository = MarketContextRepository(database)
        self.config = config
        self.running = False
        
        # Components to be initialized in start()
        self.analyzers: Dict[str, BaseAnalyzer] = {}
        self.main_loop = None
    
    async def start(self):
        """Initialize and start the context engine."""
        if self.running:
            logger.warning("Context engine is already running")
            return
        
        logger.info("Starting context engine...")
        
        # Store the event loop for callbacks
        self.main_loop = asyncio.get_running_loop()
        
        # Initialize analyzers
        await self._init_analyzers()
        
        self.running = True
        logger.info("Context engine started successfully")
    
    async def stop(self):
        """Stop the context engine and release resources."""
        logger.info("Stopping context engine...")
        self.running = False
        logger.info("Context engine stopped")
    
    async def _init_analyzers(self):
        """Initialize analyzers based on configuration."""
        logger.info("Initializing market analyzers...")
        analyzer_config = self.config.get('analyzers', {})
        analyzer_factory = AnalyzerFactory(analyzer_config)
        
        # Create default analyzers
        # self.analyzers = analyzer_factory.create_default_analyzers()
        analyzers = {}
        logger.info(analyzer_config)
        for analyzer_name in analyzer_config.keys():
            analyzers[analyzer_name] = analyzer_factory.create_analyzer(analyzer_name)
        self.analyzers = analyzers
        # Store references to commonly used analyzers
        self.swing_detector = self.analyzers.get('swing')
        self.trend_analyzer = self.analyzers.get('trend')
        self.range_detector = self.analyzers.get('range')
        self.fibbonacci_analyzer = self.analyzers.get('fibbonacci')
        
        logger.info(f"Initialized {len(self.analyzers)} market analyzers")
    
    def _get_context_cache_key(self, symbol: str, timeframe: str, exchange: str) -> str:
        """Generate a consistent cache key for a market context."""
        return CacheKeys.MARKET_STATE.format(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe
        )
    
    async def update_context(self, symbol: str, timeframe: str, candles: List[CandleDto], exchange: str = None) -> Optional[MarketContext]:
        if not candles:
            logger.warning(f"No candles provided for {symbol} {timeframe}")
            return None

        cache_key = self._get_context_cache_key(symbol, timeframe, exchange)
        cached_context_data = self.cache_service.get(cache_key)
        is_first_time = False
        if cached_context_data is None:
            is_first_time = True
            existing_context = MarketContext(symbol, timeframe, exchange)
        else:
            existing_context = MarketContext.from_dict(cached_context_data)

        # Note here, that the existing_context object is being edited directly, no new object is created
        context = existing_context
        updated_flag = False
        for analyzer_type, analyzer in self.analyzers.items():
            if analyzer:
                try:
                    context, updated = analyzer.update_market_context(existing_context, candles)
                    if updated:
                        updated_flag = True
                except Exception as e:
                    logger.error(f"Error updating context with {analyzer_type} analyzer: {e}")

        # Update time stamp for context
        context.last_updated = datetime.now().timestamp()
        context.timestamp = datetime.now().isoformat()
        context.set_current_price(candles[-1].close)
        
        # need to check the context properly here
        if not context.is_complete():
            logger.info(f"Market Context is incomplete {context}")
            
        # If this is the first market context created, add it to the cache
        # Else, add the old one to the repository and set the new one to the cache
        if is_first_time:
            logger.info(f"Market Context setting market context for the first time: {context}")
            self.cache_service.set(cache_key, context.to_dict(), expiry=CacheTTL.MARKET_STATE)
        elif updated_flag:
            self.cache_service.set(cache_key, context.to_dict(), expiry=CacheTTL.MARKET_STATE)
            try:
                await self._store_context_history(existing_context)
            except Exception as e:
                logger.error(f"Error storing historical context: {e}")

        return context
    
    async def get_multi_timeframe_contexts(
        self, symbol: str, base_timeframe: str, exchange: str = "default"
    ) -> List[MarketContext]:
        """
        Get required multi-timeframe MarketContexts for a base timeframe.
        Returns None if any required context is missing.
        """
        required_timeframes = TIMEFRAME_HIERARCHY.get(base_timeframe, [])
        if not required_timeframes:
            logger.warning(f"Timeframe {base_timeframe} not supported")
        contexts = []
        
        for tf in required_timeframes:
            cache_key = self._get_context_cache_key(symbol, tf, exchange)
            cached_context_data = self.cache_service.get(cache_key)
            if not cached_context_data:
                logger.debug(f"Missing cached context for {symbol} {tf} on {exchange}")
                continue
            
            # Convert dictionary back to MarketContext object
            try:
                context = MarketContext.from_dict(cached_context_data)
            except Exception as e:
                logger.error(f"Error converting cached context data to MarketContext for {symbol} {tf}: {e}")
                continue
            
            contexts.append(context)
        
        if contexts is None:
            return None
        return contexts
    
    async def _store_context_history(self, context_data: MarketContext) -> None:
        """
        Store historical context data in the database
        
        Args:
            context_data: Market context data dictionary
        """
        # Implementation depends on your database schema
        # This is a placeholder for the actual database operation
        try:
            self.repository.upsert_market_context(context_data)
        except Exception as e:
            logger.error(f"Failed to store historical context: {e}")