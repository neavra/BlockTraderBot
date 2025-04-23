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

logger = logging.getLogger(__name__)

TIMEFRAME_HIERARCHY: Dict[str, List[str]] = {
        "1m": ["1m", "5m", "15m"],
        "5m": ["5m", "15m", "1h"],
        "15m": ["15m", "1h", "4h"],
        "30m": ["30m", "1h", "4h"],
        "1h": ["1h", "4h", "1d"],
        "2h": ["2h", "4h", "1d"],
        "4h": ["4h", "1d", "1w"],
        "1d": ["1d", "1w"],
        "1w": ["1w", "1M"],
        "1M": ["1M"]
    }

class ContextEngine:
    """
    Main engine for market context analysis and maintenance.
    Analyzes candle data to create and update market contexts across timeframes.
    Uses cache for storing contexts instead of in-memory dictionary.
    """
    
    def __init__(
        self, 
        cache_service: CacheService,
        database_service: Database,
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
        self.database_service = database_service
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
    
    async def get_context(self, symbol: str, timeframe: str, exchange: str = None) -> Optional[MarketContext]:
        """
        Get context for specific symbol/timeframe from cache
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            timeframe: Candle timeframe (e.g., '1h', '15m')
            exchange: Exchange identifier (e.g., 'binance')
            
        Returns:
            MarketContext object or None if not found
        """
        # Use default exchange if not provided
        exchange = exchange or self.config.get('exchange', 'default')
        
        # Get from cache
        cache_key = self._get_context_cache_key(symbol, timeframe, exchange)
        cached_context = self.cache_service.get(cache_key)
        
        if cached_context:
            # Convert from cached data to MarketContext object
            return MarketContext.from_dict(cached_context)
        
        return None
    
    async def get_contexts_by_category(self, symbol: str, category: Union[TimeframeCategoryEnum, str], exchange: str = None) -> List[MarketContext]:
        """
        Get all contexts for a timeframe category from cache
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            category: Timeframe category (TimeframeCategoryEnum enum or string value)
            exchange: Exchange identifier (e.g., 'binance')
            
        Returns:
            List of MarketContext objects matching the criteria
        """
        # Use default exchange if not provided
        exchange = exchange or self.config.get('exchange', 'default')
        
        # Convert string category to enum if needed
        if isinstance(category, str):
            try:
                category = TimeframeCategoryEnum(category)
            except ValueError:
                # Try to match by value instead
                for enum_val in TimeframeCategoryEnum:
                    if enum_val.value == category:
                        category = enum_val
                        break
        
        # Get all timeframes for this category
        category_timeframes = []
        for tf, cat in TimeframeEnum._CATEGORIES.items():
            if cat == category:
                category_timeframes.append(tf)
        
        # Retrieve contexts for each matching timeframe
        contexts = []
        for tf in category_timeframes:
            context = await self.get_context(symbol, tf, exchange)
            if context:
                contexts.append(context)
        
        return contexts
    
    async def update_context(self, symbol: str, timeframe: str, candles: List[CandleDto], exchange: str = None) -> Optional[MarketContext]:
        if not candles:
            logger.warning(f"No candles provided for {symbol} {timeframe}")
            return None

        cache_key = self._get_context_cache_key(symbol, timeframe, exchange)
        existing_context = self.cache_service.get(cache_key)
        is_first_time = existing_context is None

        context = MarketContext(symbol, timeframe, exchange)
        context.timestamp = datetime.now().isoformat()
        context.set_current_price(candles[-1].close)
        updatedFlag = False
        for analyzer_type, analyzer in self.analyzers.items():
            if analyzer:
                try:
                    context, updated = analyzer.update_market_context(context, candles)
                    if updated:
                        updatedFlag = True
                except Exception as e:
                    logger.error(f"Error updating context with {analyzer_type} analyzer: {e}")

        context.last_updated = datetime.now().timestamp()

        # need to check the context properly here
        if not context.is_complete():
            logger.info(f"Market Context is incomplete {context}")
            return None
        # If this is the first market context created, add it to the cache
        # Else, add the old one to the repository and set the new one to the cache
        if is_first_time:
            self.cache_service.set(cache_key, context, expiry=CacheTTL.MARKET_STATE)
        elif updatedFlag:
            try:
                # TODO Implement Storing to repository
                await self._store_context_history(existing_context)
            except Exception as e:
                logger.error(f"Error storing historical context: {e}")

        return context
    
    async def get_multi_timeframe_contexts(
        self, symbol: str, base_timeframe: str, exchange: str = "default"
    ) -> Optional[List[MarketContext]]:
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
            context = self.cache_service.get(cache_key)

            if not context:
                logger.debug(f"Missing context for {symbol} {tf} on {exchange}")
                return None

            contexts.append(context)

        return contexts
    
    async def _store_context_history(self, context_data: Dict[str, Any]) -> None:
        """
        Store historical context data in the database
        
        Args:
            context_data: Market context data dictionary
        """
        # Implementation depends on your database schema
        # This is a placeholder for the actual database operation
        try:
            # Convert to proper format for database
            # Add timestamp for historical tracking
            historical_context = {
                "context_data": context_data,
                "archived_at": datetime.now().isoformat(),
                "symbol": context_data.get("symbol"),
                "timeframe": context_data.get("timeframe"),
                "exchange": context_data.get("exchange")
            }
            
            # Store in database using appropriate repository
            # await self.database_service.context_repository.create(historical_context)
            pass  # Placeholder until database integration is implemented
            
        except Exception as e:
            logger.error(f"Failed to store historical context: {e}")
            # Don't re-raise - this is a non-critical operation