from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging
from .types import TrendDirection, TimeframeCategory
from .market_context import MarketContext
from shared.constants import CacheKeys, CacheTTL

logger = logging.getLogger(__name__)


class MarketStructure:
    """Manages market contexts and coordinates analyzers"""

    def __init__(self, params: Dict[str, Any] = None, cache_service = None):
        self.contexts = {}  # Dict[str, MarketContext]
        self.params = params or {}
        self.cache_service = cache_service

        # Initialize analyzers using factory
        from .analyzers.factory import AnalyzerFactory
        analyzer_config = self.params.get('analyzers', {})
        self.analyzer_factory = AnalyzerFactory(analyzer_config)

        # Create default analyzers
        self.analyzers = self.analyzer_factory.create_default_analyzers()

        # Get specific analyzers for convenience
        self.swing_detector = self.analyzers.get('swing')
        self.trend_analyzer = self.analyzers.get('trend')
        self.range_detector = self.analyzers.get('range')

    def set_analyzers(self, swing_detector=None, trend_analyzer=None, range_detector=None):
        """Configure analyzers manually"""
        if swing_detector:
            self.swing_detector = swing_detector
            self.analyzers['swing'] = swing_detector

        if trend_analyzer:
            self.trend_analyzer = trend_analyzer
            self.analyzers['trend'] = trend_analyzer

        if range_detector:
            self.range_detector = range_detector
            self.analyzers['range'] = range_detector

    def get_context(self, symbol: str, timeframe: str, exchange: str = None) -> Optional[MarketContext]:
        """Get context for specific symbol/timeframe

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            timeframe: Candle timeframe (e.g., '1h', '15m')
            exchange: Exchange identifier (e.g., 'binance')

        Returns:
            MarketContext object or None if not found
        """
        # Use default exchange if not provided
        exchange = exchange or self.params.get('exchange', 'default')

        key = f"{exchange}_{symbol}_{timeframe}"
        return self.contexts.get(key)

    def get_contexts_by_category(self, symbol: str, category: Union[TimeframeCategory, str], exchange: str = None) -> List[MarketContext]:
        """Get all contexts for a timeframe category

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            category: Timeframe category (TimeframeCategory enum or string value)
            exchange: Exchange identifier (e.g., 'binance')

        Returns:
            List of MarketContext objects matching the criteria
        """
        # Use default exchange if not provided
        exchange = exchange or self.params.get('exchange', 'default')

        # Convert string category to enum if needed
        if isinstance(category, str):
            # Try to find the enum by value
            category_enum = None
            for tf_category in TimeframeCategory:
                if tf_category.value == category:
                    category_enum = tf_category
                    break

            # If not found, try to find by name
            if category_enum is None:
                try:
                    category_enum = TimeframeCategory[category.upper()]
                except KeyError:
                    logger.warning(f"Unknown timeframe category: {category}")
                    return []

            category = category_enum

        return [
            ctx for ctx in self.contexts.values()
            if ctx.symbol == symbol and ctx.exchange == exchange and ctx.timeframe_category == category
        ]

    def is_trend_aligned(self, symbol: str, timeframes: List[str], exchange: str = None) -> bool:
        """Check if trends align across timeframes

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            timeframes: List of timeframes to check
            exchange: Exchange identifier (e.g., 'binance')

        Returns:
            True if all trends are the same and not unknown
        """
        # Use default exchange if not provided
        exchange = exchange or self.params.get('exchange', 'default')

        trends = set()
        for tf in timeframes:
            ctx = self.get_context(symbol, tf, exchange)
            if ctx:
                trends.add(ctx.trend)

        # Aligned if all trends are the same and not unknown
        return len(trends) == 1 and TrendDirection.UNKNOWN.value not in trends

    def update_context(self, symbol: str, timeframe: str, candles: List[Dict[str, Any]],
                      exchange: str = None) -> Optional[MarketContext]:
        """Update or create market context

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            timeframe: Candle timeframe (e.g., '1h', '15m')
            candles: List of candle data
            exchange: Exchange identifier (e.g., 'binance')

        Returns:
            Updated MarketContext object or None if no candles provided
        """
        if not candles:
            logger.warning(f"No candles provided for {symbol} {timeframe}")
            return None

        # Use default exchange if not provided
        exchange = exchange or self.params.get('exchange', 'default')

        # Get or create context
        key = f"{exchange}_{symbol}_{timeframe}"
        logger.debug(f"Key: {key}")
        context = self.contexts.get(key)
        logger.debug(f"Context: {context}")
        if not context:
            # Try to load from cache first
            if self.cache_service:
                cache_key = CacheKeys.MARKET_STATE.format(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe
                )
                cached_context = self.cache_service.get(cache_key)
                if cached_context:
                    logger.debug(f"Loaded market context from cache for {exchange} {symbol} {timeframe}")
                    context = MarketContext.from_dict(cached_context)

            # Create new if not found in cache
            if not context:
                context = MarketContext(symbol, timeframe, exchange)

            self.contexts[key] = context

        # Update basic info
        context.timestamp = datetime.now().isoformat()
        context.set_current_price(candles[-1].get('close'))

        # Update using analyzers
        for analyzer_type, analyzer in self.analyzers.items():
            if analyzer:
                try:
                    context = analyzer.update_market_context(context, candles)
                except Exception as e:
                    logger.error(f"Error updating context with {analyzer_type} analyzer: {e}")

        context.last_updated = datetime.now().timestamp()

        # Save to cache if available
        if self.cache_service:
            cache_key = CacheKeys.MARKET_STATE.format(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe
            )
            self.cache_service.set(
                cache_key,
                context.to_dict(),
                expiry=CacheTTL.MARKET_STATE
            )

        return context

    def is_near_level(self, context: MarketContext, price: float, level_type: str = 'all', tolerance: float = 0.005) -> bool:
        """
        Check if a price is near any Fibonacci level

        Args:
            context: MarketContext object
            price: Price to check
            level_type: Type of level to check ('support', 'resistance', or 'all')
            tolerance: Price tolerance as percentage

        Returns:
            True if price is near a level, False otherwise
        """
        if not context or not context.fib_levels:
            return False

        levels_to_check = []

        if level_type == 'support' or level_type == 'all':
            levels_to_check.extend(context.fib_levels.get('support', []))

        if level_type == 'resistance' or level_type == 'all':
            levels_to_check.extend(context.fib_levels.get('resistance', []))

        # Check each level
        for level in levels_to_check:
            level_price = level.get('price')
            if level_price:
                # Calculate tolerance range
                tolerance_range = level_price * tolerance

                # Check if price is within tolerance range
                if abs(price - level_price) <= tolerance_range:
                    return True

        return False

    def detect_order_blocks(self, context: MarketContext, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect potential order blocks based on market structure

        An order block is often formed after a swing point is created

        Args:
            context: MarketContext object
            candles: List of candle dictionaries

        Returns:
            List of potential order blocks
        """
        if not context or not candles:
            return []

        # Check for swing highs and lows
        swing_high = context.swing_high
        swing_low = context.swing_low

        order_blocks = []

        # Look for demand order blocks (support)
        if swing_low and context.trend == TrendDirection.UP.value:
            # Find candle that created the swing low
            swing_index = swing_low.index
            if 0 <= swing_index < len(candles):
                # Order block is typically formed by the candle before the swing
                ob_index = max(0, swing_index - 1)

                order_blocks.append({
                    'type': 'demand',  # Buying pressure/support
                    'price_high': candles[ob_index].get('open', candles[ob_index].get('high', 0)),
                    'price_low': candles[ob_index].get('close', candles[ob_index].get('low', 0)),
                    'index': ob_index,
                    'timestamp': candles[ob_index].get('timestamp', ''),
                    'related_swing': self._swing_point_to_dict(swing_low)
                })

        # Look for supply order blocks (resistance)
        if swing_high and context.trend == TrendDirection.DOWN.value:
            # Find candle that created the swing high
            swing_index = swing_high.index
            if 0 <= swing_index < len(candles):
                # Order block is typically formed by the candle before the swing
                ob_index = max(0, swing_index - 1)

                order_blocks.append({
                    'type': 'supply',  # Selling pressure/resistance
                    'price_high': candles[ob_index].get('open', candles[ob_index].get('high', 0)),
                    'price_low': candles[ob_index].get('close', candles[ob_index].get('low', 0)),
                    'index': ob_index,
                    'timestamp': candles[ob_index].get('timestamp', ''),
                    'related_swing': self._swing_point_to_dict(swing_high)
                })

        return order_blocks

    def _swing_point_to_dict(self, point) -> Optional[Dict[str, Any]]:
        """Convert swing point to dictionary"""
        if point is None:
            return None
        return {
            'price': point.get('price', 0),
            'timestamp': point.get('timestamp', ''),
            'index': point.get('index', 0),
            'strength': point.get('strength', 0)
        }
