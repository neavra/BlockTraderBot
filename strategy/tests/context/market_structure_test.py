import unittest
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List

from strategy.domain.models.market_context import MarketContext
from strategy.context.market_structure import MarketStructure
from strategy.context.analyzers.swing_detector import SwingDetector
from strategy.context.analyzers.trend_analyzer import TrendAnalyzer
from strategy.context.analyzers.range_detector import RangeDetector
from strategy.domain.types.trend_direction_enum import TrendDirectionEnum, TimeframeCategoryEnum

class MockCacheService:
    """Mock cache service for testing"""

    def __init__(self):
        self.cache = {}

    def get(self, key: str) -> Any:
        """Get a value from the cache"""
        return self.cache.get(key)

    def set(self, key: str, value: Any, expiry: int = None) -> bool:
        """Set a value in the cache"""
        self.cache[key] = value
        return True

    def delete(self, key: str) -> bool:
        """Delete a value from the cache"""
        if key in self.cache:
            del self.cache[key]
            return True
        return False

    def keys(self, pattern: str) -> List[str]:
        """Find keys matching a pattern"""
        # Simple pattern matching for testing
        if pattern.endswith('*'):
            prefix = pattern[:-1]
            return [k for k in self.cache.keys() if k.startswith(prefix)]
        return [k for k in self.cache.keys() if k == pattern]

class MarketStructureTest(unittest.TestCase):
    """Test cases for MarketStructure class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create mock cache service
        self.cache_service = MockCacheService()

        # Create market structure with cache service
        self.market_structure = MarketStructure(
            params={
                'analyzers': {
                    'swing': {'lookback': 3, 'min_strength': 0.2},
                    'trend': {'lookback': 2},
                    'range': {'min_touches': 2, 'min_range_size': 0.3}
                }
            },
            cache_service=self.cache_service
        )

        # Create sample candles for testing
        self.candles = [
            {
                'timestamp': datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc).isoformat(),
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 102.0,
                'volume': 1000
            },
            {
                'timestamp': datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc).isoformat(),
                'open': 102.0,
                'high': 110.0,
                'low': 100.0,
                'close': 108.0,
                'volume': 1200
            },
            {
                'timestamp': datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc).isoformat(),
                'open': 108.0,
                'high': 115.0,
                'low': 105.0,
                'close': 112.0,
                'volume': 1500
            },
            {
                'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc).isoformat(),
                'open': 112.0,
                'high': 120.0,
                'low': 110.0,
                'close': 118.0,
                'volume': 1800
            },
            {
                'timestamp': datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc).isoformat(),
                'open': 118.0,
                'high': 122.0,
                'low': 115.0,
                'close': 120.0,
                'volume': 1600
            },
            {
                'timestamp': datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc).isoformat(),
                'open': 120.0,
                'high': 125.0,
                'low': 118.0,
                'close': 122.0,
                'volume': 1400
            },
            {
                'timestamp': datetime(2023, 1, 1, 6, 0, tzinfo=timezone.utc).isoformat(),
                'open': 122.0,
                'high': 128.0,
                'low': 120.0,
                'close': 125.0,
                'volume': 1700
            },
            {
                'timestamp': datetime(2023, 1, 1, 7, 0, tzinfo=timezone.utc).isoformat(),
                'open': 125.0,
                'high': 130.0,
                'low': 123.0,
                'close': 128.0,
                'volume': 1900
            }
        ]

    def test_initialization(self):
        """Test that MarketStructure initializes correctly"""
        # Check that analyzers were created
        self.assertIsNotNone(self.market_structure.swing_detector)
        self.assertIsNotNone(self.market_structure.trend_analyzer)
        self.assertIsNotNone(self.market_structure.range_detector)

        # Check that analyzers dictionary is populated
        self.assertIn('swing', self.market_structure.analyzers)
        self.assertIn('trend', self.market_structure.analyzers)
        self.assertIn('range', self.market_structure.analyzers)

        # Check that cache service is set
        self.assertEqual(self.market_structure.cache_service, self.cache_service)

    def test_update_context(self):
        """Test updating market context with candle data"""
        # Update context with candles
        context = self.market_structure.update_context(
            symbol='BTCUSDT',
            timeframe='1h',
            candles=self.candles,
            exchange='binance'
        )

        # Check that context was created
        self.assertIsNotNone(context)
        self.assertEqual(context.symbol, 'BTCUSDT')
        self.assertEqual(context.timeframe, '1h')
        self.assertEqual(context.exchange, 'binance')

        # Check that context was stored in market structure
        stored_context = self.market_structure.get_context('BTCUSDT', '1h', 'binance')
        self.assertEqual(stored_context, context)

        # Check that context was cached
        cached_data = self.cache_service.get('market:binance:BTCUSDT:1h:state')
        self.assertIsNotNone(cached_data)

        # Check that basic info was updated
        self.assertEqual(context.current_price, self.candles[-1]['close'])
        self.assertIsNotNone(context.timestamp)

    def test_get_context(self):
        """Test retrieving market context"""
        # Create a context
        self.market_structure.update_context(
            symbol='BTCUSDT',
            timeframe='1h',
            candles=self.candles,
            exchange='binance'
        )

        # Get the context
        context = self.market_structure.get_context('BTCUSDT', '1h', 'binance')

        # Check that context was retrieved
        self.assertIsNotNone(context)
        self.assertEqual(context.symbol, 'BTCUSDT')
        self.assertEqual(context.timeframe, '1h')
        self.assertEqual(context.exchange, 'binance')

        # Try to get a non-existent context
        context = self.market_structure.get_context('ETHUSDT', '1h', 'binance')
        self.assertIsNone(context)

    def test_get_contexts_by_category(self):
        """Test retrieving contexts by timeframe category"""
        # Create contexts for different timeframes
        # Clear existing contexts first
        self.market_structure.contexts = {}

        # Create new contexts
        context_1h = self.market_structure.update_context('BTCUSDT', '1h', self.candles, 'binance')
        context_4h = self.market_structure.update_context('BTCUSDT', '4h', self.candles, 'binance')
        context_1d = self.market_structure.update_context('BTCUSDT', '1d', self.candles, 'binance')
        context_15m = self.market_structure.update_context('BTCUSDT', '15m', self.candles, 'binance')
        context_5m = self.market_structure.update_context('BTCUSDT', '5m', self.candles, 'binance')

        # Collect contexts
        contexts = [context_1h, context_4h, context_1d, context_15m, context_5m]

        # Verify that contexts were created with correct categories
        htf_contexts = [ctx for ctx in contexts if ctx.timeframe in ['1h', '4h', '1d']]
        mtf_contexts = [ctx for ctx in contexts if ctx.timeframe in ['15m', '30m']]
        ltf_contexts = [ctx for ctx in contexts if ctx.timeframe in ['1m', '5m']]

        # Print contexts for debugging
        print(f"Created {len(contexts)} contexts:")
        for ctx in contexts:
            print(f"  {ctx.symbol} {ctx.timeframe} {ctx.timeframe_category}")

        # Check that contexts were created with correct categories
        for ctx in htf_contexts:
            self.assertEqual(ctx.timeframe_category, TimeframeCategoryEnum.HTF)

        for ctx in mtf_contexts:
            self.assertEqual(ctx.timeframe_category, TimeframeCategoryEnum.MTF)

        for ctx in ltf_contexts:
            self.assertEqual(ctx.timeframe_category, TimeframeCategoryEnum.LTF)

        # Check that we have the right number of contexts in each category
        self.assertEqual(len(htf_contexts), 3)  # 1h, 4h, 1d
        self.assertEqual(len(mtf_contexts), 1)  # 15m
        self.assertEqual(len(ltf_contexts), 1)  # 5m

        # Now test the get_contexts_by_category method
        retrieved_htf = self.market_structure.get_contexts_by_category('BTCUSDT', TimeframeCategoryEnum.HTF, 'binance')
        retrieved_mtf = self.market_structure.get_contexts_by_category('BTCUSDT', TimeframeCategoryEnum.MTF, 'binance')
        retrieved_ltf = self.market_structure.get_contexts_by_category('BTCUSDT', TimeframeCategoryEnum.LTF, 'binance')

        # Check that the right number of contexts were retrieved
        self.assertEqual(len(retrieved_htf), 3)  # 1h, 4h, 1d
        self.assertEqual(len(retrieved_mtf), 1)  # 15m
        self.assertEqual(len(retrieved_ltf), 1)  # 5m

    def test_is_trend_aligned(self):
        """Test checking if trends are aligned across timeframes"""
        # Create contexts with different trends
        context1 = MarketContext('BTCUSDT', '1h', 'binance')
        context1.set_trend(TrendDirectionEnum.UP.value)

        context2 = MarketContext('BTCUSDT', '4h', 'binance')
        context2.set_trend(TrendDirectionEnum.UP.value)

        context3 = MarketContext('BTCUSDT', '1d', 'binance')
        context3.set_trend(TrendDirectionEnum.DOWN.value)

        # Add contexts to market structure
        self.market_structure.contexts['binance_BTCUSDT_1h'] = context1
        self.market_structure.contexts['binance_BTCUSDT_4h'] = context2
        self.market_structure.contexts['binance_BTCUSDT_1d'] = context3

        # Check if trends are aligned
        aligned = self.market_structure.is_trend_aligned('BTCUSDT', ['1h', '4h'], 'binance')
        self.assertTrue(aligned)

        aligned = self.market_structure.is_trend_aligned('BTCUSDT', ['1h', '4h', '1d'], 'binance')
        self.assertFalse(aligned)

    def test_cache_integration(self):
        """Test integration with cache service"""
        # Update context with candles
        context = self.market_structure.update_context(
            symbol='BTCUSDT',
            timeframe='1h',
            candles=self.candles,
            exchange='binance'
        )

        # Check that context was cached
        cached_data = self.cache_service.get('market:binance:BTCUSDT:1h:state')
        self.assertIsNotNone(cached_data)

        # Clear the contexts
        self.market_structure.contexts = {}

        # Update context again - should load from cache
        context = self.market_structure.update_context(
            symbol='BTCUSDT',
            timeframe='1h',
            candles=self.candles,
            exchange='binance'
        )

        # Check that context was loaded from cache
        self.assertEqual(context.symbol, 'BTCUSDT')
        self.assertEqual(context.timeframe, '1h')
        self.assertEqual(context.exchange, 'binance')

if __name__ == '__main__':
    unittest.main()
