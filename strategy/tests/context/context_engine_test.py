import unittest
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any
from unittest.mock import Mock, MagicMock, patch, AsyncMock

from strategy.context.context_engine import ContextEngine
from strategy.domain.models.market_context import MarketContext
from shared.domain.dto.candle_dto import CandleDto
from shared.constants import CacheKeys, CacheTTL
from shared.cache.cache_service import CacheService


class TestContextEngineUpdateContext(unittest.IsolatedAsyncioTestCase):
    """Test suite for ContextEngine update_context method with integration of swing detection and Fibonacci analysis."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create mock services
        self.cache_service = Mock(spec=CacheService)
        self.database = Mock()
        
        # Configure mock cache service to store and retrieve data
        self.cache_data = {}
        
        def mock_get(key):
            return self.cache_data.get(key)
        
        def mock_set(key, value, expiry=None):
            self.cache_data[key] = value
            return True
        
        self.cache_service.get = Mock(side_effect=mock_get)
        self.cache_service.set = Mock(side_effect=mock_set)
        
        # Create configuration with both analyzers enabled
        self.config = {
            'exchange': 'binance',
            'analyzers': {
                'swing': {'lookback': 5},
                # 'trend': {'lookback': 2},
                'fibbonacci': {'buffer_percent': 0.5}
            }
        }
        
        # Create context engine
        self.context_engine = ContextEngine(
            cache_service=self.cache_service,
            database=self.database,
            config=self.config
        )
        self.mock_store_context_history = AsyncMock()
        self.context_engine._store_context_history = self.mock_store_context_history

        # Create test data
        self.symbol = 'BTCUSDT'
        self.timeframe = '1h'
        self.exchange = 'binance'
        
        # Create initial candles that will establish swing high and swing low
        self.initial_candles = self._create_initial_trend_candles()
        
        # Create new candles for second update
        self.update_candles = self._create_update_candles()
    
    def _create_initial_trend_candles(self) -> List[CandleDto]:
        """Create initial candles that establish clear swing high and swing low."""
        return [
            # Start of trend
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=102.0,
                low=98.0,
                close=101.0,
                volume=1000,
                is_closed=True
            ),
            # Low swing point formation
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=101.0,
                high=103.0,
                low=95.0,  # This will be a swing low
                close=97.0,
                volume=1200,
                is_closed=True
            ),
            # Confirmation candle for swing low
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=97.0,
                high=99.0,
                low=96.0,
                close=98.0,
                volume=1100,
                is_closed=True
            ),
            # Strong upward move
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc),
                open=98.0,
                high=110.0,  # This will be a swing high
                low=97.0,
                close=109.0,
                volume=1500,
                is_closed=True
            ),
            # Confirmation candle for swing high
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
                open=109.0,
                high=109.5,
                low=107.0,
                close=108.0,
                volume=1300,
                is_closed=True
            ),
            # Additional candle
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc),
                open=108.0,
                high=109.0,
                low=105.0,
                close=106.0,
                volume=1400,
                is_closed=True
            )
        ]
    
    def _create_update_candles(self) -> List[CandleDto]:
        """Create new candles for second update that won't change swing points."""
        return [
            # Continue from previous trend
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 6, 0, tzinfo=timezone.utc),
                open=106.0,
                high=107.0,
                low=104.0,
                close=105.0,
                volume=1200,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 7, 0, tzinfo=timezone.utc),
                open=105.0,
                high=106.0,
                low=103.0,
                close=104.0,
                volume=1300,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 8, 0, tzinfo=timezone.utc),
                open=104.0,
                high=105.0,
                low=102.0,
                close=103.0,
                volume=1100,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 9, 0, tzinfo=timezone.utc),
                open=103.0,
                high=104.0,
                low=101.0,
                close=102.0,
                volume=1050,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
                open=102.0,
                high=103.0,
                low=100.0,
                close=101.0,
                volume=1000,
                is_closed=True
            )
        ]
    
    async def test_update_context_full_flow(self):
        """Test the full flow of update_context with swing detection and Fibonacci analysis."""
        # Start the context engine
        await self.context_engine.start()
        
        # First update: establish swing high and swing low
        context_1 = await self.context_engine.update_context(
            self.symbol,
            self.timeframe,
            self.initial_candles,
            self.exchange
        )
        
        # Verify swing points were detected
        self.assertIsNotNone(context_1)
        self.assertIsNotNone(context_1.swing_high)
        self.assertIsNotNone(context_1.swing_low)
        
        # Verify swing high and low values
        self.assertEqual(context_1.swing_high['price'], 110.0)
        self.assertEqual(context_1.swing_low['price'], 95.0)
        
        # Verify Fibonacci levels were calculated
        self.assertIsNotNone(context_1.fib_levels)
        self.assertIn('support', context_1.fib_levels)
        self.assertIn('resistance', context_1.fib_levels)
        self.assertGreater(len(context_1.fib_levels['support']), 0)
        self.assertGreater(len(context_1.fib_levels['resistance']), 0)
        
        # Verify context was cached
        cache_key = self.context_engine._get_context_cache_key(self.symbol, self.timeframe, self.exchange)
        self.cache_service.set.assert_called_with(cache_key, context_1, expiry=CacheTTL.MARKET_STATE)
        
        # Verify some specific Fibonacci levels
        # For uptrend (swing low before swing high), we expect:
        # - Retracements as support levels
        # - Extensions as resistance levels
        support_prices = [level['price'] for level in context_1.fib_levels['support']]
        resistance_prices = [level['price'] for level in context_1.fib_levels['resistance']]
        
        # 0% retracement should be at the high
        self.assertIn(110.0, support_prices)
        # 100% retracement should be at the low
        self.assertIn(95.0, support_prices)
        
        # Extensions should be above the high
        for price in resistance_prices:
            self.assertGreater(price, 110.0)
        
        # Second update: use same swing points but new candles
        context_2 = await self.context_engine.update_context(
            self.symbol,
            self.timeframe,
            self.update_candles,
            self.exchange
        )
        
        # Verify swing points remained the same (since no new highs/lows)
        self.assertIsNotNone(context_2)
        if context_2.swing_high:
            self.assertEqual(context_2.swing_high['price'], 110.0)
        if context_2.swing_low:
            self.assertEqual(context_2.swing_low['price'], 95.0)
        
        # Verify Fibonacci levels are still present and unchanged
        if context_2.fib_levels:
            self.assertEqual(context_2.fib_levels['support'], context_1.fib_levels['support'])
            self.assertEqual(context_2.fib_levels['resistance'], context_1.fib_levels['resistance'])
        
        # Verify current price was updated
        self.assertEqual(context_2.current_price, 101.0)  # Last candle's close

        if any([context_1.swing_high != context_2.swing_high,
                context_1.swing_low != context_2.swing_low,
                context_1.fib_levels != context_2.fib_levels]):
            # If there was an update, verify the old context was stored
            self.mock_store_context_history.assert_called_once_with(context_1)
        else:
            # If no update, verify it wasn't called
            self.mock_store_context_history.assert_not_called()

    
    async def test_update_context_with_new_swing_high(self):
        """Test update_context when new candles create a new swing high."""
        # Start the context engine
        await self.context_engine.start()
        # First update: establish initial swing points
        context_1 = await self.context_engine.update_context(
            self.symbol,
            self.timeframe,
            self.initial_candles,
            self.exchange
        )
        
        # Verify initial swing points are set
        self.assertIsNotNone(context_1)
        self.assertEqual(context_1.swing_high['price'], 110.0)
        self.assertEqual(context_1.swing_low['price'], 95.0)
        
        # Verify context was cached
        context_key = self.context_engine._get_context_cache_key(self.symbol, self.timeframe, self.exchange)
        cached_context1 = self.cache_data.get(context_key)
        self.assertEqual(cached_context1,context_1)

        old_swing_high = context_1.swing_high
        old_swing_low = context_1.swing_low
        old_fibbs_support = context_1.fib_levels.get("support")
        old_fibbs_resistance = context_1.fib_levels.get("resistance")
        # Create candles with new swing high
        new_high_candles = [
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 6, 0, tzinfo=timezone.utc),
                open=106.0,
                high=108.0,
                low=105.0,
                close=107.0,
                volume=1300,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 7, 0, tzinfo=timezone.utc),
                open=107.0,
                high=115.0,  # New swing high
                low=106.0,
                close=114.0,
                volume=2000,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 8, 0, tzinfo=timezone.utc),
                open=114.0,
                high=114.5,
                low=112.0,
                close=113.0,
                volume=1800,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 9, 0, tzinfo=timezone.utc),
                open=113.0,
                high=113.5,
                low=111.0,
                close=112.0,
                volume=1700,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
                open=112.0,
                high=112.5,
                low=110.0,
                close=111.0,
                volume=1600,
                is_closed=True
            )
        ]
        
        # Second update with new swing high
        context_2 = await self.context_engine.update_context(
            self.symbol,
            self.timeframe,
            new_high_candles,
            self.exchange
        )
        
        # Verify context is returned
        self.assertIsNotNone(context_2)
        new_swing_high = context_2.swing_high
        new_swing_low = context_2.swing_low
        new_fibbs_support = context_2.fib_levels.get("support")
        new_fibbs_resistance = context_2.fib_levels.get("resistance")
        # Debug print to see what's happening
        print("===== Debug test with new swing high =====")
        if context_1:
            print(f"Debug - Old Swing High: {cached_context1.swing_high}")
            print(f"Debug - Old Swing Low: {cached_context1.swing_low}")
        if context_2:
            print(f"Debug - New Swing High: {context_2.swing_high}")
            print(f"Debug - New Swing Low: {context_2.swing_low}")
        
        # Verify swing high was updated
        if context_2 and context_2.swing_high:
            self.assertEqual(context_2.swing_high['price'], 115.0)
        
        # Verify swing low remains unchanged
        if context_2 and context_2.swing_low:
            self.assertEqual(context_2.swing_low['price'], 95.0)
        
        # If we have both swing points, check Fibonacci levels
        if context_2 and context_2.fib_levels and 'support' in context_2.fib_levels and 'resistance' in context_2.fib_levels:
            support_prices = [level['price'] for level in context_2.fib_levels['support']]
            resistance_prices = [level['price'] for level in context_2.fib_levels['resistance']]
            
            # 0% retracement should be at the new high
            self.assertIn(115.0, support_prices)
            # 100% retracement should still be at the low
            self.assertIn(95.0, support_prices)
            
            # Extensions should be above the new high
            for price in resistance_prices:
                self.assertGreater(price, 115.0)

        if old_swing_high != new_swing_high or old_swing_low != new_swing_low or old_fibbs_resistance != new_fibbs_resistance or old_fibbs_support != new_fibbs_support:
            # If there was an update, verify the old context was stored
            self.mock_store_context_history.assert_called_once_with(context_1)
        else:
            # If no update, verify it wasn't called
            self.mock_store_context_history.assert_not_called()

    async def test_update_context_with_new_swing_low(self):
        """Test update_context when new candles create a new swing low."""
        # Start the context engine
        await self.context_engine.start()

        # Ensure no existing context
        self.cache_data.clear()
        
        # First update: establish initial swing points
        context_1 = await self.context_engine.update_context(
            self.symbol,
            self.timeframe,
            self.initial_candles,
            self.exchange
        )
        
        # Verify initial swing points are set
        self.assertIsNotNone(context_1)
        self.assertEqual(context_1.swing_high['price'], 110.0)
        self.assertEqual(context_1.swing_low['price'], 95.0)
        
        # Verify context was cached
        context_key = self.context_engine._get_context_cache_key(self.symbol, self.timeframe, self.exchange)
        cached_context1 = self.cache_data.get(context_key)
        self.assertEqual(cached_context1, context_1)

        old_swing_high = context_1.swing_high
        old_swing_low = context_1.swing_low
        old_fibbs_support = context_1.fib_levels.get("support")
        old_fibbs_resistance = context_1.fib_levels.get("resistance")
        # Create candles with new swing low
        new_low_candles = [
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 6, 0, tzinfo=timezone.utc),
                open=106.0,
                high=108.0,
                low=105.0,
                close=107.0,
                volume=1300,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 7, 0, tzinfo=timezone.utc),
                open=107.0,
                high=108.0,
                low=90.0,  # New swing low
                close=92.0,
                volume=2000,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 8, 0, tzinfo=timezone.utc),
                open=92.0,
                high=94.0,
                low=91.0,
                close=93.0,
                volume=1800,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 9, 0, tzinfo=timezone.utc),
                open=93.0,
                high=95.0,
                low=92.0,
                close=94.0,
                volume=1700,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
                open=94.0,
                high=96.0,
                low=93.0,
                close=95.0,
                volume=1600,
                is_closed=True
            )
        ]
        
        # Second update with new swing low
        context_2 = await self.context_engine.update_context(
            self.symbol,
            self.timeframe,
            new_low_candles,
            self.exchange
        )
        
        # Verify context is returned
        self.assertIsNotNone(context_2)
        new_swing_high = context_2.swing_high
        new_swing_low = context_2.swing_low
        new_fibbs_support = context_2.fib_levels.get("support")
        new_fibbs_resistance = context_2.fib_levels.get("resistance")
        # Debug print to see what's happening
        print("===== Debug test with new swing low =====")
        if context_1:
            print(f"Debug - Old Swing High: {old_swing_high}")
            print(f"Debug - Old Swing Low: {old_swing_low}")
            print(f"Debug - Old Fibbs Support: {old_fibbs_support}")
            print(f"Debug - Old Fibbs Resistance: {old_fibbs_resistance}")
        if context_2:
            print(f"Debug - New Swing High: {new_swing_high}")
            print(f"Debug - New Swing Low: {new_swing_low}")
            print(f"Debug - New Fibbs Support: {new_fibbs_support}")
            print(f"Debug - New Fibbs Resistance: {new_fibbs_resistance}")
        
        # Verify swing low was updated
        if context_2 and context_2.swing_low:
            self.assertEqual(context_2.swing_low['price'], 90.0)
        
        # Verify swing high remains unchanged
        if context_2 and context_2.swing_high:
            self.assertEqual(context_2.swing_high['price'], 110.0)
        
        # If we have both swing points, check Fibonacci levels
        if context_2 and context_2.fib_levels and 'support' in context_2.fib_levels and 'resistance' in context_2.fib_levels:
            support_prices = [level['price'] for level in context_2.fib_levels['support']]
            resistance_prices = [level['price'] for level in context_2.fib_levels['resistance']]
            
            self.assertIn(90.0, resistance_prices)  # 0% retracement (at the low)
            self.assertIn(110.0, resistance_prices)  # 100% retracement (at the high)
            
            # Check that extension levels (supports) are below the low
            for price in support_prices:
                self.assertLess(price, 90.0)
            
            # Check that all resistance prices are between or equal to the low and high
            for price in resistance_prices:
                self.assertGreaterEqual(price, 90.0)
                self.assertLessEqual(price, 110.0)

            if old_swing_high != new_swing_high or old_swing_low != new_swing_low or old_fibbs_resistance != new_fibbs_resistance or old_fibbs_support != new_fibbs_support:
                # If there was an update, verify the old context was stored
                self.mock_store_context_history.assert_called_once_with(context_1)
            else:
                # If no update, verify it wasn't called
                self.mock_store_context_history.assert_not_called()

    
    async def test_update_context_first_time(self):
        """Test update_context when no existing context is present."""
        # Start the context engine
        await self.context_engine.start()
        
        # Ensure no existing context
        self.cache_data.clear()
        
        # First update should create initial context
        context = await self.context_engine.update_context(
            self.symbol,
            self.timeframe,
            self.initial_candles,
            self.exchange
        )
        
        # Verify context was created
        self.assertIsNotNone(context)
        
        # Verify all components were populated
        self.assertIsNotNone(context.swing_high)
        self.assertIsNotNone(context.swing_low)
        self.assertIsNotNone(context.fib_levels)
        self.assertEqual(context.symbol, self.symbol)
        self.assertEqual(context.timeframe, self.timeframe)
        self.assertEqual(context.exchange, self.exchange)
        
        # Verify context was cached
        self.context_engine._get_context_cache_key(self.symbol, self.timeframe, self.exchange)
        self.assertTrue(self.cache_service.set.called)
        
        # Verify that _store_context_history was NOT called for first-time context
        self.mock_store_context_history.assert_not_called() 

    
    async def test_update_context_incomplete_data(self):
        """Test update_context when the resulting context is incomplete."""
        # Start the context engine
        await self.context_engine.start()
        
        # Create candles that won't form clear swing points
        insufficient_candles = [
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.0,
                volume=1000,
                is_closed=True
            ),
            CandleDto(
                symbol=self.symbol,
                exchange=self.exchange,
                timeframe=self.timeframe,
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.0,
                volume=1000,
                is_closed=True
            )
        ]
        
        # Update with insufficient data
        context = await self.context_engine.update_context(
            self.symbol,
            self.timeframe,
            insufficient_candles,
            self.exchange
        )
        
        # Verify no context was returned since it's incomplete
        self.assertIsNone(context)
        self.mock_store_context_history.assert_not_called()         


if __name__ == '__main__':
    unittest.main()