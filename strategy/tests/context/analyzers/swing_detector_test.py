import unittest
import asyncio
from datetime import datetime, timezone
from typing import List

from strategy.context.analyzers.swing_detector import SwingDetector
from strategy.domain.models.market_context import MarketContext
from shared.domain.dto.candle_dto import CandleDto

class TestSwingDetector(unittest.IsolatedAsyncioTestCase):
    """Test suite for SwingDetector class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create swing detector with default parameters
        self.detector = SwingDetector(lookback=5)
        
        # Create a market context
        self.context = MarketContext(
            symbol='BTCUSDT',
            timeframe='1h',
            exchange='binance'
        )
        
        # Create test candles for different scenarios
        self.uptrend_candles = self._create_uptrend_candles()
        self.downtrend_candles = self._create_downtrend_candles()
        self.range_candles = self._create_range_candles()
        self.insufficient_candles = self._create_candles(2)  # Less than lookback
    
    def _create_candles(self, count: int) -> List[CandleDto]:
        """Create a specific number of basic candles."""
        return [
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, i, 0, tzinfo=timezone.utc),
                open=100.0,
                high=105.0,
                low=95.0,
                close=100.0,
                volume=1000,
                is_closed=True
            )
            for i in range(count)
        ]
    
    def _create_uptrend_candles(self) -> List[CandleDto]:
        """Create candles showing an uptrend with clear swing highs and lows."""
        return [
            # Base candles forming the trend
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=105.0,
                low=98.0,
                close=103.0,
                volume=1000,
                is_closed=True
            ),
            # Lower low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=103.0,
                high=104.0,
                low=96.0,  # This will be a swing low (confirmed by next candle)
                close=101.0,
                volume=1200,
                is_closed=True
            ),
            # Confirming candle for the swing low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=101.0,
                high=106.0,
                low=98.0,
                close=105.0,
                volume=1300,
                is_closed=True
            ),
            # Higher high
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc),
                open=105.0,
                high=110.0,  # This will be a swing high (confirmed by next candle)
                low=104.0,
                close=108.0,
                volume=1400,
                is_closed=True
            ),
            # Confirming candle for the swing high
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
                open=108.0,
                high=109.0,
                low=105.0,
                close=107.0,
                volume=1500,
                is_closed=True
            ),
            # Continuing the trend
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc),
                open=107.0,
                high=112.0,
                low=106.0,
                close=111.0,
                volume=1600,
                is_closed=True
            ),
            # Another potential swing high
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 6, 0, tzinfo=timezone.utc),
                open=111.0,
                high=115.0,  # This will be the new swing high (highest of all)
                low=110.0,
                close=114.0,
                volume=1700,
                is_closed=True
            ),
            # Confirming candle for the new swing high
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 7, 0, tzinfo=timezone.utc),
                open=114.0,
                high=114.5,
                low=112.0,
                close=113.0,
                volume=1800,
                is_closed=True
            )
        ]
    
    def _create_downtrend_candles(self) -> List[CandleDto]:
        """Create candles showing a downtrend with clear swing highs and lows."""
        return [
            # Base candles forming the trend
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=120.0,
                high=122.0,
                low=119.0,
                close=121.0,
                volume=1000,
                is_closed=True
            ),
            # Higher high
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=121.0,
                high=125.0,  # This will be a swing high (confirmed by next candle)
                low=120.0,
                close=122.0,
                volume=1200,
                is_closed=True
            ),
            # Confirming candle for the swing high
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=122.0,
                high=123.0,
                low=117.0,
                close=118.0,
                volume=1300,
                is_closed=True
            ),
            # Lower low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc),
                open=118.0,
                high=119.0,
                low=115.0,  # This will be a swing low (confirmed by next candle)
                close=116.0,
                volume=1400,
                is_closed=True
            ),
            # Confirming candle for the swing low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
                open=116.0,
                high=118.0,
                low=115.5,
                close=117.0,
                volume=1500,
                is_closed=True
            ),
            # Continuing the trend
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc),
                open=117.0,
                high=117.5,
                low=112.0,
                close=113.0,
                volume=1600,
                is_closed=True
            ),
            # Another potential swing low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 6, 0, tzinfo=timezone.utc),
                open=113.0,
                high=113.5,
                low=110.0,  # This will be the new swing low (lowest of all)
                close=111.0,
                volume=1700,
                is_closed=True
            ),
            # Confirming candle for the new swing low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 7, 0, tzinfo=timezone.utc),
                open=111.0,
                high=112.0,
                low=110.5,
                close=111.5,
                volume=1800,
                is_closed=True
            )
        ]
    
    def _create_range_candles(self) -> List[CandleDto]:
        """Create candles showing a range-bound market with multiple swing points."""
        return [
            # Start of range
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=105.0,
                low=98.0,
                close=103.0,
                volume=1000,
                is_closed=True
            ),
            # Bottom of range
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=103.0,
                high=104.0,
                low=95.0,  # This will be a swing low
                close=97.0,
                volume=1200,
                is_closed=True
            ),
            # Confirming candle for the swing low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=97.0,
                high=101.0,
                low=96.0,
                close=100.0,
                volume=1300,
                is_closed=True
            ),
            # Top of range
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc),
                open=100.0,
                high=106.0,  # This will be a swing high
                low=99.0,
                close=105.0,
                volume=1400,
                is_closed=True
            ),
            # Confirming candle for the swing high
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
                open=105.0,
                high=105.5,
                low=103.0,
                close=104.0,
                volume=1500,
                is_closed=True
            ),
            # Back to bottom of range
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc),
                open=104.0,
                high=104.0,
                low=97.0,
                close=98.0,
                volume=1600,
                is_closed=True
            ),
            # Potential new swing low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 6, 0, tzinfo=timezone.utc),
                open=98.0,
                high=99.0,
                low=96.0,  # Another swing low but not the lowest
                close=97.0,
                volume=1700,
                is_closed=True
            ),
            # Confirming candle for the new swing low
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 7, 0, tzinfo=timezone.utc),
                open=97.0,
                high=100.0,
                low=97.0,
                close=99.0,
                volume=1800,
                is_closed=True
            )
        ]
    
    async def test_insufficient_candles(self):
        """Test behavior with insufficient candles (less than lookback)."""
        # Analyze with insufficient candles
        result = self.detector.analyze(self.insufficient_candles)
        
        # Check that no swing points are detected
        self.assertIsNone(result["swing_high"])
        self.assertIsNone(result["swing_low"])
    
    async def test_uptrend_swing_detection(self):
        """Test detection of swing points in an uptrend."""
        # Analyze uptrend candles
        result = self.detector.analyze(self.uptrend_candles)
        
        # Check that swing high was detected
        self.assertIsNotNone(result["swing_high"])
        high = result["swing_high"]
        self.assertIsNotNone(high["price"])
        self.assertIsNotNone(high["index"])
        self.assertIsNotNone(high["timestamp"])
        
        # Check swing high value - should be the highest high in the series (115.0)
        self.assertEqual(high["price"], 115.0)
        
        # Check that swing low was detected
        self.assertIsNotNone(result["swing_low"])
        low = result["swing_low"]
        self.assertIsNotNone(low["price"])
        self.assertIsNotNone(low["index"])
        self.assertIsNotNone(low["timestamp"])
        
        # Check swing low value - should be the lowest low in the series (96.0)
        self.assertEqual(low["price"], 96.0)
    
    async def test_downtrend_swing_detection(self):
        """Test detection of swing points in a downtrend."""
        # Analyze downtrend candles
        result = self.detector.analyze(self.downtrend_candles)
        
        # Check that swing high was detected
        self.assertIsNotNone(result["swing_high"])
        high = result["swing_high"]
        
        # Check swing high value - should be the highest high in the series (125.0)
        self.assertEqual(high["price"], 125.0)
        
        # Check that swing low was detected
        self.assertIsNotNone(result["swing_low"])
        low = result["swing_low"]
        
        # Check swing low value - should be the lowest low in the series (110.0)
        self.assertEqual(low["price"], 110.0)
    
    async def test_range_swing_detection(self):
        """Test detection of swing points in a range-bound market."""
        # Analyze range candles
        result = self.detector.analyze(self.range_candles)
        
        # Check that swing high was detected
        self.assertIsNotNone(result["swing_high"])
        high = result["swing_high"]
        
        # Check swing high value - should be the highest high in the series (106.0)
        self.assertEqual(high["price"], 106.0)
        
        # Check that swing low was detected
        self.assertIsNotNone(result["swing_low"])
        low = result["swing_low"]
        
        # Check swing low value - should be the lowest low in the series (95.0)
        self.assertEqual(low["price"], 95.0)
    
    async def test_update_market_context_new_swings(self):
        """Test updating market context with new swing points."""
        # Update context with uptrend candles
        updated_context, updated = self.detector.update_market_context(self.context, self.uptrend_candles)
        
        # Should be updated
        self.assertTrue(updated)
        
        # Check that swing high was updated
        self.assertIsNotNone(updated_context.swing_high)
        high = updated_context.swing_high
        self.assertEqual(high["price"], 115.0)
        
        # Check that swing low was updated
        self.assertIsNotNone(updated_context.swing_low)
        low = updated_context.swing_low
        self.assertEqual(low["price"], 96.0)
    
    async def test_update_market_context_with_existing_swings(self):
        """Test updating market context when it already has swing points."""
        # First set some existing swing points
        existing_high = {
            "price": 120.0,  # Higher than any in our test candles
            "index": 0,
            "timestamp": datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
        }
        existing_low = {
            "price": 90.0,  # Lower than any in our test candles
            "index": 0,
            "timestamp": datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
        }
        
        self.context.set_swing_high(existing_high)
        self.context.set_swing_low(existing_low)
        
        # Update context with downtrend candles
        updated_context, updated = self.detector.update_market_context(self.context, self.downtrend_candles)
        
        # Should be updated for the highest swing (125.0 > 120.0)
        self.assertTrue(updated)
        
        # Check that swing high was updated (125.0 > 120.0)
        self.assertEqual(updated_context.swing_high["price"], 125.0)
        
        # Check that swing low was NOT updated (90.0 < 110.0)
        self.assertEqual(updated_context.swing_low["price"], 90.0)
        
    
    async def test_update_market_context_same_swings(self):
        """Test updating market context with the same data twice."""
        # First update
        updated_context, updated = self.detector.update_market_context(self.context, self.uptrend_candles)
        self.assertTrue(updated)
        
        # Second update with same data
        second_update, second_updated = self.detector.update_market_context(updated_context, self.uptrend_candles)
        
        # Should not be updated (same swings)
        self.assertFalse(second_updated)
        
        # Check that swing points are the same
        self.assertEqual(second_update.swing_high["price"], updated_context.swing_high["price"])
        self.assertEqual(second_update.swing_low["price"], updated_context.swing_low["price"])
        
if __name__ == '__main__':
    unittest.main()