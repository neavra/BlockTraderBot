import unittest
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

from strategy.context.analyzers.fibbonacci_analyzer import FibonacciAnalyzer
from strategy.domain.models.market_context import MarketContext
from shared.domain.dto.candle_dto import CandleDto

class TestFibonacciAnalyzer(unittest.TestCase):
    """Test suite for FibonacciAnalyzer class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create analyzer with default parameters
        self.analyzer = FibonacciAnalyzer()
        
        # Create analyzer with custom parameters
        self.custom_analyzer = FibonacciAnalyzer(buffer_percent=1.0)
        
        # Create a market context
        self.context = MarketContext(
            symbol='BTCUSDT',
            timeframe='1h',
            exchange='binance'
        )
        
        # Create sample candles
        self.candles = [
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=105.0,
                low=95.0,
                close=102.0,
                volume=1000,
                is_closed=True
            ),
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=102.0,
                high=110.0,
                low=98.0,
                close=103.0,
                volume=1200,
                is_closed=True
            ),
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=103.0,
                high=115.0,
                low=92.0,
                close=108.0,
                volume=1500,
                is_closed=True
            )
        ]
        
        # Create sample swing points for uptrend
        self.uptrend_swing_high = {
            'price': 115.0,
            'index': 2,
            'timestamp': datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc).isoformat(),
            'strength': 0.8
        }
        
        self.uptrend_swing_low = {
            'price': 92.0,
            'index': 2,
            'timestamp': datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc).isoformat(),
            'strength': 0.7
        }
        
        # Create sample swing points for downtrend
        self.downtrend_swing_high = {
            'price': 110.0,
            'index': 1,
            'timestamp': datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc).isoformat(),
            'strength': 0.75
        }
        
        self.downtrend_swing_low = {
            'price': 92.0,
            'index': 2,
            'timestamp': datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc).isoformat(),
            'strength': 0.85
        }
    
    def test_initialization(self):
        """Test initialization with default and custom parameters."""
        # Test default parameters
        self.assertEqual(self.analyzer.retracement_levels, [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0])
        self.assertEqual(self.analyzer.extension_levels, [1.272, 1.618, 2.0, 2.618])
        self.assertEqual(self.analyzer.buffer, 0.005)  # 0.5%
        
        # Test custom parameters
        self.assertEqual(self.custom_analyzer.buffer, 0.01)  # 1.0%
    
    def test_analyze_uptrend(self):
        """Test calculation of Fibonacci levels in an uptrend."""
        # Calculate levels for uptrend
        levels = self.analyzer.analyze(115.0, 92.0, uptrend=True)
        
        # Check that both support and resistance levels were calculated
        self.assertIn('support', levels)
        self.assertIn('resistance', levels)
        
        # Check that we have the expected number of levels
        self.assertEqual(len(levels['support']), 7)  # 7 retracement levels
        self.assertEqual(len(levels['resistance']), 4)  # 4 extension levels
        
        # Verify some specific levels
        # 0% retracement should be at the high price
        self.assertEqual(levels['support'][0]['price'], 115.0)
        self.assertEqual(levels['support'][0]['level'], 0.0)
        self.assertEqual(levels['support'][0]['type'], 'retracement')
        
        # 100% retracement should be at the low price
        self.assertEqual(levels['support'][6]['price'], 92.0)
        self.assertEqual(levels['support'][6]['level'], 1.0)
        self.assertEqual(levels['support'][6]['type'], 'retracement')
        
        # Check a sample extension level (1.618)
        for level in levels['resistance']:
            if level['level'] == 1.618:
                self.assertAlmostEqual(level['price'], 115.0 + (115.0 - 92.0) * 1.618, places=1)
                self.assertEqual(level['type'], 'extension')
                break
        else:
            self.fail("1.618 extension level not found")
    
    def test_analyze_downtrend(self):
        """Test calculation of Fibonacci levels in a downtrend."""
        # Calculate levels for downtrend
        levels = self.analyzer.analyze(110.0, 92.0, uptrend=False)
        
        # Check that both support and resistance levels were calculated
        self.assertIn('support', levels)
        self.assertIn('resistance', levels)
        
        # Check that we have the expected number of levels
        self.assertEqual(len(levels['resistance']), 7)  # 7 retracement levels
        self.assertEqual(len(levels['support']), 4)  # 4 extension levels
        
        # Verify some specific levels
        # 0% retracement should be at the low price
        self.assertEqual(levels['resistance'][0]['price'], 92.0)
        self.assertEqual(levels['resistance'][0]['level'], 0.0)
        self.assertEqual(levels['resistance'][0]['type'], 'retracement')
        
        # 100% retracement should be at the high price
        self.assertEqual(levels['resistance'][6]['price'], 110.0)
        self.assertEqual(levels['resistance'][6]['level'], 1.0)
        self.assertEqual(levels['resistance'][6]['type'], 'retracement')
        
        # Check a sample extension level (1.618)
        for level in levels['support']:
            if level['level'] == 1.618:
                self.assertAlmostEqual(level['price'], 92.0 - (110.0 - 92.0) * 1.618, places=1)
                self.assertEqual(level['type'], 'extension')
                break
        else:
            self.fail("1.618 extension level not found")
    
    def test_update_context_with_swing_points(self):
        """Test updating context with Fibonacci levels using swing points."""
        # Set up context with swing points for uptrend
        context = self.context
        context.swing_high = self.uptrend_swing_high
        context.swing_low = self.uptrend_swing_low
        
        # Update context
        updated_context, updated = self.analyzer.update_market_context(context, self.candles)
        
        # Check that context was updated
        self.assertTrue(updated)
        self.assertIsNotNone(updated_context.fib_levels)
        self.assertIn('support', updated_context.fib_levels)
        self.assertIn('resistance', updated_context.fib_levels)
        self.assertGreater(len(updated_context.fib_levels['support']), 0)
        self.assertGreater(len(updated_context.fib_levels['resistance']), 0)
        
        # Test with missing swing points
        context = MarketContext(
            symbol='BTCUSDT',
            timeframe='1h',
            exchange='binance'
        )
        # No swing high/low set
        
        # Update context
        updated_context, updated = self.analyzer.update_market_context(context, self.candles)
        
        # Check that context was not updated
        self.assertFalse(updated)
        
        # Test with invalid swing points
        context = MarketContext(
            symbol='BTCUSDT',
            timeframe='1h',
            exchange='binance'
        )
        context.swing_high = {'invalid': 'data'}
        context.swing_low = {'invalid': 'data'}
        
        # Update context
        updated_context, updated = self.analyzer.update_market_context(context, self.candles)
        
        # Check that context was not updated
        self.assertFalse(updated)
    
    def test_support_and_resistance_sorting(self):
        """Test that support and resistance levels are correctly sorted."""
        # Calculate levels for uptrend
        levels = self.analyzer.analyze(115.0, 92.0, uptrend=True)
        
        # Check that support levels are sorted in descending order (highest first)
        for i in range(len(levels['support']) - 1):
            self.assertGreaterEqual(levels['support'][i]['price'], levels['support'][i+1]['price'])
        
        # Check that resistance levels are sorted in ascending order (lowest first)
        for i in range(len(levels['resistance']) - 1):
            self.assertLessEqual(levels['resistance'][i]['price'], levels['resistance'][i+1]['price'])
    
    def test_level_ordering(self):
        """Test that the Fibonacci levels have the correct relative ordering."""
        # Calculate levels for uptrend
        levels = self.analyzer.analyze(115.0, 92.0, uptrend=True)
        
        # Extract prices from support levels
        support_prices = [level['price'] for level in levels['support']]
        
        # For uptrend, support prices should be in this order:
        # high price (0%) > 23.6% > 38.2% > 50% > 61.8% > 78.6% > low price (100%)
        self.assertEqual(support_prices[0], 115.0)  # 0%
        self.assertEqual(support_prices[-1], 92.0)  # 100%
        
        # Check that the prices decrease monotonically
        self.assertTrue(all(support_prices[i] > support_prices[i+1] for i in range(len(support_prices)-1)))
        
        # Check that extension levels are beyond the swing points
        for level in levels['resistance']:
            self.assertGreater(level['price'], 115.0)  # All extension levels should be higher than the high

    def test_uptrend_vs_downtrend_logic(self):
        """Test the logic for determining uptrend vs downtrend in the analyzer."""
        # Test uptrend - high > low
        uptrend_levels = self.analyzer.analyze(115.0, 92.0, uptrend=True)
        
        # Verify uptrend has correct structure
        self.assertEqual(len(uptrend_levels['support']), 7)  # Retracements as support
        self.assertEqual(len(uptrend_levels['resistance']), 4)  # Extensions as resistance
        
        # Test downtrend - high < low  
        downtrend_levels = self.analyzer.analyze(115.0, 92.0, uptrend=False)
        
        # Verify downtrend has correct structure
        self.assertEqual(len(downtrend_levels['resistance']), 7)  # Retracements as resistance
        self.assertEqual(len(downtrend_levels['support']), 4)  # Extensions as support


if __name__ == '__main__':
    unittest.main()