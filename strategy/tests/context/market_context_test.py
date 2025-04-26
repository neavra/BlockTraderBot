import unittest
from datetime import datetime, timezone
from strategy.domain.models.market_context import MarketContext
from strategy.domain.types.trend_direction_enum import TrendDirectionEnum

class MarketContextTest(unittest.TestCase):
    """Test cases for MarketContext class"""
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create a market context
        self.context = MarketContext(
            symbol='BTCUSDT',
            timeframe='1h',
            exchange='binance'
        )
        
        # Create sample swing points
        self.swing_high = {
            'price': 50000.0,
            'index': 10,
            'timestamp': datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc).isoformat(),
            'strength': 0.02
        }
        
        self.swing_low = {
            'price': 48000.0,
            'index': 15,
            'timestamp': datetime(2023, 1, 1, 15, 0, tzinfo=timezone.utc).isoformat(),
            'strength': 0.03
        }
    
    def test_initialization(self):
        """Test that MarketContext initializes correctly"""
        self.assertEqual(self.context.symbol, 'BTCUSDT')
        self.assertEqual(self.context.timeframe, '1h')
        self.assertEqual(self.context.exchange, 'binance')
        self.assertEqual(self.context.trend, TrendDirectionEnum.UNKNOWN.value)
        self.assertIsNone(self.context.current_price)
        self.assertIsNone(self.context.swing_high)
        self.assertIsNone(self.context.swing_low)
        self.assertEqual(self.context.swing_high_history, [])
        self.assertEqual(self.context.swing_low_history, [])
        self.assertFalse(self.context.is_in_range)
    
    def test_set_current_price(self):
        """Test setting current price"""
        self.context.set_current_price(49000.0)
        self.assertEqual(self.context.current_price, 49000.0)
    
    def test_set_swing_high(self):
        """Test setting swing high"""
        self.context.set_swing_high(self.swing_high)
        
        # Check that swing high was set
        self.assertEqual(self.context.swing_high, self.swing_high)
        
        # Check that swing high was added to history
        self.assertEqual(len(self.context.swing_high_history), 1)
        self.assertEqual(self.context.swing_high_history[0], self.swing_high)
        
        # Check that setting the same swing high doesn't duplicate it
        self.context.set_swing_high(self.swing_high)
        self.assertEqual(len(self.context.swing_high_history), 1)
        
        # Check that setting a different swing high adds it to history
        new_swing_high = self.swing_high.copy()
        new_swing_high['index'] = 20
        self.context.set_swing_high(new_swing_high)
        self.assertEqual(len(self.context.swing_high_history), 2)
        self.assertEqual(self.context.swing_high_history[0], new_swing_high)
    
    def test_set_swing_low(self):
        """Test setting swing low"""
        self.context.set_swing_low(self.swing_low)
        
        # Check that swing low was set
        self.assertEqual(self.context.swing_low, self.swing_low)
        
        # Check that swing low was added to history
        self.assertEqual(len(self.context.swing_low_history), 1)
        self.assertEqual(self.context.swing_low_history[0], self.swing_low)
    
    def test_set_trend(self):
        """Test setting trend"""
        self.context.set_trend(TrendDirectionEnum.UP.value)
        self.assertEqual(self.context.trend, TrendDirectionEnum.UP.value)
        
        self.context.set_trend(TrendDirectionEnum.DOWN.value)
        self.assertEqual(self.context.trend, TrendDirectionEnum.DOWN.value)
    
    def test_set_range(self):
        """Test setting range"""
        self.context.set_range(
            high=50000.0,
            low=48000.0,
            equilibrium=49000.0,
            strength=0.7,
            timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc).isoformat()
        )
        
        # Check that range was set
        self.assertEqual(self.context.range_high, 50000.0)
        self.assertEqual(self.context.range_low, 48000.0)
        self.assertEqual(self.context.range_equilibrium, 49000.0)
        self.assertEqual(self.context.range_strength, 0.7)
        self.assertTrue(self.context.is_in_range)
        
        # Check that range size was calculated
        self.assertAlmostEqual(self.context.range_size, 0.04166, places=4)  # (50000 - 48000) / 48000
    
    def test_clear_range(self):
        """Test clearing range"""
        # Set a range first
        self.context.set_range(50000.0, 48000.0, 49000.0)
        
        # Clear the range
        self.context.clear_range()
        
        # Check that range was cleared
        self.assertIsNone(self.context.range_high)
        self.assertIsNone(self.context.range_low)
        self.assertIsNone(self.context.range_equilibrium)
        self.assertFalse(self.context.is_in_range)
    
    def test_check_if_in_range(self):
        """Test checking if price is in range"""
        # Set a range
        self.context.set_range(50000.0, 48000.0, 49000.0)
        
        # Check prices within range
        self.assertTrue(self.context.check_if_in_range(49000.0))
        self.assertTrue(self.context.check_if_in_range(48000.0))
        self.assertTrue(self.context.check_if_in_range(50000.0))
        
        # Check prices outside range
        self.assertFalse(self.context.check_if_in_range(47000.0))
        self.assertFalse(self.context.check_if_in_range(51000.0))
        
        # Check with tolerance
        self.assertTrue(self.context.check_if_in_range(47800.0, tolerance=0.01))  # 48000 * 0.99 = 47520
        self.assertTrue(self.context.check_if_in_range(50200.0, tolerance=0.01))  # 50000 * 1.01 = 50500
    
    def test_to_dict(self):
        """Test converting to dictionary"""
        # Set some values
        self.context.set_current_price(49000.0)
        self.context.set_swing_high(self.swing_high)
        self.context.set_swing_low(self.swing_low)
        self.context.set_trend(TrendDirectionEnum.UP.value)
        self.context.set_range(50000.0, 48000.0, 49000.0)
        
        # Convert to dictionary
        data = self.context.to_dict()
        
        # Check that all fields were included
        self.assertEqual(data['symbol'], 'BTCUSDT')
        self.assertEqual(data['timeframe'], '1h')
        self.assertEqual(data['exchange'], 'binance')
        self.assertEqual(data['current_price'], 49000.0)
        self.assertEqual(data['swing_high'], self.swing_high)
        self.assertEqual(data['swing_low'], self.swing_low)
        self.assertEqual(data['trend'], TrendDirectionEnum.UP.value)
        self.assertEqual(data['range_high'], 50000.0)
        self.assertEqual(data['range_low'], 48000.0)
        self.assertEqual(data['range_equilibrium'], 49000.0)
        self.assertTrue(data['is_in_range'])
    
    def test_from_dict(self):
        """Test creating from dictionary"""
        # Create a dictionary
        data = {
            'symbol': 'ETHUSDT',
            'timeframe': '4h',
            'exchange': 'binance',
            'current_price': 3000.0,
            'swing_high': self.swing_high,
            'swing_low': self.swing_low,
            'trend': TrendDirectionEnum.DOWN.value,
            'range_high': 3200.0,
            'range_low': 2800.0,
            'range_equilibrium': 3000.0,
            'is_in_range': True,
            'range_size': 0.1428,
            'range_strength': 0.8,
            'range_detected_at': datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc).isoformat(),
            'last_updated': datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc).timestamp()
        }
        
        # Create from dictionary
        context = MarketContext.from_dict(data)
        
        # Check that all fields were set
        self.assertEqual(context.symbol, 'ETHUSDT')
        self.assertEqual(context.timeframe, '4h')
        self.assertEqual(context.exchange, 'binance')
        self.assertEqual(context.current_price, 3000.0)
        self.assertEqual(context.swing_high, self.swing_high)
        self.assertEqual(context.swing_low, self.swing_low)
        self.assertEqual(context.trend, TrendDirectionEnum.DOWN.value)
        self.assertEqual(context.range_high, 3200.0)
        self.assertEqual(context.range_low, 2800.0)
        self.assertEqual(context.range_equilibrium, 3000.0)
        self.assertTrue(context.is_in_range)
        self.assertEqual(context.range_size, 0.1428)
        self.assertEqual(context.range_strength, 0.8)

if __name__ == '__main__':
    unittest.main()
