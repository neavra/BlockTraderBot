import unittest
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List

# Import the StructureBreakIndicator class
from strategy.indicators.bos import StructureBreakIndicator
from shared.domain.dto.candle_dto import CandleDto

class TestStructureBreakIndicator(unittest.TestCase):
    """Test suite for StructureBreakIndicator class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create indicator with default parameters
        self.indicator = StructureBreakIndicator()
        
        # Create indicator with custom parameters
        self.custom_indicator = StructureBreakIndicator(params={
            'lookback_period': 5,           # Smaller lookback
            'confirmation_candles': 2,      # More confirmations required
            'min_break_percentage': 0.001   # Higher threshold (0.1%)
        })
        
        # Create market context with swing points
        self.market_context = {
            'swing_high': {
                'price': 105.0,
                'index': 3,
                'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc).isoformat()
            },
            'swing_low': {
                'price': 95.0,
                'index': 5,
                'timestamp': datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc).isoformat()
            },
            'current_price': 100.0
        }
        
        # Create test candles for various BOS scenarios
        
        # Higher High (HH) scenario - breaks above swing high
        self.hh_candles = [self._create_candle(i, data) for i, data in enumerate([
            {'o': 100, 'h': 103, 'l': 98, 'c': 102, 'dt': datetime(2023, 1, 1, 0, 0)},
            {'o': 102, 'h': 104, 'l': 100, 'c': 103, 'dt': datetime(2023, 1, 1, 1, 0)},
            {'o': 103, 'h': 104.5, 'l': 101, 'c': 104, 'dt': datetime(2023, 1, 1, 2, 0)},
            {'o': 104, 'h': 105, 'l': 102, 'c': 103, 'dt': datetime(2023, 1, 1, 3, 0)},  # This is the swing high
            {'o': 103, 'h': 104, 'l': 100, 'c': 101, 'dt': datetime(2023, 1, 1, 4, 0)},
            {'o': 101, 'h': 102, 'l': 95, 'c': 97, 'dt': datetime(2023, 1, 1, 5, 0)},    # This is the swing low
            {'o': 97, 'h': 101, 'l': 96, 'c': 100, 'dt': datetime(2023, 1, 1, 6, 0)},
            {'o': 100, 'h': 104, 'l': 99, 'c': 102, 'dt': datetime(2023, 1, 1, 7, 0)},
            {'o': 102, 'h': 106, 'l': 101, 'c': 105, 'dt': datetime(2023, 1, 1, 8, 0)},  # BOS - Higher High
            {'o': 105, 'h': 107, 'l': 103, 'c': 106, 'dt': datetime(2023, 1, 1, 9, 0)},  # Confirmation
        ])]
        
        # Lower Low (LL) scenario - breaks below swing low
        self.ll_candles = [self._create_candle(i, data) for i, data in enumerate([
            {'o': 100, 'h': 103, 'l': 98, 'c': 102, 'dt': datetime(2023, 1, 1, 0, 0)},
            {'o': 102, 'h': 104, 'l': 100, 'c': 103, 'dt': datetime(2023, 1, 1, 1, 0)},
            {'o': 103, 'h': 104.5, 'l': 101, 'c': 104, 'dt': datetime(2023, 1, 1, 2, 0)},
            {'o': 104, 'h': 105, 'l': 102, 'c': 103, 'dt': datetime(2023, 1, 1, 3, 0)},  # This is the swing high
            {'o': 103, 'h': 104, 'l': 100, 'c': 101, 'dt': datetime(2023, 1, 1, 4, 0)},
            {'o': 101, 'h': 102, 'l': 95, 'c': 97, 'dt': datetime(2023, 1, 1, 5, 0)},    # This is the swing low
            {'o': 97, 'h': 99, 'l': 96, 'c': 98, 'dt': datetime(2023, 1, 1, 6, 0)},
            {'o': 98, 'h': 100, 'l': 94, 'c': 96, 'dt': datetime(2023, 1, 1, 7, 0)},
            {'o': 96, 'h': 97, 'l': 93, 'c': 94, 'dt': datetime(2023, 1, 1, 8, 0)},      # BOS - Lower Low
            {'o': 94, 'h': 95, 'l': 92, 'c': 93, 'dt': datetime(2023, 1, 1, 9, 0)},      # Confirmation
        ])]
        
        # Higher Low (HL) scenario - creates a low higher than swing low
        self.hl_candles = [self._create_candle(i, data) for i, data in enumerate([
            {'o': 100, 'h': 103, 'l': 98, 'c': 102, 'dt': datetime(2023, 1, 1, 0, 0)},
            {'o': 102, 'h': 104, 'l': 100, 'c': 103, 'dt': datetime(2023, 1, 1, 1, 0)},
            {'o': 103, 'h': 104.5, 'l': 101, 'c': 104, 'dt': datetime(2023, 1, 1, 2, 0)},
            {'o': 104, 'h': 105, 'l': 102, 'c': 103, 'dt': datetime(2023, 1, 1, 3, 0)},  # This is the swing high
            {'o': 103, 'h': 104, 'l': 100, 'c': 101, 'dt': datetime(2023, 1, 1, 4, 0)},
            {'o': 101, 'h': 102, 'l': 95, 'c': 97, 'dt': datetime(2023, 1, 1, 5, 0)},    # This is the swing low
            {'o': 97, 'h': 101, 'l': 96, 'c': 98, 'dt': datetime(2023, 1, 1, 6, 0)},
            {'o': 98, 'h': 102, 'l': 97, 'c': 99, 'dt': datetime(2023, 1, 1, 7, 0)},
            {'o': 99, 'h': 103, 'l': 98, 'c': 101, 'dt': datetime(2023, 1, 1, 8, 0)},    # Higher Low (HL) relative to swing low
            {'o': 101, 'h': 104, 'l': 99, 'c': 102, 'dt': datetime(2023, 1, 1, 9, 0)},
        ])]
        
        # Lower High (LH) scenario - creates a high lower than swing high
        self.lh_candles = [self._create_candle(i, data) for i, data in enumerate([
            {'o': 100, 'h': 103, 'l': 98, 'c': 102, 'dt': datetime(2023, 1, 1, 0, 0)},
            {'o': 102, 'h': 104, 'l': 100, 'c': 103, 'dt': datetime(2023, 1, 1, 1, 0)},
            {'o': 103, 'h': 104.5, 'l': 101, 'c': 104, 'dt': datetime(2023, 1, 1, 2, 0)},
            {'o': 104, 'h': 105, 'l': 102, 'c': 103, 'dt': datetime(2023, 1, 1, 3, 0)},  # This is the swing high
            {'o': 103, 'h': 104, 'l': 100, 'c': 101, 'dt': datetime(2023, 1, 1, 4, 0)},
            {'o': 101, 'h': 102, 'l': 95, 'c': 97, 'dt': datetime(2023, 1, 1, 5, 0)},    # This is the swing low
            {'o': 97, 'h': 99, 'l': 94, 'c': 96, 'dt': datetime(2023, 1, 1, 6, 0)},
            {'o': 96, 'h': 101, 'l': 95, 'c': 98, 'dt': datetime(2023, 1, 1, 7, 0)},
            {'o': 98, 'h': 103, 'l': 97, 'c': 101, 'dt': datetime(2023, 1, 1, 8, 0)},    # Lower High (LH) relative to swing high
            {'o': 101, 'h': 104, 'l': 100, 'c': 103, 'dt': datetime(2023, 1, 1, 9, 0)},
        ])]
        
        # No BOS scenario - price stays within swing high/low range
        self.no_bos_candles = [self._create_candle(i, data) for i, data in enumerate([
            {'o': 100, 'h': 103, 'l': 98, 'c': 102, 'dt': datetime(2023, 1, 1, 0, 0)},
            {'o': 102, 'h': 104, 'l': 100, 'c': 103, 'dt': datetime(2023, 1, 1, 1, 0)},
            {'o': 103, 'h': 104.5, 'l': 101, 'c': 104, 'dt': datetime(2023, 1, 1, 2, 0)},
            {'o': 104, 'h': 105, 'l': 102, 'c': 103, 'dt': datetime(2023, 1, 1, 3, 0)},  # This is the swing high
            {'o': 103, 'h': 104, 'l': 100, 'c': 101, 'dt': datetime(2023, 1, 1, 4, 0)},
            {'o': 101, 'h': 102, 'l': 95, 'c': 97, 'dt': datetime(2023, 1, 1, 5, 0)},    # This is the swing low
            {'o': 97, 'h': 100, 'l': 96, 'c': 98, 'dt': datetime(2023, 1, 1, 6, 0)},
            {'o': 98, 'h': 102, 'l': 97, 'c': 100, 'dt': datetime(2023, 1, 1, 7, 0)},
            {'o': 100, 'h': 103, 'l': 98, 'c': 101, 'dt': datetime(2023, 1, 1, 8, 0)},   # No BOS, within range
            {'o': 101, 'h': 104, 'l': 96, 'c': 102, 'dt': datetime(2023, 1, 1, 9, 0)},   # No BOS, within range
        ])]
        
        # Convert all candle lists to dictionaries for testing
        self.hh_candle_dicts = [self._candle_to_dict(candle) for candle in self.hh_candles]
        self.ll_candle_dicts = [self._candle_to_dict(candle) for candle in self.ll_candles]
        self.hl_candle_dicts = [self._candle_to_dict(candle) for candle in self.hl_candles]
        self.lh_candle_dicts = [self._candle_to_dict(candle) for candle in self.lh_candles]
        self.no_bos_candle_dicts = [self._candle_to_dict(candle) for candle in self.no_bos_candles]
    
    def _create_candle(self, index: int, data: Dict[str, Any]) -> CandleDto:
        """Create a CandleDto object with given parameters."""
        return CandleDto(
            symbol="BTCUSDT",
            exchange="binance",
            timeframe="1h",
            timestamp=data['dt'].replace(tzinfo=timezone.utc),
            open=data['o'],
            high=data['h'],
            low=data['l'],
            close=data['c'],
            volume=1000 + index * 100,  # Just a dummy value that increases with index
            is_closed=True
        )
    
    def _candle_to_dict(self, candle: CandleDto) -> Dict[str, Any]:
        """Convert a CandleDto to a dictionary for the indicator."""
        return {
            'open': candle.open,
            'high': candle.high,
            'low': candle.low,
            'close': candle.close,
            'volume': candle.volume,
            'timestamp': candle.timestamp
        }
    
    def test_initialization(self):
        """Test initialization with default and custom parameters."""
        # Test default parameters
        self.assertEqual(self.indicator.params['lookback_period'], 10)
        self.assertEqual(self.indicator.params['confirmation_candles'], 1)
        self.assertEqual(self.indicator.params['min_break_percentage'], 0.0005)
        
        # Test custom parameters
        self.assertEqual(self.custom_indicator.params['lookback_period'], 5)
        self.assertEqual(self.custom_indicator.params['confirmation_candles'], 2)
        self.assertEqual(self.custom_indicator.params['min_break_percentage'], 0.001)
    
    def test_empty_candles(self):
        """Test behavior with empty or insufficient candles."""
        async def run_test():
            # Test with empty candles
            result = await self.indicator.calculate({
                'candles': [],
                'market_context': self.market_context
            })
            self.assertEqual(len(result['breaks']), 0)
            self.assertFalse(result['has_bullish_break'])
            self.assertFalse(result['has_bearish_break'])
            self.assertIsNone(result['latest_break'])
            
            # Test with insufficient candles (less than 3)
            result = await self.indicator.calculate({
                'candles': self.hh_candle_dicts[:2],
                'market_context': self.market_context
            })
            self.assertEqual(len(result['breaks']), 0)
            self.assertFalse(result['has_bullish_break'])
            self.assertFalse(result['has_bearish_break'])
            self.assertIsNone(result['latest_break'])
            
        asyncio.run(run_test())
    
    def test_missing_market_context(self):
        """Test behavior with missing market context."""
        async def run_test():
            # Test with no market context
            result = await self.indicator.calculate({
                'candles': self.hh_candle_dicts
            })
            self.assertEqual(len(result['breaks']), 0)
            self.assertFalse(result['has_bullish_break'])
            self.assertFalse(result['has_bearish_break'])
            self.assertIsNone(result['latest_break'])
            
            # Test with empty market context
            result = await self.indicator.calculate({
                'candles': self.hh_candle_dicts,
                'market_context': {}
            })
            self.assertEqual(len(result['breaks']), 0)
            self.assertFalse(result['has_bullish_break'])
            self.assertFalse(result['has_bearish_break'])
            self.assertIsNone(result['latest_break'])
            
            # Test with invalid swing points
            result = await self.indicator.calculate({
                'candles': self.hh_candle_dicts,
                'market_context': {'swing_high': {}, 'swing_low': {}}
            })
            self.assertEqual(len(result['breaks']), 0)
            self.assertFalse(result['has_bullish_break'])
            self.assertFalse(result['has_bearish_break'])
            self.assertIsNone(result['latest_break'])
            
        asyncio.run(run_test())
    
    def test_higher_high_detection(self):
        """Test detection of Higher High (HH) breakouts."""
        async def run_test():
            # Calculate with Higher High scenario
            result = await self.indicator.calculate({
                'candles': self.hh_candle_dicts,
                'market_context': self.market_context
            })
            
            # Verify a bullish break was detected
            self.assertTrue(result['has_bullish_break'])
            
            # Find all higher high breaks in the results
            higher_highs = [b for b in result['breaks'] if b['break_type'] == 'higher_high']
            
            # There should be at least one higher high
            self.assertGreaterEqual(len(higher_highs), 1)
            
            # Verify details of the most recent higher high
            latest_hh = higher_highs[0]
            self.assertEqual(latest_hh['break_type'], 'higher_high')
            self.assertGreater(latest_hh['break_value'], 0)  # Break value should be positive
            self.assertGreater(latest_hh['break_percentage'], 0)  # Break percentage should be positive
            self.assertEqual(latest_hh['swing_reference'], 105.0)  # Reference to swing high
            
            # Check that timestamp was preserved
            self.assertIn('timestamp', latest_hh)
            
            # Verify candle data is included
            self.assertIn('candle', latest_hh)
            
        asyncio.run(run_test())
    
    def test_lower_low_detection(self):
        """Test detection of Lower Low (LL) breakouts."""
        async def run_test():
            # Calculate with Lower Low scenario
            result = await self.indicator.calculate({
                'candles': self.ll_candle_dicts,
                'market_context': self.market_context
            })
            
            # Verify a bearish break was detected
            self.assertTrue(result['has_bearish_break'])
            
            # Find all lower low breaks in the results
            lower_lows = [b for b in result['breaks'] if b['break_type'] == 'lower_low']
            
            # There should be at least one lower low
            self.assertGreaterEqual(len(lower_lows), 1)
            
            # Verify details of the most recent lower low
            latest_ll = lower_lows[0]
            self.assertEqual(latest_ll['break_type'], 'lower_low')
            self.assertGreater(latest_ll['break_value'], 0)  # Break value should be positive
            self.assertGreater(latest_ll['break_percentage'], 0)  # Break percentage should be positive
            self.assertEqual(latest_ll['swing_reference'], 95.0)  # Reference to swing low
            
            # Check that timestamp was preserved
            self.assertIn('timestamp', latest_ll)
            
            # Verify candle data is included
            self.assertIn('candle', latest_ll)
            
        asyncio.run(run_test())
    
    def test_higher_low_detection(self):
        """Test detection of Higher Lows (HL)."""
        async def run_test():
            # Calculate with Higher Low scenario
            result = await self.indicator.calculate({
                'candles': self.hl_candle_dicts,
                'market_context': self.market_context
            })
            
            # Verify a bullish structural element was detected
            self.assertTrue(result['has_bullish_break'])
            
            # Find all higher low breaks in the results
            higher_lows = [b for b in result['breaks'] if b['break_type'] == 'higher_low']
            
            # There should be at least one higher low
            self.assertGreaterEqual(len(higher_lows), 1)
            
            # Verify details of the most recent higher low
            latest_hl = higher_lows[0]
            self.assertEqual(latest_hl['break_type'], 'higher_low')
            self.assertGreater(latest_hl['break_value'], 0)  # Break value should be positive
            self.assertGreater(latest_hl['break_percentage'], 0)  # Break percentage should be positive
            self.assertEqual(latest_hl['swing_reference'], 95.0)  # Reference to swing low
            
        asyncio.run(run_test())
    
    def test_lower_high_detection(self):
        """Test detection of Lower Highs (LH)."""
        async def run_test():
            # Calculate with Lower High scenario
            result = await self.indicator.calculate({
                'candles': self.lh_candle_dicts,
                'market_context': self.market_context
            })
            
            # Verify a bearish structural element was detected
            self.assertTrue(result['has_bearish_break'])
            
            # Find all lower high breaks in the results
            lower_highs = [b for b in result['breaks'] if b['break_type'] == 'lower_high']
            
            # There should be at least one lower high
            self.assertGreaterEqual(len(lower_highs), 1)
            
            # Verify details of the most recent lower high
            latest_lh = lower_highs[0]
            self.assertEqual(latest_lh['break_type'], 'lower_high')
            self.assertGreater(latest_lh['break_value'], 0)  # Break value should be positive
            self.assertGreater(latest_lh['break_percentage'], 0)  # Break percentage should be positive
            self.assertEqual(latest_lh['swing_reference'], 105.0)  # Reference to swing high
            
        asyncio.run(run_test())
    
    def test_no_bos_detection(self):
        """Test behavior when no BOS events are present."""
        async def run_test():
            # Calculate with No BOS scenario
            result = await self.indicator.calculate({
                'candles': self.no_bos_candle_dicts,
                'market_context': self.market_context
            })
            
            # There should be no bullish/bearish breaks if price remains within range
            # Check if there are any higher_high or lower_low types
            higher_highs = [b for b in result['breaks'] if b['break_type'] == 'higher_high']
            lower_lows = [b for b in result['breaks'] if b['break_type'] == 'lower_low']
            
            self.assertEqual(len(higher_highs), 0)
            self.assertEqual(len(lower_lows), 0)
            
            # There may be higher_low or lower_high detected even without true BOS
            # So we specifically check for absence of actual breaks
            
        asyncio.run(run_test())
    
    def test_confirmation_requirement(self):
        """Test behavior with different confirmation requirements."""
        async def run_test():
            # Let's create a more controlled test case specifically for confirmations
            # Create candles where there's a clear breakout followed by confirmations
            breakout_candles = [
                {'open': 100, 'high': 102, 'low': 98, 'close': 101, 'timestamp': datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)},
                {'open': 101, 'high': 103, 'low': 99, 'close': 102, 'timestamp': datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc)},
                # Candle that breaks above swing high (105.0)
                {'open': 102, 'high': 106, 'low': 101, 'close': 105, 'timestamp': datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc)},
                # First confirmation candle
                {'open': 105, 'high': 107, 'low': 104, 'close': 106, 'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc)},
                # Second confirmation candle
                {'open': 106, 'high': 108, 'low': 105, 'close': 107, 'timestamp': datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc)}
            ]
            
            # Test with 1 confirmation (default indicator)
            one_confirmation = breakout_candles[:4]  # Up to and including first confirmation
            default_result = await self.indicator.calculate({
                'candles': one_confirmation,
                'market_context': self.market_context
            })
            
            # Find higher high breaks in the result
            default_hh = [b for b in default_result['breaks'] if b['break_type'] == 'higher_high']
            
            # Default indicator (1 confirmation) should detect the break
            self.assertGreaterEqual(len(default_hh), 1)
            if len(default_hh) > 0:
                self.assertEqual(default_hh[0]['candle']['high'], 106)  # The breakout candle's high
            
            # With custom indicator (2 confirmations needed)
            custom_result = await self.custom_indicator.calculate({
                'candles': one_confirmation,
                'market_context': self.market_context
            })
            
            # Find higher high breaks
            custom_hh = [b for b in custom_result['breaks'] if b['break_type'] == 'higher_high']
            
            # Custom indicator should not detect (needs 2 confirmations)
            self.assertEqual(len(custom_hh), 0)
            
            # Now test with two confirmations available
            two_confirmations = breakout_candles
            full_custom_result = await self.custom_indicator.calculate({
                'candles': two_confirmations,
                'market_context': self.market_context
            })
            
            full_custom_hh = [b for b in full_custom_result['breaks'] if b['break_type'] == 'higher_high']
            
            # Custom indicator should now detect with 2 confirmations
            self.assertGreaterEqual(len(full_custom_hh), 1)
            if len(full_custom_hh) > 0:
                self.assertEqual(full_custom_hh[0]['candle']['high'], 106)  # The breakout candle's high
            
        asyncio.run(run_test())
    
    def test_break_thresholds(self):
        """Test behavior with different break thresholds."""
        async def run_test():
            # Create a candle list with a marginal higher high
            marginal_hh_candles = [candle.copy() for candle in self.hh_candle_dicts]
            
            # Modify the breakout candle to have a very small break
            breakout_idx = 8  # The candle that breaks
            marginal_hh_candles[breakout_idx]['high'] = 105.06  # Just 0.06 above swing high (105.0)
            
            # With default indicator (0.05% threshold)
            default_result = await self.indicator.calculate({
                'candles': marginal_hh_candles,
                'market_context': self.market_context
            })
            
            # With custom indicator (0.1% threshold)
            custom_result = await self.custom_indicator.calculate({
                'candles': marginal_hh_candles,
                'market_context': self.market_context
            })
            
            # Find higher high breaks in both results
            default_hh = [b for b in default_result['breaks'] if b['break_type'] == 'higher_high']
            custom_hh = [b for b in custom_result['breaks'] if b['break_type'] == 'higher_high']
            
            # Default indicator should detect the break (0.05% threshold)
            self.assertGreaterEqual(len(default_hh), 1)
            
            # Custom indicator should not detect (0.1% threshold)
            self.assertEqual(len(custom_hh), 0)
            
        asyncio.run(run_test())
    
    def test_sorting_of_breaks(self):
        """Test that breaks are sorted correctly (most recent first)."""
        async def run_test():
            # Create a scenario with multiple breaks
            # Combine candles from different scenarios
            combined_candles = self.hh_candle_dicts + self.ll_candle_dicts[5:]  # Add some lower lows after higher highs
            
            result = await self.indicator.calculate({
                'candles': combined_candles,
                'market_context': self.market_context
            })
            
            # Check that breaks are sorted by index (descending)
            if len(result['breaks']) >= 2:
                for i in range(len(result['breaks']) - 1):
                    self.assertGreaterEqual(result['breaks'][i]['index'], result['breaks'][i+1]['index'])
            
            # Verify latest_break is the most recent one
            if result['breaks']:
                self.assertEqual(result['latest_break'], result['breaks'][0])
            
        asyncio.run(run_test())
    
    def test_requirements(self):
        """Test that requirements are correctly reported."""
        requirements = self.indicator.get_requirements()
        
        # Check basic requirements
        self.assertTrue(requirements['candles'])
        self.assertTrue(requirements['market_context'])
        self.assertEqual(requirements['lookback_period'], 10)
        
        # Check timeframes
        self.assertIn('1m', requirements['timeframes'])
        self.assertIn('5m', requirements['timeframes'])
        self.assertIn('1h', requirements['timeframes'])
        self.assertIn('4h', requirements['timeframes'])
        self.assertIn('1d', requirements['timeframes'])
        
        # Check that there are no indicator dependencies
        self.assertEqual(requirements['indicators'], [])


if __name__ == '__main__':
    unittest.main()