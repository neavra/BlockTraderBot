import unittest
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List
from unittest.mock import MagicMock, AsyncMock

# Import the StructureBreakIndicator class
from strategy.indicators.bos import StructureBreakIndicator
from strategy.domain.dto.bos_dto import StructureBreakDto, StructureBreakResultDto
from strategy.domain.models.market_context import MarketContext
from shared.domain.dto.candle_dto import CandleDto

class TestStructureBreakIndicator(unittest.TestCase):
    """Test suite for StructureBreakIndicator class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the FvgRepository
        self.mock_repository = MagicMock()
        
        # Set up the bulk_create_bos method to return a successful result
        self.mock_repository.bulk_create_bos = AsyncMock(return_value=[])
        # Create indicator with default parameters
        self.indicator = StructureBreakIndicator(repository=self.mock_repository)
        
        # Create indicator with custom parameters
        self.custom_indicator = StructureBreakIndicator(
            repository=self.mock_repository,
            params={
            'lookback_period': 5,           # Smaller lookback
            'confirmation_candles': 2,      # More confirmations required
            'min_break_percentage': 0.001   # Higher threshold (0.1%)
        })
        
        # Create market context with swing points
        self.market_context = MarketContext(symbol= "BTCUSDT", timeframe="1h")
        self.market_context.swing_high = {
                'price': 105.0,
                'index': 3,
                'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc).isoformat()
            }
        
        self.market_context.swing_low = {
                'price': 95.0,
                'index': 5,
                'timestamp': datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc).isoformat()
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

        data_dict = {
            "candles": self.hh_candles,
            "market_contexts": [self.market_context],
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "exchange": "binance",
            "current_price": 100,
            "timestamp": datetime.now().isoformat()
        }

        self.data = data_dict
        
        # Convert all candle lists to dictionaries for testing
        # self.hh_candle_dicts = [self._candle_to_dict(candle) for candle in self.hh_candles]
        # self.ll_candle_dicts = [self._candle_to_dict(candle) for candle in self.ll_candles]
        # self.hl_candle_dicts = [self._candle_to_dict(candle) for candle in self.hl_candles]
        # self.lh_candle_dicts = [self._candle_to_dict(candle) for candle in self.lh_candles]
        # self.no_bos_candle_dicts = [self._candle_to_dict(candle) for candle in self.no_bos_candles]
    
    def _create_candle(self, index: int, data: Dict[str, Any]) -> CandleDto:
        """Create a CandleDto object with given parameters."""
        return CandleDto(
            symbol="BTCUSDT",
            exchange="binance",
            timeframe="1h",
            timestamp=data['dt'],
            open=data['o'],
            high=data['h'],
            low=data['l'],
            close=data['c'],
            volume=1000 + index * 100,  # Just a dummy value that increases with index
            is_closed=True
        )
    
    # def _candle_to_dict(self, candle: CandleDto) -> Dict[str, Any]:
    #     """Convert a CandleDto to a dictionary for the indicator."""
    #     return {
    #         'open': candle.open,
    #         'high': candle.high,
    #         'low': candle.low,
    #         'close': candle.close,
    #         'volume': candle.volume,
    #         'timestamp': candle.timestamp
    #     }
    
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
            self.data["candles"] = []
            result = await self.indicator.calculate(self.data)
            self.assertEqual(len(result.bullish_breaks), 0)
            self.assertEqual(len(result.bearish_breaks), 0)
            self.assertFalse(result.has_bullish_break)
            self.assertFalse(result.has_bearish_break)
            self.assertIsNone(result.latest_break)
            
            # Test with insufficient candles (less than 3)
            self.data["candles"] = self.hh_candles[:2]
            result = await self.indicator.calculate(self.data)
            self.assertEqual(len(result.bullish_breaks), 0)
            self.assertEqual(len(result.bearish_breaks), 0)
            self.assertFalse(result.has_bullish_break)
            self.assertFalse(result.has_bearish_break)
            self.assertIsNone(result.latest_break)
            
        asyncio.run(run_test())
    
    def test_missing_market_context(self):
        """Test behavior with missing market context."""
        async def run_test():
            # Test with empty market context
            self.data["candles"] = self.hh_candles
            self.data["market_contexts"] = [MarketContext(symbol= "BTCUSDT", timeframe="1h")]
            result = await self.indicator.calculate(self.data)
            self.assertEqual(len(result.bullish_breaks), 0)
            self.assertEqual(len(result.bearish_breaks), 0)
            self.assertFalse(result.has_bullish_break)
            self.assertFalse(result.has_bearish_break)
            self.assertIsNone(result.latest_break)
            
        asyncio.run(run_test())
    
    def test_higher_high_detection(self):
        """Test detection of Higher High (HH) breakouts."""
        async def run_test():
            # Calculate with Higher High scenarioa
            self.data["candles"] = self.hh_candles
            result = await self.indicator.calculate(self.data)
            
            # Verify a bullish break was detected
            self.assertTrue(result.has_bullish_break)
            
            # Find all higher high breaks 
            higher_highs = result.higher_highs
            
            # There should be at least one higher high
            self.assertGreaterEqual(len(higher_highs), 1)
            
            # Verify details of the most recent higher high
            latest_hh = higher_highs[0]
            self.assertEqual(latest_hh.break_type, 'higher_high')
            self.assertGreater(latest_hh.break_value, 0)  # Break value should be positive
            self.assertGreater(latest_hh.break_percentage, 0)  # Break percentage should be positive
            self.assertEqual(latest_hh.swing_reference, 105.0)  # Reference to swing high
            
            # Verify candle data is included
            self.assertIsNotNone(latest_hh.candle)
            
        asyncio.run(run_test())
    
    def test_lower_low_detection(self):
        """Test detection of Lower Low (LL) breakouts."""
        async def run_test():
            # Calculate with Lower Low scenario
            self.data["candles"] = self.ll_candles
            result = await self.indicator.calculate(self.data)
            print(result)
            # Verify a bearish break was detected
            self.assertTrue(result.has_bearish_break)
            
            # Find all lower low breaks
            lower_lows = result.lower_lows
            
            # There should be at least one lower low
            self.assertGreaterEqual(len(lower_lows), 1)
            
            # Verify details of the most recent lower low
            latest_ll = lower_lows[0]
            self.assertEqual(latest_ll.break_type, 'lower_low')
            self.assertGreater(latest_ll.break_value, 0)  # Break value should be positive
            self.assertGreater(latest_ll.break_percentage, 0)  # Break percentage should be positive
            self.assertEqual(latest_ll.swing_reference, 95.0)  # Reference to swing low
            
            # Verify candle data is included
            self.assertIsNotNone(latest_ll.candle)
            
        asyncio.run(run_test())
    
    def test_higher_low_detection(self):
        """Test detection of Higher Lows (HL)."""
        async def run_test():
            # Calculate with Higher Low scenario
            self.data["candles"] = self.hl_candles
            result = await self.indicator.calculate(self.data)
            
            # Verify a bullish structural element was detected
            self.assertTrue(result.has_bullish_break)
            
            # Find all higher low breaks
            higher_lows = result.higher_lows
            
            # There should be at least one higher low
            self.assertGreaterEqual(len(higher_lows), 1)
            
            # Verify details of the most recent higher low
            latest_hl = higher_lows[0]
            self.assertEqual(latest_hl.break_type, 'higher_low')
            self.assertGreater(latest_hl.break_value, 0)  # Break value should be positive
            self.assertGreater(latest_hl.break_percentage, 0)  # Break percentage should be positive
            self.assertEqual(latest_hl.swing_reference, 95.0)  # Reference to swing low
            
        asyncio.run(run_test())
    
    def test_lower_high_detection(self):
        """Test detection of Lower Highs (LH)."""
        async def run_test():
            # Calculate with Lower High scenario
            self.data["candles"] = self.lh_candles
            result = await self.indicator.calculate(self.data)
            
            # Verify a bearish structural element was detected
            self.assertTrue(result.has_bearish_break)
            
            # Find all lower high breaks
            lower_highs = result.lower_highs
            
            # There should be at least one lower high
            self.assertGreaterEqual(len(lower_highs), 1)
            
            # Verify details of the most recent lower high
            latest_lh = lower_highs[0]
            self.assertEqual(latest_lh.break_type, 'lower_high')
            self.assertGreater(latest_lh.break_value, 0)  # Break value should be positive
            self.assertGreater(latest_lh.break_percentage, 0)  # Break percentage should be positive
            self.assertEqual(latest_lh.swing_reference, 105.0)  # Reference to swing high
            
        asyncio.run(run_test())
    
    def test_no_bos_detection(self):
        """Test behavior when no BOS events are present."""
        async def run_test():
            # Calculate with No BOS scenario
            self.data["candles"] = self.no_bos_candles
            result = await self.indicator.calculate(self.data)
            
            # There should be no higher highs or lower lows detected
            self.assertEqual(len(result.higher_highs), 0)
            self.assertEqual(len(result.lower_lows), 0)
            
            # There may be higher lows or lower highs, so we don't test those specifically
            
        asyncio.run(run_test())
    
    def test_confirmation_requirement(self):
        """Test behavior with different confirmation requirements."""
        async def run_test():
            # Let's create a more controlled test case specifically for confirmations
            # Create candles where there's a clear breakout followed by confirmations
            breakout_candles = [self._create_candle(i, data) for i, data in enumerate([
                {'o': 100, 'h': 102, 'l': 98, 'c': 101, 'dt': datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)},
                {'o': 101, 'h': 103, 'l': 99, 'c': 102, 'dt': datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc)},
                # Candle that breaks above swing high (105.0)
                {'o': 102, 'h': 106, 'l': 101, 'c': 105, 'dt': datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc)},
                # First confirmation candle
                {'o': 105, 'h': 107, 'l': 104, 'c': 106, 'dt': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc)},
                # Second confirmation candle
                {'o': 106, 'h': 108, 'l': 105, 'c': 107, 'dt': datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc)}
            ])]
            
            # Test with 1 confirmation (default indicator)
            one_confirmation = breakout_candles[:4]  # Up to and including first confirmation
            self.data["candles"] = one_confirmation
            default_result = await self.indicator.calculate(self.data)
            
            # Default indicator (1 confirmation) should detect the break
            self.assertGreaterEqual(len(default_result.higher_highs), 1)
            if len(default_result.higher_highs) > 0:
                self.assertEqual(default_result.higher_highs[0].candle.high, 106)  # The breakout candle's high
            
            # With custom indicator (2 confirmations needed)
            custom_result = await self.custom_indicator.calculate(self.data)
            
            # Custom indicator should not detect (needs 2 confirmations)
            self.assertEqual(len(custom_result.higher_highs), 0)
            
            # Now test with two confirmations available
            two_confirmations = breakout_candles
            self.data["candles"] = two_confirmations
            full_custom_result = await self.custom_indicator.calculate(self.data)
            
            # Custom indicator should now detect with 2 confirmations
            self.assertGreaterEqual(len(full_custom_result.higher_highs), 1)
            if len(full_custom_result.higher_highs) > 0:
                self.assertEqual(full_custom_result.higher_highs[0].candle.high, 106)  # The breakout candle's high
            
        asyncio.run(run_test())
    
    def test_break_thresholds(self):
        """Test behavior with different break thresholds."""
        async def run_test():
            # Create a candle list with a marginal higher high
            marginal_hh_candles = [candle for candle in self.hh_candles]
            
            # Modify the breakout candle to have a very small break
            breakout_idx = 8  # The candle that breaks
            marginal_hh_candles[breakout_idx].high = 105.06  # Just 0.06 above swing high (105.0)
            self.data["candles"] = marginal_hh_candles
            # With default indicator (0.05% threshold)
            default_result = await self.indicator.calculate(self.data)
            
            # With custom indicator (0.1% threshold)
            custom_result = await self.custom_indicator.calculate(self.data)
            
            # Default indicator should detect the break (0.05% threshold)
            self.assertGreaterEqual(len(default_result.higher_highs), 1)
            
            # Custom indicator should not detect (0.1% threshold)
            self.assertEqual(len(custom_result.higher_highs), 0)
            
        asyncio.run(run_test())
    
    def test_sorting_of_breaks(self):
        """Test that breaks are sorted correctly (most recent first)."""
        async def run_test():
            # Create a scenario with multiple breaks
            # Combine candles from different scenarios
            combined_candles = self.hh_candles + self.ll_candles[5:]  # Add some lower lows after higher highs
            self.data["candles"] = combined_candles
            result = await self.indicator.calculate(self.data)
            
            # Check that bullish breaks are sorted by index (descending)
            if len(result.bullish_breaks) >= 2:
                for i in range(len(result.bullish_breaks) - 1):
                    self.assertGreaterEqual(result.bullish_breaks[i].index, result.bullish_breaks[i+1].index)
            
            # Check that bearish breaks are sorted by index (descending)
            if len(result.bearish_breaks) >= 2:
                for i in range(len(result.bearish_breaks) - 1):
                    self.assertGreaterEqual(result.bearish_breaks[i].index, result.bearish_breaks[i+1].index)
                    
            # Verify latest_break is the most recent one from either list
            if result.bullish_breaks or result.bearish_breaks:
                all_breaks = result.all_breaks
                most_recent_break = max(all_breaks, key=lambda b: b.index) if all_breaks else None
                if most_recent_break:
                    self.assertEqual(result.latest_break, most_recent_break)
            
        asyncio.run(run_test())

    def test_multi_timeframe_detection(self):
        """Test BOS detection across multiple timeframes."""
        async def run_test():
            # Create multiple market contexts for different timeframes
            # First context has NO valid swing points
            context_1h = MarketContext(symbol="BTCUSDT", timeframe="1h")
            # Not setting swing points for this one
            
            # Second context has valid swing points
            context_4h = MarketContext(symbol="BTCUSDT", timeframe="4h")
            context_4h.swing_high = {
                'price': 105.0,
                'index': 3,
                'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc).isoformat()
            }
            context_4h.swing_low = {
                'price': 95.0,
                'index': 5,
                'timestamp': datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc).isoformat()
            }
            
            # Third context also has valid swing points but should not be used
            # if breaks are found in context_4h
            context_1d = MarketContext(symbol="BTCUSDT", timeframe="1d")
            context_1d.swing_high = {
                'price': 110.0,
                'index': 2,
                'timestamp': datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc).isoformat()
            }
            context_1d.swing_low = {
                'price': 90.0,
                'index': 4,
                'timestamp': datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc).isoformat()
            }
            
            # Test data with all three contexts
            data = self.data.copy()
            data["candles"] = self.hh_candles
            data["market_contexts"] = [context_1h, context_4h, context_1d]
            
            # Monitor which contexts get checked by patching the _detect_breaks method
            original_detect_breaks = self.indicator._detect_breaks
            detect_breaks_calls = []
            
            def mock_detect_breaks(*args, **kwargs):
                # Record the swing values used in the call
                swing_high_price = args[1]
                swing_low_price = args[2]
                detect_breaks_calls.append((swing_high_price, swing_low_price))
                return original_detect_breaks(*args, **kwargs)
            
            self.indicator._detect_breaks = mock_detect_breaks
            
            # Run the indicator
            result = await self.indicator.calculate(data)
            
            # Restore original method
            self.indicator._detect_breaks = original_detect_breaks
            
            # Verify results
            self.assertIsInstance(result, StructureBreakResultDto)
            
            # We should have detected breaks
            all_breaks = result.bullish_breaks + result.bearish_breaks
            self.assertGreater(len(all_breaks), 0, "No breaks detected with valid contexts")
            
            # Verify we skipped the first context (which had no swing points)
            # and used the second context (4h timeframe)
            self.assertEqual(len(detect_breaks_calls), 1, 
                            "Should have checked exactly one context (skipping first, stopping after second)")
            
            # Verify we used the swing values from the 4h context
            used_swing_high, used_swing_low = detect_breaks_calls[0]
            self.assertEqual(used_swing_high, 105.0, "Used wrong swing high value")
            self.assertEqual(used_swing_low, 95.0, "Used wrong swing low value")
            
        asyncio.run(run_test())

    def test_higher_timeframe_fallback(self):
        """Test fallback to higher timeframe when no breaks found in lower timeframes."""
        async def run_test():
            # Create test candles where no breaks will be detected with first two contexts
            no_breaks_candles = self.no_bos_candles.copy()
            
            # Create multiple market contexts for different timeframes
            # First context has valid swing points, but positioned so no breaks will be detected
            context_1h = MarketContext(symbol="BTCUSDT", timeframe="1h")
            context_1h.swing_high = {
                'price': 110.0,  # Much higher than any price in our candles
                'index': 3,
                'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc).isoformat()
            }
            context_1h.swing_low = {
                'price': 90.0,   # Much lower than any price in our candles
                'index': 5,
                'timestamp': datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc).isoformat()
            }
            
            # Second context also has swing points that won't detect breaks
            context_4h = MarketContext(symbol="BTCUSDT", timeframe="4h")
            context_4h.swing_high = {
                'price': 108.0,  # Still too high for breaks
                'index': 3,
                'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc).isoformat()
            }
            context_4h.swing_low = {
                'price': 92.0,   # Still too low for breaks
                'index': 5,
                'timestamp': datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc).isoformat()
            }
            
            # Third context has swing points positioned to detect breaks
            context_1d = MarketContext(symbol="BTCUSDT", timeframe="1d")
            context_1d.swing_high = {
                'price': 105.0,  # Same as in our original test
                'index': 3,
                'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc).isoformat()
            }
            context_1d.swing_low = {
                'price': 95.0,   # Same as in our original test
                'index': 5,
                'timestamp': datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc).isoformat()
            }
            
            # Test data with all three contexts
            data = self.data.copy()
            data["candles"] = self.hh_candles  # Using higher high candles to ensure we find breaks on the third context
            data["market_contexts"] = [context_1h, context_4h, context_1d]
            
            # Monitor which contexts get checked by patching the _detect_breaks method
            original_detect_breaks = self.indicator._detect_breaks
            detect_breaks_calls = []
            
            def mock_detect_breaks(*args, **kwargs):
                # Record the swing values used in the call
                swing_high_price = args[1]
                swing_low_price = args[2]
                detect_breaks_calls.append((swing_high_price, swing_low_price))
                
                # For the first two contexts, return empty lists
                if len(detect_breaks_calls) <= 2:
                    return [], []
                else:
                    # For the third context, call the original method
                    return original_detect_breaks(*args, **kwargs)
            
            self.indicator._detect_breaks = mock_detect_breaks
            
            # Run the indicator
            result = await self.indicator.calculate(data)
            
            # Restore original method
            self.indicator._detect_breaks = original_detect_breaks
            
            # Verify results
            self.assertIsInstance(result, StructureBreakResultDto)
            
            # We should have detected breaks from the third context
            all_breaks = result.bullish_breaks + result.bearish_breaks
            self.assertGreater(len(all_breaks), 0, "No breaks detected with third context")
            
            # Verify we checked all three contexts
            self.assertEqual(len(detect_breaks_calls), 3, 
                            "Should have checked all three contexts")
            
            # Verify the swing values we used in each check
            self.assertEqual(detect_breaks_calls[0], (110.0, 90.0), "Wrong values for 1h context")
            self.assertEqual(detect_breaks_calls[1], (108.0, 92.0), "Wrong values for 4h context")
            self.assertEqual(detect_breaks_calls[2], (105.0, 95.0), "Wrong values for 1d context")
            
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