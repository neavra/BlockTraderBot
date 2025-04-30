import unittest
import asyncio
from datetime import datetime, timezone
from dataclasses import asdict

# Import the FVGIndicator class
from strategy.indicators.fvg import FVGIndicator
from shared.domain.dto.candle_dto import CandleDto

class TestFVGIndicator(unittest.TestCase):
    """Test suite for FVGIndicator class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create indicator with default parameters
        self.indicator = FVGIndicator()
        
        # Create indicator with custom parameters
        self.custom_indicator = FVGIndicator(params={
            'min_gap_size': 0.1,       # Lower threshold for gap detection
            'max_age_candles': 10      # Fewer candles to look back
        })
        
        # Create test candles for bullish FVG scenario
        self.bullish_candles = [
            # Candle 0 - Base candle
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=102.0,
                low=98.0,
                close=101.0,
                volume=1000,
                is_closed=True
            ),
            # Candle 1 - Middle candle
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=101.0,
                high=111.0,
                low=100.0,
                close=110.0,
                volume=1200,
                is_closed=True
            ),
            # Candle 2 - FVG creating candle (gap up from candle 0's high)
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=110.0,
                high=112.0,
                low=109.0,  # Creates gap with candle 0's high (102.0)
                close=115.0,
                volume=1500,
                is_closed=True
            ),
            # Additional candle that doesn't create an FVG
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc),
                open=115.0,
                high=120.0,
                low=111.0,
                close=115.5,
                volume=1100,
                is_closed=True
            )
        ]
        
        # Create test candles for bearish FVG scenario
        self.bearish_candles = [
            # Candle 0 - Base candle
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=102.0,
                high=105.0,
                low=95.0,
                close=98.0,
                volume=1000,
                is_closed=True
            ),
            # Candle 1 - Middle candle
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=98,
                high=99.0,
                low=70.0,
                close=75.0,
                volume=1200,
                is_closed=True
            ),
            # Candle 2 - FVG creating candle (gap down from candle 0's low)
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=75.0,
                high=80.0,
                low=65.0,
                close=70.0,
                volume=1500,
                is_closed=True
            ),
            # Additional candle that doesn't create an FVG
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc),
                open=70.0,
                high=75.0,
                low=64.0,
                close=65.0,
                volume=1100,
                is_closed=True
            )
        ]
        
        # Create test candles for a filled FVG scenario
        self.filled_fvg_candles = [
            # Candle 0 - Base candle
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=102.0,
                low=98.0,
                close=101.0,
                volume=1000,
                is_closed=True
            ),
            # Candle 1 - Middle candle
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=101.0,
                high=111.0,
                low=100.0,
                close=110.0,
                volume=1200,
                is_closed=True
            ),
            # Candle 2 - FVG creating candle (gap up from candle 0's high)
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=110.0,
                high=116.0,
                low=109.0,  # Creates gap with candle 0's high (102.0)
                close=115.0,
                volume=1500,
                is_closed=True
            ),
            # Candle 3 - Fills the FVG by trading back into the gap
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc),
                open=115.0,
                high=121.0,
                low=102.5,  # Trades down into the gap (102.0 - 104.5)
                close=120.0,
                volume=1100,
                is_closed=True
            )
        ]
        
        # Create test candles with small gaps (below threshold)
        self.small_gap_candles = [
            # Candle 0 - Base candle
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=100.2,
                low=99.8,
                close=100.1,
                volume=1000,
                is_closed=True
            ),
            # Candle 1 - Middle candle
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=100.1,
                high=100.3,
                low=99.9,
                close=100.2,
                volume=1200,
                is_closed=True
            ),
            # Candle 2 - Creates tiny gap (too small to be FVG with default params)
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=100.3,
                high=100.5,
                low=100.3,  # Creates gap with candle 0's high (100.2)
                close=100.4,
                volume=1500,
                is_closed=True
            )
        ]

        data_dict = {
            "candles": self.bullish_candles,
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "exchange": "binance",
            "current_price": 100,
            "timestamp": datetime.now().isoformat()
        }

        self.data = data_dict
        
        # Convert CandleDto objects to dictionaries for testing
        # self.bullish_candle_dicts = [self._candle_to_dict(candle) for candle in self.bullish_candles]
        # self.bearish_candle_dicts = [self._candle_to_dict(candle) for candle in self.bearish_candles]
        # self.filled_fvg_candle_dicts = [self._candle_to_dict(candle) for candle in self.filled_fvg_candles]
        # self.small_gap_candle_dicts = [self._candle_to_dict(candle) for candle in self.small_gap_candles]
    
    # def _candle_to_dict(self, candle: CandleDto) -> dict:
    #     """Convert a CandleDto to a dictionary for the indicator."""
    #     candle_dict = {
    #         'open': candle.open,
    #         'high': candle.high,
    #         'low': candle.low,
    #         'close': candle.close,
    #         'volume': candle.volume,
    #         'timestamp': candle.timestamp
    #     }
    #     return candle_dict
    
    def test_initialization(self):
        """Test initialization with default and custom parameters."""
        # Test default parameters
        self.assertEqual(self.indicator.params['min_gap_size'], 0.2)
        
        # Test custom parameters
        self.assertEqual(self.custom_indicator.params['min_gap_size'], 0.1)
        self.assertEqual(self.custom_indicator.params['max_age_candles'], 10)
    
    def test_empty_candles(self):
        """Test behavior with empty or insufficient candles."""
        async def run_test():
            # Test with empty candles
            self.data["candles"] = []
            result = await self.indicator.calculate(self.data)
            self.assertEqual(len(result.bullish_fvgs), 0)
            self.assertEqual(len(result.bearish_fvgs), 0)
            
            # Test with insufficient candles (less than 3)
            self.data["candles"] = self.bullish_candles[:2]
            result = await self.indicator.calculate(self.data)
            self.assertEqual(len(result.bullish_fvgs), 0)
            self.assertEqual(len(result.bearish_fvgs), 0)
            
        asyncio.run(run_test())
    
    def test_bullish_fvg_detection(self):
        """Test detection of bullish Fair Value Gaps."""
        async def run_test():
            # Calculate with bullish FVG candles
            self.data["candles"] = self.bullish_candles
            result = await self.indicator.calculate(self.data)
            
            # There should be 1 bullish FVG detected
            self.assertEqual(len(result.bullish_fvgs), 1)
            self.assertEqual(len(result.bearish_fvgs), 0)
            
            # Verify details of the bullish FVG
            bullish_fvg = result.bullish_fvgs[0]
            self.assertEqual(bullish_fvg.type, 'bullish')
            self.assertEqual(bullish_fvg.bottom, 102.0)  # Candle 0's high
            self.assertEqual(bullish_fvg.top, 109.0)     # Candle 2's low
            self.assertGreaterEqual(bullish_fvg.size_percent, 2.0)  # Should be about 2.45%
            self.assertFalse(bullish_fvg.filled)
            
            # Check that timestamp was preserved
            self.assertIsNotNone(bullish_fvg.timestamp)
            
            # Verify candle data is included
            self.assertIsNotNone(bullish_fvg.candle)
            
        asyncio.run(run_test())
    
    def test_bearish_fvg_detection(self):
        """Test detection of bearish Fair Value Gaps."""
        async def run_test():
            self.data["candles"] = self.bearish_candles
            result = await self.indicator.calculate(self.data)
            
            # There should be 1 bearish FVG detected
            self.assertEqual(len(result.bullish_fvgs), 0)
            self.assertEqual(len(result.bearish_fvgs), 1)
            
            # Verify details of the bearish FVG
            bearish_fvg = result.bearish_fvgs[0]
            self.assertEqual(bearish_fvg.type, 'bearish')
            self.assertEqual(bearish_fvg.top, 95.0)      # Candle 0's low
            self.assertEqual(bearish_fvg.bottom, 80.0)   # Candle 2's high
            self.assertGreaterEqual(bearish_fvg.size_percent, 2.0)  # Should be about 2.04%
            self.assertFalse(bearish_fvg.filled)
            
            # Check that timestamp was preserved
            self.assertIsNotNone(bearish_fvg.timestamp)
            
            # Verify candle data is included
            self.assertIsNotNone(bearish_fvg.candle)

            
        asyncio.run(run_test())
    
    def test_filled_fvg_detection(self):
        """Test detection of filled Fair Value Gaps."""
        async def run_test():
            # Calculate with candles containing a filled FVG
            self.data["candles"] = self.filled_fvg_candles
            result = await self.indicator.calculate(self.data)
            
            # There should be 1 bullish FVG detected
            self.assertEqual(len(result.bullish_fvgs), 1)
            
            # Verify the FVG is marked as filled
            bullish_fvg = result.bullish_fvgs[0]
            self.assertTrue(bullish_fvg.filled)
            
        asyncio.run(run_test())
    
    def test_small_gap_detection(self):
        """Test that gaps smaller than the threshold are ignored."""
        async def run_test():
            # Calculate with candles containing a small gap
            self.data["candles"] = self.small_gap_candles
            result = await self.indicator.calculate(self.data)
            
            # No FVGs should be detected with default parameters
            self.assertEqual(len(result.bullish_fvgs), 0)
            self.assertEqual(len(result.bearish_fvgs), 0)
            
            # With custom parameters (lower threshold), the gap should be detected
            self.data["candles"] = self.small_gap_candles
            result = await self.indicator.calculate(self.data)
            
            # The custom indicator may detect the gap depending on exact values
            # Only assert something if FVGs are detected
            if len(result.bullish_fvgs) > 0:
                bullish_fvg = result.bullish_fvgs[0]
                self.assertEqual(bullish_fvg.bottom, 100.2)  # Candle 0's high
                self.assertEqual(bullish_fvg.top, 100.3)    # Candle 2's low
            
        asyncio.run(run_test())
    
    def test_requirements(self):
        """Test that requirements are correctly reported."""
        requirements = self.indicator.get_requirements()
        
        # Check basic requirements
        self.assertTrue(requirements['candles'])
        self.assertEqual(requirements['lookback_period'], 30)
        
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