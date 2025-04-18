import unittest
import asyncio
from datetime import datetime, timezone
from dataclasses import asdict

# Import the DojiCandleIndicator class
from strategy.indicators.doji_candle import DojiCandleIndicator
from shared.domain.dto.candle_dto import CandleDto

class TestDojiCandleIndicator(unittest.TestCase):
    """Test suite for DojiCandleIndicator class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create indicator with default parameters
        self.indicator = DojiCandleIndicator()
        
        # Create indicator with custom parameters
        self.custom_indicator = DojiCandleIndicator(params={
            'max_body_to_range_ratio': 0.2,     # More permissive ratio
            'min_range_to_price_ratio': 0.003,  # Lower minimum range
            'lookback_period': 10               # Shorter lookback
        })
        
        # Create some test candles as CandleDto objects
        self.candles = [
            # Perfect doji with almost no body and decent range
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
                open=100.0,
                high=105.0,
                low=95.0,
                close=100.1,  # Very small body
                volume=1000,
                is_closed=True
            ),
            # Near-doji with small body
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc),
                open=101.0,
                high=106.0,
                low=96.0,
                close=103.0,  # Small body but larger than perfect doji
                volume=1200,
                is_closed=True
            ),
            # Not a doji - body too large relative to range
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc),
                open=102.0,
                high=110.0,
                low=98.0,
                close=108.0,  # Large body
                volume=1500,
                is_closed=True
            ),
            # Not a doji - range too small relative to price
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc),
                open=100.0,
                high=100.2,
                low=99.9,
                close=100.0,  # Small body but tiny range
                volume=800,
                is_closed=True
            ),
            # Another perfect doji
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
                open=105.0,
                high=110.0,
                low=100.0,
                close=105.0,  # Zero body
                volume=1800,
                is_closed=True
            )
        ]
        
        # Convert CandleDto objects to dictionaries for testing
        # self.candle_dicts = [self._candle_to_dict(candle) for candle in self.candles]
    
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
        self.assertEqual(self.indicator.params['max_body_to_range_ratio'], 0.1)
        self.assertEqual(self.indicator.params['min_range_to_price_ratio'], 0.005)
        self.assertEqual(self.indicator.params['lookback_period'], 20)
        
        # Test custom parameters
        self.assertEqual(self.custom_indicator.params['max_body_to_range_ratio'], 0.2)
        self.assertEqual(self.custom_indicator.params['min_range_to_price_ratio'], 0.003)
        self.assertEqual(self.custom_indicator.params['lookback_period'], 10)
    
    def test_empty_candles(self):
        """Test behavior with empty or insufficient candles."""
        async def run_test():
            # Test with empty candles
            result = await self.indicator.calculate({'candles': []})
            self.assertEqual(len(result.dojis), 0)
            self.assertFalse(result.has_doji)
            self.assertIsNone(result.latest_doji)

            
            # Test with insufficient candles (less than 3)
            result = await self.indicator.calculate({'candles': self.candles[:2]})
            self.assertEqual(len(result.dojis), 0)
            self.assertFalse(result.has_doji)
            self.assertIsNone(result.latest_doji)
            
        asyncio.run(run_test())
    
    def test_doji_detection_default_params(self):
        """Test doji detection with default parameters."""
        async def run_test():
            # Calculate with all candles
            result = await self.indicator.calculate(self.candles)
            
            # There should be 2 dojis with default parameters (the first and last candles)
            self.assertEqual(len(result.dojis), 2)
            self.assertTrue(result.has_doji)
            
            # Check that the latest doji is the most recent one (candles are processed in reverse)
            self.assertIsNotNone(result.latest_doji)
            self.assertEqual(result.latest_doji.index, 4)  # Index of last candle
            
            # Verify details of detected dojis
            for doji in result.dojis:
                # Ensure body_to_range_ratio is within limits
                self.assertLessEqual(doji.body_to_range_ratio, self.indicator.params['max_body_to_range_ratio'])
                
                # Check that timestamp was preserved
                self.assertIsNotNone(doji.timestamp)
                
                # Verify candle data is included
                self.assertIsNotNone(doji.candle)
                
                # Verify strength calculation (higher for smaller bodies)
                self.assertAlmostEqual(doji.strength, 1.0 - doji.body_to_range_ratio)
            
        asyncio.run(run_test())
    
    def test_doji_detection_custom_params(self):
        """Test doji detection with custom parameters."""
        async def run_test():
            # Calculate with all candles using custom parameters
            result = await self.custom_indicator.calculate(self.candles)
            
            # With more permissive parameters, we should detect more dojis
            self.assertTrue(len(result.dojis) >= 2)
            self.assertTrue(result.has_doji)
            
            # Check specific candles that should be detected
            first_candle_detected = False
            second_candle_detected = False
            
            for doji in result.dojis:
                if doji.index == 0:  # Perfect doji
                    first_candle_detected = True
                elif doji.index == 1:  # Near doji that should be detected with custom params
                    second_candle_detected = True
            
            self.assertTrue(first_candle_detected)
            self.assertTrue(second_candle_detected)
            
        asyncio.run(run_test())
    
    def test_edge_cases(self):
        """Test edge cases like zero range candles."""
        async def run_test():
            # Create a candle with zero range
            zero_range_candle = CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc),
                open=100.0,
                high=100.0,
                low=100.0,
                close=100.0,
                volume=500,
                is_closed=True
            )
            
            test_candles = self.candles + [zero_range_candle]
            
            # The indicator should handle this without errors
            result = await self.indicator.calculate(test_candles)
            
            # Zero range candle should be skipped (not causing errors)
            self.assertEqual(len(result.dojis), 2)
            
        asyncio.run(run_test())
    
    def test_requirements(self):
        """Test that requirements are correctly reported."""
        requirements = self.indicator.get_requirements()
        
        # Check basic requirements
        self.assertTrue(requirements['candles'])
        self.assertEqual(requirements['lookback_period'], 20)
        
        # Check timeframes
        self.assertIn('1m', requirements['timeframes'])
        self.assertIn('5m', requirements['timeframes'])
        self.assertIn('1h', requirements['timeframes'])
        self.assertIn('4h', requirements['timeframes'])
        self.assertIn('1d', requirements['timeframes'])
        
        # Check that there are no indicator dependencies
        self.assertEqual(requirements['indicators'], [])
    
    def test_candle_sorting(self):
        """Test that dojis are sorted correctly (most recent first)."""
        async def run_test():
            # Reverse the candle order to ensure sorting works
            reversed_candles = list(reversed(self.candles))
            
            result = await self.indicator.calculate(reversed_candles)
            
            # Check that dojis are sorted by index (descending)
            if len(result.dojis) >= 2:
                for i in range(len(result.dojis) - 1):
                    self.assertGreaterEqual(result.dojis[i].index, result.dojis[i+1].index)
            
        asyncio.run(run_test())


if __name__ == '__main__':
    unittest.main()