import unittest
import asyncio
from datetime import datetime, timezone
from dataclasses import asdict

# Import the OrderBlockIndicator class and dependencies
from strategy.indicators.composite_indicators.order_block import OrderBlockIndicator

class TestOrderBlockIndicator(unittest.TestCase):
    """Test suite for OrderBlockIndicator class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create indicator with default parameters
        self.indicator = OrderBlockIndicator()
        
        # Create indicator with custom parameters
        self.custom_indicator = OrderBlockIndicator(params={
            'max_body_to_range_ratio': 0.5,   # More permissive ratio
            'min_wick_to_body_ratio': 1.2,    # Less strict wick requirement
            'max_detection_window': 3,        # Shorter detection window
            'require_bos': False              # Make BOS optional for testing
        })
        
        # Create a simple set of candles for testing
        self.candles = [
            # Candle 0
            {
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 98.0,   # Bearish candle
                'volume': 1000,
                'timestamp': datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
            },
            # Candle 1
            {
                'open': 98.0,
                'high': 104.0,
                'low': 96.0,
                'close': 103.0,  # Bullish candle
                'volume': 1200,
                'timestamp': datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc)
            },
            # Candle 2
            {
                'open': 103.0,
                'high': 110.0,
                'low': 102.0,
                'close': 108.0,  # Bullish candle
                'volume': 1500,
                'timestamp': datetime(2023, 1, 1, 2, 0, tzinfo=timezone.utc)
            },
            # Candle 3
            {
                'open': 108.0,
                'high': 112.0,
                'low': 106.0,
                'close': 107.0,  # Bearish candle
                'volume': 1300,
                'timestamp': datetime(2023, 1, 1, 3, 0, tzinfo=timezone.utc)
            },
            # Candle 4
            {
                'open': 107.0,
                'high': 109.0,
                'low': 102.0,
                'close': 104.0,  # Bearish candle
                'volume': 1100,
                'timestamp': datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc)
            }
        ]
        
        # Create mock doji data
        # For demand blocks: we need bearish (red) dojis
        # For supply blocks: we need bullish (green) dojis
        self.doji_data = {
            'dojis': [
                # Bearish doji at index 0
                {
                    'index': 0,
                    'body_to_range_ratio': 0.08,  # Small body (2 / 10 = 0.2)
                    'candle': self.candles[0].copy(),
                    'timestamp': self.candles[0]['timestamp'],
                    'strength': 0.92  # 1.0 - body_to_range_ratio
                },
                # Bullish doji at index 1
                {
                    'index': 1,
                    'body_to_range_ratio': 0.07,  # Small body (5 / 8 = 0.625)
                    'candle': self.candles[1].copy(),
                    'timestamp': self.candles[1]['timestamp'],
                    'strength': 0.93  # 1.0 - body_to_range_ratio
                }
            ],
            'has_doji': True,
            'latest_doji': {
                'index': 1,
                'body_to_range_ratio': 0.07,
                'candle': self.candles[1].copy(),
                'timestamp': self.candles[1]['timestamp'],
                'strength': 0.93
            }
        }
        
        # Create mock FVG data
        self.fvg_data = {
            'bullish_fvgs': [
                # Bullish FVG at index 2 (after bearish doji at index 0)
                {
                    'type': 'bullish',
                    'candle_index': 2,
                    'top': 102.0,   # Low of candle 2
                    'bottom': 104.0, # High of candle 0
                    'size': 6.0,
                    'candle': self.candles[2].copy(),
                    'timestamp': self.candles[2]['timestamp'],
                    'filled': False
                }
            ],
            'bearish_fvgs': [
                # Bearish FVG at index 3 (after bullish doji at index 1)
                {
                    'type': 'bearish',
                    'candle_index': 3,
                    'top': 104.0,   # Low of candle 1
                    'bottom': 112.0, # High of candle 3
                    'size': 8.0,
                    'candle': self.candles[3].copy(),
                    'timestamp': self.candles[3]['timestamp'],
                    'filled': False
                }
            ]
        }
        
        # Create mock BOS data
        self.bos_data = {
            'breaks': [
                # Bullish BOS at index 3 (confirming bullish FVG)
                {
                    'break_type': 'higher_high',
                    'index': 3,
                    'candle': self.candles[3].copy(),
                    'timestamp': self.candles[3]['timestamp'],
                    'swing_reference': 105.0
                },
                # Bearish BOS at index 4 (confirming bearish FVG)
                {
                    'break_type': 'lower_low',
                    'index': 4,
                    'candle': self.candles[4].copy(),
                    'timestamp': self.candles[4]['timestamp'],
                    'swing_reference': 106.0
                }
            ],
            'has_bullish_break': True,
            'has_bearish_break': True,
            'latest_break': {
                'break_type': 'lower_low',
                'index': 4,
                'candle': self.candles[4].copy(),
                'timestamp': self.candles[4]['timestamp'],
                'swing_reference': 106.0
            }
        }
    
    def test_initialization(self):
        """Test initialization with default and custom parameters."""
        # Test default parameters
        self.assertEqual(self.indicator.params['max_body_to_range_ratio'], 0.4)
        self.assertEqual(self.indicator.params['min_wick_to_body_ratio'], 1.5)
        self.assertEqual(self.indicator.params['max_detection_window'], 5)
        self.assertTrue(self.indicator.params['require_doji'])
        self.assertTrue(self.indicator.params['require_bos'])
        
        # Test custom parameters
        self.assertEqual(self.custom_indicator.params['max_body_to_range_ratio'], 0.5)
        self.assertEqual(self.custom_indicator.params['min_wick_to_body_ratio'], 1.2)
        self.assertEqual(self.custom_indicator.params['max_detection_window'], 3)
        self.assertFalse(self.custom_indicator.params['require_bos'])
    
    def test_empty_candles(self):
        """Test behavior with empty or insufficient candles."""
        async def run_test():
            # Test with empty candles
            result = await self.indicator.calculate({'candles': []})
            self.assertEqual(len(result.demand_blocks), 0)
            self.assertEqual(len(result.supply_blocks), 0)
            self.assertFalse(result.has_demand_block)
            self.assertFalse(result.has_supply_block)
            self.assertIsNone(result.latest_block)
            
            # Test with insufficient candles (less than 5)
            result = await self.indicator.calculate({'candles': self.candles[:4]})
            self.assertEqual(len(result.demand_blocks), 0)
            self.assertEqual(len(result.supply_blocks), 0)
            
        asyncio.run(run_test())
    
    def test_missing_dependency_data(self):
        """Test behavior when required indicator data is missing."""
        async def run_test():
            # Test without doji data (required)
            data = {
                'candles': self.candles,
                'fvg_data': self.fvg_data,
                'bos_data': self.bos_data
            }
            result = await self.indicator.calculate(data)
            self.assertEqual(len(result.demand_blocks), 0)
            self.assertEqual(len(result.supply_blocks), 0)
            self.assertFalse(result.has_demand_block)
            self.assertFalse(result.has_supply_block)
            
            # Test with empty doji data
            data = {
                'candles': self.candles,
                'doji_data': {'dojis': []},
                'fvg_data': self.fvg_data,
                'bos_data': self.bos_data
            }
            result = await self.indicator.calculate(data)
            self.assertEqual(len(result.demand_blocks), 0)
            self.assertEqual(len(result.supply_blocks), 0)
            
        asyncio.run(run_test())
    
    def test_order_block_detection(self):
        """Test detection of both demand and supply order blocks."""
        async def run_test():
            # Test with complete data
            data = {
                'candles': self.candles,
                'doji_data': self.doji_data,
                'fvg_data': self.fvg_data,
                'bos_data': self.bos_data
            }
            
            # Test with default indicator (requires BOS)
            result = await self.indicator.calculate(data)
            
            # We should detect both a demand and supply block
            self.assertTrue(result.has_demand_block)
            self.assertTrue(result.has_supply_block)
            self.assertEqual(len(result.demand_blocks), 1)
            self.assertEqual(len(result.supply_blocks), 1)
            
            # Verify demand block details
            demand_block = result.demand_blocks[0]
            self.assertEqual(demand_block.type, 'demand')
            self.assertEqual(demand_block.index, 0)  # Bearish doji at index 0
            
            # Verify supply block details
            supply_block = result.supply_blocks[0]
            self.assertEqual(supply_block.type, 'supply')
            self.assertEqual(supply_block.index, 1)  # Bullish doji at index 1
            
            # Verify that blocks contain complete data about components
            self.assertIsNotNone(demand_block.doji_data)
            self.assertIsNotNone(demand_block.related_fvg)
            self.assertIsNotNone(demand_block.bos_data)
            
        asyncio.run(run_test())
    
    def test_custom_params(self):
        """Test detection with custom parameters."""
        async def run_test():
            # Test with complete data
            data = {
                'candles': self.candles,
                'doji_data': self.doji_data,
                'fvg_data': self.fvg_data,
                'bos_data': self.bos_data
            }
            
            # Custom indicator doesn't require BOS
            result = await self.custom_indicator.calculate(data)
            
            # We should still detect both blocks even without BOS
            self.assertTrue(result.has_demand_block)
            self.assertTrue(result.has_supply_block)
            
        asyncio.run(run_test())
    
    def test_sequence_requirement(self):
        """Test that the sequence requirement is enforced."""
        async def run_test():
            # Create data with FVG outside the detection window (too far from doji)
            modified_fvg_data = {
                'bullish_fvgs': [
                    {
                        'type': 'bullish',
                        'candle_index': 4,  # Too far from doji at index 0 with custom window of 3
                        'top': 102.0,
                        'bottom': 104.0,
                        'size': 2.0,
                        'candle': self.candles[4].copy(),
                        'timestamp': self.candles[4]['timestamp'],
                        'filled': False
                    }
                ],
                'bearish_fvgs': self.fvg_data['bearish_fvgs']
            }
            
            data = {
                'candles': self.candles,
                'doji_data': self.doji_data,
                'fvg_data': modified_fvg_data,
                'bos_data': self.bos_data
            }
            
            # Test with custom indicator (max_detection_window = 3)
            result = await self.custom_indicator.calculate(data)
            
            # We should not detect a demand block because FVG is outside window
            self.assertFalse(result.has_demand_block)
            self.assertEqual(len(result.demand_blocks), 0)
            
            # We should still detect a supply block
            self.assertTrue(result.has_supply_block)
            
        asyncio.run(run_test())
    
    def test_requirements(self):
        """Test that requirements are correctly reported."""
        requirements = self.indicator.get_requirements()
        
        # Check basic requirements
        self.assertTrue(requirements['candles'])
        self.assertEqual(requirements['lookback_period'], 50)
        
        # Check timeframes
        timeframes = requirements['timeframes']
        self.assertIn('15m', timeframes)
        self.assertIn('1h', timeframes)
        self.assertIn('4h', timeframes)
        self.assertIn('1d', timeframes)
        
        # Check indicator dependencies
        dependencies = requirements['indicators']
        self.assertIn('structure_break', dependencies)
        self.assertIn('fvg', dependencies)
        self.assertIn('doji_candle', dependencies)


if __name__ == '__main__':
    unittest.main()