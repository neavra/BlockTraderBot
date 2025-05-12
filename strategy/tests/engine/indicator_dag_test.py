import unittest
from unittest.mock import Mock, patch, AsyncMock, MagicMock, call
import asyncio
from datetime import datetime, timezone
import json

from strategy.engine.indicator_dag import IndicatorDAG
from strategy.domain.types.indicator_type_enum import IndicatorType
from strategy.indicators.base import Indicator
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.models.market_context import MarketContext


class TestIndicatorDAG(unittest.IsolatedAsyncioTestCase):
    """Test suite for IndicatorDAG class."""
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test method."""
        # Create mock indicators
        self.mock_doji_indicator = AsyncMock(spec=Indicator)
        self.mock_fvg_indicator = AsyncMock(spec=Indicator)
        self.mock_bos_indicator = AsyncMock(spec=Indicator)
        self.mock_order_block_indicator = AsyncMock(spec=Indicator)
        
        # Set up their requirements
        self.mock_doji_indicator.get_requirements.return_value = {
            'indicators': []  # Doji has no dependencies
        }
        self.mock_fvg_indicator.get_requirements.return_value = {
            'indicators': []  # FVG has no dependencies
        }
        self.mock_bos_indicator.get_requirements.return_value = {
            'indicators': []  # BOS has no dependencies (but uses market context)
        }
        self.mock_order_block_indicator.get_requirements.return_value = {
            'indicators': [
                IndicatorType.DOJI_CANDLE,
                IndicatorType.FVG,
                IndicatorType.STRUCTURE_BREAK
            ]  # Order Block depends on all three
        }
        
        # Create the DAG
        self.dag = IndicatorDAG()
        
        # Set up mock calculate methods
        self.mock_doji_indicator.calculate = AsyncMock(return_value={"dojis": [{"type": "doji", "strength": 0.8}]})
        self.mock_fvg_indicator.calculate = AsyncMock(return_value={"bullish_fvgs": [], "bearish_fvgs": []})
        self.mock_bos_indicator.calculate = AsyncMock(return_value={"bullish_breaks": [], "bearish_breaks": []})
        self.mock_order_block_indicator.calculate = AsyncMock(return_value={"demand_blocks": [], "supply_blocks": []})
        
    def test_register_indicator(self):
        """Test registering indicators."""
        # Register the indicators
        self.dag.register_indicator(IndicatorType.DOJI_CANDLE, self.mock_doji_indicator)
        self.dag.register_indicator(IndicatorType.FVG, self.mock_fvg_indicator)
        
        # Verify they were registered
        self.assertIn(IndicatorType.DOJI_CANDLE, self.dag.indicators)
        self.assertIn(IndicatorType.FVG, self.dag.indicators)
        self.assertEqual(self.dag.indicators[IndicatorType.DOJI_CANDLE], self.mock_doji_indicator)
        self.assertEqual(self.dag.indicators[IndicatorType.FVG], self.mock_fvg_indicator)
        
        # Verify dependencies
        self.assertIn(IndicatorType.DOJI_CANDLE, self.dag.dependencies)
        self.assertIn(IndicatorType.FVG, self.dag.dependencies)
        self.assertEqual(self.dag.dependencies[IndicatorType.DOJI_CANDLE], [])
        self.assertEqual(self.dag.dependencies[IndicatorType.FVG], [])
        
    def test_compute_execution_order_simple(self):
        """Test computing execution order with no dependencies."""
        # Register indicators with no dependencies
        self.dag.register_indicator(IndicatorType.DOJI_CANDLE, self.mock_doji_indicator)
        self.dag.register_indicator(IndicatorType.FVG, self.mock_fvg_indicator)
        
        # Compute execution order
        order = self.dag.compute_execution_order()
        
        # Both should be in the order (order doesn't matter in this case)
        self.assertEqual(len(order), 2)
        self.assertIn(IndicatorType.DOJI_CANDLE, order)
        self.assertIn(IndicatorType.FVG, order)
        
    def test_compute_execution_order_with_dependencies(self):
        """Test computing execution order with dependencies."""
        # Register indicators with dependencies
        self.dag.register_indicator(IndicatorType.DOJI_CANDLE, self.mock_doji_indicator)
        self.dag.register_indicator(IndicatorType.FVG, self.mock_fvg_indicator)
        self.dag.register_indicator(IndicatorType.STRUCTURE_BREAK, self.mock_bos_indicator)
        self.dag.register_indicator(IndicatorType.ORDER_BLOCK, self.mock_order_block_indicator)
        
        # Compute execution order
        order = self.dag.compute_execution_order()
        
        # Order block should come last
        self.assertEqual(len(order), 4)
        self.assertEqual(order[-1], IndicatorType.ORDER_BLOCK)
        
        # All dependencies should come before order block
        order_block_index = order.index(IndicatorType.ORDER_BLOCK)
        doji_index = order.index(IndicatorType.DOJI_CANDLE)
        fvg_index = order.index(IndicatorType.FVG)
        bos_index = order.index(IndicatorType.STRUCTURE_BREAK)
        
        self.assertLess(doji_index, order_block_index)
        self.assertLess(fvg_index, order_block_index)
        self.assertLess(bos_index, order_block_index)
        
    def test_circular_dependency_detection(self):
        """Test detection of circular dependencies using existing indicator types."""
        # Create a circular dependency between ORDER_BLOCK and FVG
        # The key is that ORDER_BLOCK depends on FVG, and we'll make FVG depend on ORDER_BLOCK
        
        # Clear any previous registrations
        self.dag = IndicatorDAG()
        
        # First register ORDER_BLOCK normally
        order_block = AsyncMock(spec=Indicator)
        fvg = AsyncMock(spec=Indicator)
        
        self.dag.register_indicator(IndicatorType.ORDER_BLOCK, order_block, dependencies=[IndicatorType.FVG])
        self.dag.register_indicator(IndicatorType.FVG, fvg, dependencies=[IndicatorType.ORDER_BLOCK])

        # Computing execution order should raise ValueError due to circular dependency
        with self.assertRaises(ValueError):
            self.dag.compute_execution_order()
            
    async def test_run_indicators_simple(self):
        """Test running indicators with no dependencies."""
        # Register indicators with no dependencies
        self.dag.register_indicator(IndicatorType.DOJI_CANDLE, self.mock_doji_indicator)
        self.dag.register_indicator(IndicatorType.FVG, self.mock_fvg_indicator)
        
        # Create test candle data
        candles = [
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime.now(timezone.utc),
                open=40000.0,
                high=41000.0,
                low=39500.0,
                close=40500.0,
                volume=100.0,
                is_closed=True
            )
        ]
        
        # Create mock market context
        market_context = MarketContext(
            symbol="BTCUSDT",
            timeframe="1h",
            exchange="binance"
        )
        
        # Run indicators
        results = await self.dag.run_indicators(candles, [market_context])
        
        # Check that both indicators were run
        self.mock_doji_indicator.calculate.assert_called_once()
        self.mock_fvg_indicator.calculate.assert_called_once()
        
        # Check results
        self.assertIn('doji_candle', results)
        self.assertIn('fvg', results)
        
    async def test_run_indicators_with_dependencies_full(self):
        """Test running all indicators including OrderBlock with all its dependencies."""
        # Register all indicators including OrderBlock
        self.dag.register_indicator(IndicatorType.DOJI_CANDLE, self.mock_doji_indicator)
        self.dag.register_indicator(IndicatorType.FVG, self.mock_fvg_indicator)
        self.dag.register_indicator(IndicatorType.STRUCTURE_BREAK, self.mock_bos_indicator)
        self.dag.register_indicator(IndicatorType.ORDER_BLOCK, self.mock_order_block_indicator)
        
        # Create test candle data
        candles = [
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime.now(timezone.utc),
                open=40000.0,
                high=41000.0,
                low=39500.0,
                close=40500.0,
                volume=100.0,
                is_closed=True
            ),
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime.now(timezone.utc),
                open=40500.0,
                high=42000.0,
                low=40000.0,
                close=41500.0,
                volume=120.0,
                is_closed=True
            ),
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime.now(timezone.utc),
                open=41500.0,
                high=43000.0,
                low=41000.0,
                close=42500.0,
                volume=150.0,
                is_closed=True
            )
        ]
        
        # Create mock market context with swing points
        market_context = MarketContext(
            symbol="BTCUSDT",
            timeframe="1h",
            exchange="binance"
        )
        # Add swing points
        market_context.set_swing_high({'price': 43000.0, 'index': 2, 'timestamp': datetime.now(timezone.utc)})
        market_context.set_swing_low({'price': 39500.0, 'index': 0, 'timestamp': datetime.now(timezone.utc)})
        
        # Define expected data dictionary for each indicator
        expected_data_dict = {
            'candles': candles,
            'market_contexts': [market_context],
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'exchange': 'binance',
            'current_price': 42500.0,
            'timestamp': unittest.mock.ANY  # We don't care about the exact timestamp
        }
        
        # Run indicators
        results = await self.dag.run_indicators(candles, [market_context])
        
        # Check that all indicators were run in the correct order
        self.mock_doji_indicator.calculate.assert_called_once()
        self.mock_fvg_indicator.calculate.assert_called_once()
        self.mock_bos_indicator.calculate.assert_called_once()
        self.mock_order_block_indicator.calculate.assert_called_once()
        
        # Check that OrderBlock was called with data that includes results from dependencies
        order_block_call_args = self.mock_order_block_indicator.calculate.call_args[0][0]
        
        # Verify that OrderBlock received data with dependency results
        self.assertIn('doji_candle_data', order_block_call_args)
        self.assertIn('fvg_data', order_block_call_args)
        self.assertIn('structure_break_data', order_block_call_args)
        
        # Make sure the candles and market contexts were passed correctly
        for key in ['candles', 'market_contexts', 'symbol', 'timeframe', 'exchange']:
            self.assertIn(key, order_block_call_args)
        
        # Check results
        self.assertIn('doji_candle', results)
        self.assertIn('fvg', results)
        self.assertIn('structure_break', results)
        self.assertIn('order_block', results)
        
    async def test_run_indicators_with_subset_requested(self):
        """Test running only a subset of indicators."""
        # Register all indicators
        self.dag.register_indicator(IndicatorType.DOJI_CANDLE, self.mock_doji_indicator)
        self.dag.register_indicator(IndicatorType.FVG, self.mock_fvg_indicator)
        self.dag.register_indicator(IndicatorType.STRUCTURE_BREAK, self.mock_bos_indicator)
        self.dag.register_indicator(IndicatorType.ORDER_BLOCK, self.mock_order_block_indicator)
        
        # Create test candle data
        candles = [
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime.now(timezone.utc),
                open=40000.0,
                high=41000.0,
                low=39500.0,
                close=40500.0,
                volume=100.0,
                is_closed=True
            )
        ]
        
        # Create mock market context
        market_context = MarketContext(
            symbol="BTCUSDT",
            timeframe="1h",
            exchange="binance"
        )
        
        # Run only the FVG indicator
        results = await self.dag.run_indicators(candles, [market_context], requested_indicators=[IndicatorType.FVG])
        
        # Check that only FVG was run
        self.mock_doji_indicator.calculate.assert_not_called()
        self.mock_fvg_indicator.calculate.assert_called_once()
        self.mock_bos_indicator.calculate.assert_not_called()
        self.mock_order_block_indicator.calculate.assert_not_called()
        
        # Check results only contain FVG
        self.assertNotIn('doji_candle', results)
        self.assertIn('fvg', results)
        self.assertNotIn('structure_break', results)
        self.assertNotIn('order_block', results)
        
    async def test_run_indicators_with_dependency_error(self):
        """Test running indicators when a dependency fails."""
        # Register all indicators
        self.dag.register_indicator(IndicatorType.DOJI_CANDLE, self.mock_doji_indicator)
        self.dag.register_indicator(IndicatorType.FVG, self.mock_fvg_indicator)
        self.dag.register_indicator(IndicatorType.STRUCTURE_BREAK, self.mock_bos_indicator)
        self.dag.register_indicator(IndicatorType.ORDER_BLOCK, self.mock_order_block_indicator)
        
        # Make FVG indicator fail
        self.mock_fvg_indicator.calculate.side_effect = Exception("FVG calculation failed")
        
        # Create test candle data
        candles = [
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime.now(timezone.utc),
                open=40000.0,
                high=41000.0,
                low=39500.0,
                close=40500.0,
                volume=100.0,
                is_closed=True
            )
        ]
        
        # Create mock market context
        market_context = MarketContext(
            symbol="BTCUSDT",
            timeframe="1h",
            exchange="binance"
        )
        
        # Run indicators
        results = await self.dag.run_indicators(candles, [market_context])
        
        # Check that all indicators were called, even though FVG failed
        self.mock_doji_indicator.calculate.assert_called_once()
        self.mock_fvg_indicator.calculate.assert_called_once()
        self.mock_bos_indicator.calculate.assert_called_once()
        self.mock_order_block_indicator.calculate.assert_called_once()
        
        # Check results
        self.assertIn('doji_candle', results)
        self.assertIn('fvg', results)
        self.assertIn('structure_break', results)
        self.assertIn('order_block', results)
        
        # Check that FVG result contains the error
        self.assertIn('error', results['fvg'])
        
    async def test_build_data_dictionary(self):
        """Test building the initial data dictionary."""
        # Create test candle data
        candles = [
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime.now(timezone.utc),
                open=40000.0,
                high=41000.0,
                low=39500.0,
                close=40500.0,
                volume=100.0,
                is_closed=True
            ),
            CandleDto(
                symbol="BTCUSDT",
                exchange="binance",
                timeframe="1h",
                timestamp=datetime.now(timezone.utc),
                open=40500.0,
                high=42000.0,
                low=40000.0,
                close=41500.0,
                volume=120.0,
                is_closed=True
            )
        ]
        
        # Create mock market context
        market_context = MarketContext(
            symbol="BTCUSDT",
            timeframe="1h",
            exchange="binance"
        )
        
        # Build data dictionary
        data_dict = self.dag.build_data_dictionary(candles, [market_context])
        
        # Check data dictionary
        self.assertEqual(data_dict['symbol'], "BTCUSDT")
        self.assertEqual(data_dict['timeframe'], "1h")
        self.assertEqual(data_dict['exchange'], "binance")
        self.assertEqual(data_dict['current_price'], 41500.0)  # Last candle's close
        self.assertEqual(data_dict['candles'], candles)
        self.assertEqual(data_dict['market_contexts'], [market_context])
        self.assertIn('timestamp', data_dict)


if __name__ == '__main__':
    unittest.main()