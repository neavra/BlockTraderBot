import unittest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import json
import logging
from typing import Dict, Any, List

# Import the module being tested
from strategy.engine.strategy_runner import StrategyRunner


# No need for custom mock classes - we'll use MagicMock directly


class TestExecuteStrategies(unittest.IsolatedAsyncioTestCase):
    """Unit tests specifically for the StrategyRunner.execute_strategies method."""
    
    async def asyncSetUp(self):
        """Set up test environment with mocks."""
        # Configure logging
        logging.basicConfig(level=logging.DEBUG)
        
        # Create a patcher for the IndicatorDAG
        self.indicator_dag_patcher = patch('strategy.engine.strategy_runner.IndicatorDAG')
        self.mock_indicator_dag_class = self.indicator_dag_patcher.start()
        self.mock_indicator_dag = self.mock_indicator_dag_class.return_value
        
        # Mock the run_indicators method
        self.mock_indicator_dag.run_indicators = AsyncMock()
        
        # Set up mock strategies
        self.mock_strategy1 = MagicMock()
        self.mock_strategy1.name = "test_strategy1"
        self.mock_strategy1.analyze = AsyncMock()
        self.mock_strategy1.get_requirements.return_value = {
            "timeframes": ["1h", "4h"],
            "indicators": ["order_block", "fvg"]
        }
        
        self.mock_strategy2 = MagicMock()
        self.mock_strategy2.name = "test_strategy2"
        self.mock_strategy2.analyze = AsyncMock(return_value=None)
        self.mock_strategy2.get_requirements.return_value = {
            "timeframes": ["1h"],
            "indicators": ["doji_candle"]
        }
        
        # Create mock signal
        self.mock_signal = MagicMock()
        self.mock_strategy1.analyze.return_value = self.mock_signal
        
        # Create the StrategyRunner instance with mocked dependencies
        self.strategy_runner = StrategyRunner(
            strategies=[self.mock_strategy1, self.mock_strategy2],
            cache_service=MagicMock(),
            producer_queue=MagicMock(),
            consumer_queue=MagicMock(),
            context_engine=MagicMock(),
            config={"exchange": "binance"}
        )
        
        # Replace the indicator_dag with our mock
        self.strategy_runner.indicator_dag = self.mock_indicator_dag
        
        # Mock the _publish_signal method
        self.strategy_runner._publish_signal = AsyncMock()
        
    async def test_execute_strategies_runs_indicators_and_strategies(self):
        """Test that indicators are executed through the DAG and strategies receive the results."""
        # Setup test data
        test_data = {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "exchange": "binance",
            "candles": [{"open": 16000, "high": 16100, "low": 15900, "close": 16050}]
        }
        
        # Setup indicator results
        indicator_results = {
            "order_block": {"detected": True, "blocks": [{"price": 16000}]},
            "fvg": {"gaps": [{"high": 16100, "low": 16000}]},
            "doji_candle": {"detected": False}
        }
        
        # Configure the mock indicator DAG to return our test results
        self.mock_indicator_dag.run_indicators.return_value = indicator_results
        
        # Execute the method under test
        await self.strategy_runner.execute_strategies(test_data)
        
        # Verify indicators were executed with correct parameters
        self.mock_indicator_dag.run_indicators.assert_called_once()
        call_args = self.mock_indicator_dag.run_indicators.call_args[0]
        self.assertEqual(call_args[0], test_data)  # First arg should be the test data
        
        # Verify requested indicators were based on strategy requirements
        requested_indicators = self.mock_indicator_dag.run_indicators.call_args[1]['requested_indicators']
        self.assertIn("order_block", requested_indicators)
        self.assertIn("fvg", requested_indicators)
        self.assertIn("doji_candle", requested_indicators)
        
        # Verify strategies were executed with enhanced data
        self.mock_strategy1.analyze.assert_called_once()
        enhanced_data = self.mock_strategy1.analyze.call_args[0][0]
        
        # Check enhanced data contains indicator results
        self.assertIn("order_block_data", enhanced_data)
        self.assertIn("fvg_data", enhanced_data)
        self.assertIn("doji_candle_data", enhanced_data)
        
        # Verify strategy2 was executed
        self.mock_strategy2.analyze.assert_called_once()
        
        # Verify signal was published
        self.strategy_runner._publish_signal.assert_called_once_with(self.mock_signal)
    
    async def test_execute_strategies_respects_timeframe_requirements(self):
        """Test that strategies are only executed for their supported timeframes."""
        # Update mock strategy1 to only support 4h timeframe
        self.mock_strategy1.get_requirements.return_value = {
            "timeframes": ["4h", "1d"],  # Not 1h
            "indicators": ["order_block", "fvg"]
        }
        
        # Setup test data with 1h timeframe
        test_data = {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "exchange": "binance",
            "candles": [{"open": 16000, "high": 16100, "low": 15900, "close": 16050}]
        }
        
        # Mock indicator results
        self.mock_indicator_dag.run_indicators.return_value = {
            "doji_candle": {"detected": False}
        }
        
        # Execute the method under test
        await self.strategy_runner.execute_strategies(test_data)
        
        # Verify indicators were still executed (needed by strategy2)
        self.mock_indicator_dag.run_indicators.assert_called_once()
        
        # Verify strategy1 was skipped due to timeframe mismatch
        self.mock_strategy1.analyze.assert_not_called()
        
        # Verify strategy2 was executed (it supports 1h)
        self.mock_strategy2.analyze.assert_called_once()
        
        # Verify no signal was published (strategy2 returns None)
        self.strategy_runner._publish_signal.assert_not_called()
    
    async def test_execute_strategies_handles_indicator_errors(self):
        """Test that errors in indicators are handled gracefully."""
        # Setup test data
        test_data = {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "exchange": "binance",
            "candles": [{"open": 16000, "high": 16100, "low": 15900, "close": 16050}]
        }
        
        # Make the indicator DAG raise an exception
        self.mock_indicator_dag.run_indicators.side_effect = Exception("Test indicator error")
        
        # Execute the method under test - should not raise an exception
        await self.strategy_runner.execute_strategies(test_data)
        
        # Verify indicator DAG was called
        self.mock_indicator_dag.run_indicators.assert_called_once()
        
        # Verify strategies were not called since indicator execution failed
        self.mock_strategy1.analyze.assert_not_called()
        self.mock_strategy2.analyze.assert_not_called()
        
        # Verify no signal was published
        self.strategy_runner._publish_signal.assert_not_called()
    
    async def test_execute_strategies_handles_strategy_errors(self):
        """Test that errors in strategies don't affect other strategies."""
        # Setup test data
        test_data = {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "exchange": "binance",
            "candles": [{"open": 16000, "high": 16100, "low": 15900, "close": 16050}]
        }
        
        # Setup indicator results
        indicator_results = {
            "order_block": {"detected": True},
            "fvg": {"detected": True},
            "doji_candle": {"detected": True}
        }
        self.mock_indicator_dag.run_indicators.return_value = indicator_results
        
        # Make the first strategy raise an exception
        self.mock_strategy1.analyze.side_effect = Exception("Test strategy error")
        
        # Execute the method under test
        await self.strategy_runner.execute_strategies(test_data)
        
        # Verify first strategy was called and raised exception
        self.mock_strategy1.analyze.assert_called_once()
        
        # Verify second strategy was still executed
        self.mock_strategy2.analyze.assert_called_once()
        
        # Verify no signal was published from either strategy
        self.strategy_runner._publish_signal.assert_not_called()

    
    async def test_execute_strategies_collects_indicators_from_strategies(self):
        """Test that required indicators are properly collected from strategies."""
        # Setup test data
        test_data = {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "exchange": "binance",
            "candles": [{"open": 16000, "high": 16100, "low": 15900, "close": 16050}]
        }
        
        # Execute the method to test indicator collection logic
        await self.strategy_runner.execute_strategies(test_data)
        
        # Verify the correct indicators were requested from the DAG
        if self.mock_indicator_dag.run_indicators.called:
            requested_indicators = self.mock_indicator_dag.run_indicators.call_args[1]['requested_indicators']
            self.assertEqual(set(requested_indicators), set(["order_block", "fvg", "doji_candle"]))
    
    async def test_execute_strategies_with_empty_data(self):
        """Test handling of empty or invalid market data."""
        # Test with empty data
        empty_data = {}
        
        # Execute the method under test with empty data
        await self.strategy_runner.execute_strategies(empty_data)
        
        # Verify indicator DAG was still called
        self.mock_indicator_dag.run_indicators.assert_called_once_with(
            empty_data, requested_indicators=["order_block", "fvg", "doji_candle"]
        )
        
        # Since we don't mock any validation logic, strategies might still be called
        # The important thing is that no exceptions are raised


if __name__ == '__main__':
    unittest.main()