import asyncio
import unittest
from typing import Dict, Any

# Import the IndicatorDAG class
from strategy.engine.indicator_dag import IndicatorDAG

# Import actual indicator implementations
from strategy.indicators.fvg import FVGIndicator
from strategy.indicators.doji_candle import DojiCandleIndicator
from strategy.indicators.bos import StructureBreakIndicator
from strategy.indicators.composite_indicators.order_block import OrderBlockIndicator
from strategy.indicators.composite_indicators.hidden_ob import HiddenOrderBlockIndicator

class TestIndicatorDAG(unittest.TestCase):
    def setUp(self):
        # Create the DAG
        self.dag = IndicatorDAG()
        
        # Create actual indicator instances
        self.fvg = FVGIndicator()
        self.structure_break = StructureBreakIndicator()
        self.doji_candle = DojiCandleIndicator()
        self.orderblock = OrderBlockIndicator()
        self.hidden_orderblock = HiddenOrderBlockIndicator()
        
        # Register indicators with their dependencies
        # The dependencies come from each indicator's get_requirements() method
        self.dag.register_indicator("fvg", self.fvg, [])
        self.dag.register_indicator("structure_break", self.structure_break, [])
        self.dag.register_indicator("doji_candle", self.doji_candle, [])
        self.dag.register_indicator("orderblock", self.orderblock, ["structure_break", "fvg"])
        self.dag.register_indicator("hidden_orderblock", self.hidden_orderblock, ["orderblock", "fvg"])
    
    def test_compute_execution_order(self):
        # Compute the execution order
        order = self.dag.compute_execution_order()
        
        # Print the execution order for inspection
        print(f"Execution order: {order}")
        
        # Check that dependencies come before dependents
        self.assertLess(order.index("fvg"), order.index("orderblock"))
        self.assertLess(order.index("structure_break"), order.index("orderblock"))
        self.assertLess(order.index("orderblock"), order.index("hidden_orderblock"))
        
        # Optional: Check if doji_candle is included (depends on your actual requirements)
        self.assertIn("doji_candle", order)
    
    def test_auto_discover_dependencies(self):
        """Test automatic discovery of dependencies from indicators' get_requirements"""
        # Create a new DAG for this test
        dag = IndicatorDAG()
        
        # Register indicators WITHOUT manually specifying dependencies
        dag.register_indicator("fvg", self.fvg)
        dag.register_indicator("structure_break", self.structure_break)
        dag.register_indicator("doji_candle", self.doji_candle)
        dag.register_indicator("orderblock", self.orderblock)
        dag.register_indicator("hidden_orderblock", self.hidden_orderblock)
        
        # Auto-discover dependencies
        for name, indicator in dag.indicators.items():
            requirements = indicator.get_requirements()
            dependencies = requirements.get('indicators', [])
            dag.dependencies[name] = dependencies
        
        # Compute execution order with auto-discovered dependencies
        order = dag.compute_execution_order()
        print(f"Auto-discovered execution order: {order}")
        
        # Check that dependencies come before dependents
        self.assertLess(order.index("fvg"), order.index("orderblock"))
        self.assertLess(order.index("structure_break"), order.index("orderblock"))
        self.assertLess(order.index("orderblock"), order.index("hidden_orderblock"))
    
    def test_run_indicators(self):
        # Create a test to run the indicators and verify results
        async def run_test():
            # Create sample market data for testing
            data = {
                "candles": [
                    {"open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000},
                    {"open": 105, "high": 115, "low": 100, "close": 110, "volume": 1200},
                    {"open": 110, "high": 120, "low": 105, "close": 115, "volume": 1500},
                ],
                "symbol": "BTCUSDT",
                "timeframe": "1h",
                "exchange": "binance",
                "current_price": 115
            }
            
            # Run all indicators
            results = await self.dag.run_indicators(data)
            
            # Check all indicators were run
            self.assertIn("fvg", results)
            self.assertIn("structure_break", results)
            self.assertIn("doji_candle", results)
            self.assertIn("orderblock", results)
            self.assertIn("hidden_orderblock", results)
            
            # Run just hidden_orderblock (should also run dependencies)
            results = await self.dag.run_indicators(data, ["hidden_orderblock"])
            
            # Check that hidden_orderblock and dependencies were run
            self.assertIn("fvg", results)
            self.assertIn("orderblock", results)
            self.assertIn("hidden_orderblock", results)
            
            # Print results for inspection
            print(f"Results when requesting hidden_orderblock: {list(results.keys())}")
            
            return True
        
        # Run the async test
        result = asyncio.run(run_test())
        self.assertTrue(result)
    
    def test_circular_dependency_detection(self):
        # Create a new DAG with circular dependencies
        dag = IndicatorDAG()
        
        # Create test indicators with circular dependencies
        class TestIndicator:
            def __init__(self, name, dependencies=None):
                self.name = name
                self._dependencies = dependencies or []
            
            def get_requirements(self):
                return {"indicators": self._dependencies}
            
            async def calculate(self, data):
                return {"result": f"{self.name}_result"}
        
        # Create indicators with circular dependencies
        ind_a = TestIndicator("ind_a", ["ind_b"])
        ind_b = TestIndicator("ind_b", ["ind_c"])
        ind_c = TestIndicator("ind_c", ["ind_a"])
        
        # Register with circular dependencies
        dag.register_indicator("ind_a", ind_a, ["ind_b"])
        dag.register_indicator("ind_b", ind_b, ["ind_c"])
        dag.register_indicator("ind_c", ind_c, ["ind_a"])
        
        # Computing execution order should raise ValueError
        with self.assertRaises(ValueError) as context:
            dag.compute_execution_order()
        
        # Check error message contains "Circular dependency"
        self.assertIn("Circular dependency", str(context.exception))

if __name__ == "__main__":
    unittest.main()