import logging
from typing import Dict, List, Any, Set, Optional
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.models.market_context import MarketContext
from strategy.indicators.base import Indicator
from strategy.domain.types.indicator_type_enum import IndicatorType
from datetime import datetime

logger = logging.getLogger(__name__)

class IndicatorDAG:
    """
    Manages indicator dependencies and execution order using a Directed Acyclic Graph.
    Ensures indicators are executed in the correct order based on their dependencies.
    """

    def __init__(self):
        """Initialize the indicator DAG."""
        self.indicators: Dict[IndicatorType, Indicator] = {}
        self.dependencies: Dict[IndicatorType, List[IndicatorType]] = {}
        self.execution_order: List[IndicatorType] = []

    def register_indicator(self, name: IndicatorType, indicator_instance: Indicator, dependencies: Optional[List[IndicatorType]] = None):
        """
        Register an indicator with its dependencies.

        Args:
            name: Name of the indicator
            indicator_instance: The indicator instance
            dependencies: List of indicator names this indicator depends on
        """
        self.indicators[name] = indicator_instance
        self.dependencies[name] = dependencies or []
        self.execution_order = []
        logger.debug(f"Registered indicator '{name}' with dependencies: {dependencies}")

    def compute_execution_order(self) -> List[IndicatorType]:
        """
        Compute the optimal execution order using topological sort.

        Returns:
            List of indicator names in execution order
        """
        if self.execution_order:
            return self.execution_order

        visited = set()
        temp_mark = set()
        order = []

        def visit(node: IndicatorType):
            if node in temp_mark:
                raise ValueError(f"Circular dependency detected involving {node}")
            if node not in visited:
                temp_mark.add(node)
                for dep in self.dependencies.get(node, []):
                    if dep not in self.indicators:
                        logger.warning(f"Dependency '{dep}' for indicator '{node}' not found")
                        continue
                    visit(dep)
                temp_mark.remove(node)
                visited.add(node)
                order.append(node)

        for node in self.indicators:
            if node not in visited:
                visit(node)

        self.execution_order = list(order)
        logger.info(f"Computed indicator execution order: {[e for e in self.execution_order]}")
        return self.execution_order

    async def run_indicators(self, candle_data: List[CandleDto], market_contexts: List[MarketContext], requested_indicators: Optional[List[IndicatorType]] = None) -> Dict[str, Any]:
        """
        Run indicators in optimal order.

        Args:
            candle_data: Input market data
            market_contexts: list of market contexts according to time frame hierarchy
            requested_indicators: Specific indicators to run (if None, runs all)

        Returns:
            Dictionary of indicator results
        """
        execution_order = self.compute_execution_order()
        results = {}

        if requested_indicators is not None:
            required = set(requested_indicators)
            for indicator in requested_indicators:
                deps_to_process = list(self.dependencies.get(indicator, []))
                while deps_to_process:
                    dep = deps_to_process.pop(0)
                    if dep not in required:
                        required.add(dep)
                        deps_to_process.extend(self.dependencies.get(dep, []))
            execution_order = [ind for ind in execution_order if ind in required]

        data = self.build_data_dictionary(candle_data, market_contexts)
        for indicator_name in execution_order:
            try:
                indicator = self.indicators[indicator_name]
                indicator_result = await indicator.calculate(data)

                results[indicator_name.value] = indicator_result
                data[f"{indicator_name.value}_data"] = indicator_result

                logger.debug(f"Executed indicator '{indicator_name}'")
            except Exception as e:
                logger.error(f"Error executing indicator '{indicator_name}': {e}", exc_info=True)
                results[indicator_name.value] = {"error": str(e)}
                data[f"{indicator_name.value}_data"] = {"error": str(e)}

        results["market_contexts"] = market_contexts
        results["current_price"] = candle_data[-1].close
        return results

    def build_data_dictionary(
        self,
        candle_data: List[CandleDto],
        market_contexts: List[MarketContext],
    ) -> Dict[str, Any]:
        """
        Build a properly formatted data dictionary for indicator calculations.

        Args:
            "symbol": symbol,
            "timeframe": timeframe,
            "exchange": exchange,
            candle_data: List of candle data objects
            market_contexts: List of market context objects for multiple timeframes

        Returns:
            Dictionary containing all necessary data for indicator calculations
        """
        first_candle = candle_data[0]
        last_candle = candle_data[-1]

        symbol = first_candle.symbol
        timeframe = first_candle.timeframe
        exchange = first_candle.exchange
        current_price = last_candle.close

        data_dict = {
            "candles": candle_data,
            "market_contexts": market_contexts,
            "symbol": symbol,
            "timeframe": timeframe,
            "exchange": exchange,
            "current_price": current_price,
            "timestamp": datetime.now().isoformat()
        }

        return data_dict
