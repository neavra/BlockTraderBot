import logging
from typing import Dict, List, Any, Set, Optional
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.models.market_context import MarketContext
from strategy.indicators.base import Indicator
from datetime import datetime

logger = logging.getLogger(__name__)

class IndicatorDAG:
    """
    Manages indicator dependencies and execution order using a Directed Acyclic Graph.
    Ensures indicators are executed in the correct order based on their dependencies.
    """
    
    def __init__(self):
        """Initialize the indicator DAG."""
        self.indicators: Dict[str,Indicator] = {}
        self.dependencies = {}
        self.execution_order = []
    
    def register_indicator(self, name: str, indicator_instance: Indicator, dependencies: Optional[List[str]] = None):
        """
        Register an indicator with its dependencies.
        
        Args:
            name: Name of the indicator
            indicator_instance: The indicator instance
            dependencies: List of indicator names this indicator depends on
        """
        self.indicators[name] = indicator_instance
        self.dependencies[name] = dependencies or []
        # Reset execution order when registration changes
        self.execution_order = []
        logger.debug(f"Registered indicator '{name}' with dependencies: {dependencies}")
    
    def compute_execution_order(self) -> List[str]:
        """
        Compute the optimal execution order using topological sort.
        
        Returns:
            List of indicator names in execution order
        """
        if self.execution_order:
            return self.execution_order
            
        # Initialize variables for topological sort
        visited = set()
        temp_mark = set()
        order = []
        
        def visit(node):
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
        
        # Visit all nodes
        for node in self.indicators:
            if node not in visited:
                visit(node)
        
        # Reverse for correct execution order
        self.execution_order = list(order)
        logger.info(f"Computed indicator execution order: {self.execution_order}")
        return self.execution_order
    
    async def run_indicators(self, candle_data: List[CandleDto], market_contexts: List[MarketContext], requested_indicators: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run indicators in optimal order.
        
        Args:
            candle_data: Input market data
            market_contexts: list of market contexts according to time frame hierarchy
            requested_indicators: Specific indicators to run (if None, runs all)
            
        Returns:
            Dictionary of indicator results
        """
        # Compute execution order if not already done
        execution_order = self.compute_execution_order()
        
        # Initialize results dictionary
        results = {}
        
        # Filter to requested indicators if specified
        if requested_indicators is not None:
            # Also include dependencies
            required = set(requested_indicators)
            for indicator in requested_indicators:
                # Add all dependencies recursively
                deps_to_process = list(self.dependencies.get(indicator, []))
                while deps_to_process:
                    dep = deps_to_process.pop(0)
                    required.add(dep)
                    deps_to_process.extend([d for d in self.dependencies.get(dep, []) if d not in required])
            
            # Filter execution order to required indicators
            execution_order = [ind for ind in execution_order if ind in required]
        """
        Input data dictionary format:
        {
            "candles": List[CandleDto],  # List of candle data objects
            "market_contexts": List[MarketContext],  # List of market context objects for multiple timeframes
            "symbol": str,  # Trading pair symbol (e.g., "BTCUSDT")
            "timeframe": str,  # Candle timeframe (e.g., "1h")
            "exchange": str,  # Exchange name (e.g., "binance")
            "current_price": float,  # Current market price (optional)
            
            # The following keys are added as indicators are executed:
            "indicator_name_data": IndicatorResultDto,  # Results from each indicator
        }
        """
        # Run indicators in order
        data = self.build_data_dictionary(candle_data, market_contexts)
        for indicator_name in execution_order:
            try:
                # Run the indicator with the continuously enriched data dictionary
                indicator = self.indicators[indicator_name]
                indicator_result = await indicator.calculate(data)
                
                # Store result in both results dict and data dict
                results[indicator_name] = indicator_result
                data[f"{indicator_name}_data"] = indicator_result
                
                logger.debug(f"Executed indicator '{indicator_name}'")
            except Exception as e:
                logger.error(f"Error executing indicator '{indicator_name}': {e}", exc_info=True)
                # Continue with other indicators despite errors
                results[indicator_name] = {"error": str(e)}
                data[f"{indicator_name}_data"] = {"error": str(e)}
        
        return results
    
    def build_data_dictionary(
        candle_data: List[CandleDto], 
        market_contexts: List[MarketContext],
    ) -> Dict[str, Any]:
        """
        Build a properly formatted data dictionary for indicator calculations.
        
        Args:
            candle_data: List of candle data objects
            market_contexts: List of market context objects for multiple timeframes
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            exchange: Exchange name
            
        Returns:
            Dictionary containing all necessary data for indicator calculations
            
        Format:
        {
            "candles": List[CandleDto],  # List of candle data objects
            "market_contexts": List[MarketContext],  # List of market context objects
            "symbol": str,  # Trading pair symbol
            "timeframe": str,  # Candle timeframe
            "exchange": str,  # Exchange name
            "current_price": float,  # Current market price (from most recent candle)
        }
        """
        
        first_candle = candle_data[0]
        last_candle = candle_data[-1]
        
        symbol = first_candle.symbol
        timeframe = first_candle.timeframe
        exchange = first_candle.exchange
            
        current_price = last_candle.close
        
        # Create the base data dictionary
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