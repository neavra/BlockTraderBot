from typing import Dict, Any, Type
from strategy.indicators.base import Indicator
from strategy.strategies.base import Strategy

# Import all indicator implementations
from strategy.composite_indicators.order_block import OrderBlockIndicator
from strategy.indicators.fvg import FVGIndicator
from strategy.indicators.doji_candle import DojiCandleIndicator
# from strategy.indicators.structure import StructureBreakIndicator

class IndicatorFactory:
    """Factory for creating indicator instances by name."""
    
    def __init__(self):
        """Initialize the factory with indicator mappings."""
        self._indicators = {
            'orderblock': OrderBlockIndicator,
            'fvg': FVGIndicator,
            'doji': DojiCandleIndicator,
            # 'structure_break': StructureBreakIndicator,
        }
    
    def create_indicator(self, name: str, params: Dict[str, Any] = None) -> Indicator:
        """
        Create an indicator instance by name.
        
        Args:
            name: Name of the indicator to create
            params: Parameters to pass to the indicator constructor
            
        Returns:
            Instantiated indicator
            
        Raises:
            ValueError: If indicator name is unknown
        """
        if name not in self._indicators:
            raise ValueError(f"Unknown indicator: {name}")
        
        indicator_class = self._indicators[name]
        return indicator_class(params=params)
    
    def register_indicator(self, name: str, indicator_class: Type[Indicator]):
        """Register a new indicator class."""
        self._indicators[name] = indicator_class