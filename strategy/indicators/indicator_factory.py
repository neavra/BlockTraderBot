from typing import Dict, Any, Type
from strategy.indicators.base import Indicator
from strategy.strategies.base import Strategy

# Import all indicator implementations
from strategy.indicators.composite_indicators.order_block import OrderBlockIndicator
from strategy.indicators.composite_indicators.hidden_ob import HiddenOrderBlockIndicator
from strategy.indicators.fvg import FVGIndicator
from strategy.indicators.doji_candle import DojiCandleIndicator
from strategy.indicators.bos import StructureBreakIndicator
from strategy.domain.types.indicator_type_enum import IndicatorType

class IndicatorFactory:
    """Factory for creating indicator instances by name."""
    
    def __init__(self):
        """Initialize the factory with indicator mappings."""
        self._indicators: Dict[IndicatorType, Type[Indicator]] = {
            IndicatorType.ORDER_BLOCK: OrderBlockIndicator,
            IndicatorType.FVG: FVGIndicator,
            IndicatorType.STRUCTURE_BREAK: StructureBreakIndicator,
            IndicatorType.DOJI_CANDLE: DojiCandleIndicator,
            IndicatorType.HIDDEN_ORDER_BLOCK: HiddenOrderBlockIndicator,
        }
    
    def create_indicator(self, name: IndicatorType, params: Dict[str, Any] = None) -> Indicator:
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
    
    def register_indicator(self, name: IndicatorType, indicator_class: Type[Indicator]):
        """Register a new indicator class."""
        self._indicators[name] = indicator_class