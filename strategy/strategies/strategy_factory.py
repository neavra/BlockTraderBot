# strategy/strategies/strategy_factory.py
from typing import Dict, Any, Type
from strategy.indicators.base import Indicator
from strategy.strategies.base import Strategy

# Import all strategy implementations
from strategy.strategies.hidden_ob_strategy import HiddenOrderBlockStrategy

class StrategyFactory:
    """Factory for creating strategy instances by name."""
    
    def __init__(self):
        """Initialize the factory with strategy mappings."""
        self._strategies = {
            'hidden_order_block': HiddenOrderBlockStrategy,
        }
    
    def create_strategy(self, name: str, indicators: Dict[str, Indicator] = None, 
                      params: Dict[str, Any] = None) -> Strategy:
        """
        Create a strategy instance by name.
        
        Args:
            name: Name of the strategy to create
            indicators: Dictionary of indicators to use
            params: Parameters to pass to the strategy constructor
            
        Returns:
            Instantiated strategy
            
        Raises:
            ValueError: If strategy name is unknown
        """
        if name not in self._strategies:
            raise ValueError(f"Unknown strategy: {name}")
        
        strategy_class = self._strategies[name]
        return strategy_class(indicators=indicators, params=params)
    
    def register_strategy(self, name: str, strategy_class: Type[Strategy]):
        """Register a new strategy class."""
        self._strategies[name] = strategy_class