# strategy/strategies/base.py
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

from strategy.indicators.base import Indicator
from strategy.signals.model import Signal

class Strategy(ABC):
    """
    Base class for all trading strategies
    """
    
    def __init__(self, name: str, indicators: Dict[str, Indicator], params: Dict[str, Any] = None):
        """
        Initialize the strategy
        
        Args:
            name: Strategy name
            indicators: Dictionary of indicators used by this strategy
            params: Strategy parameters
        """
        self.name = name
        self.indicators = indicators
        self.params = params or {}
        
    @abstractmethod
    async def analyze(self, data: Dict[str, Any]) -> Optional[Signal]:
        """
        Analyze market data and generate signals if conditions are met
        
        Args:
            data: Market data dictionary
            
        Returns:
            Signal object if a trading signal is generated, None otherwise
        """
        pass
    
    def get_requirements(self) -> Dict[str, Any]:
        """
        Get combined data requirements from all indicators
        
        Returns:
            Dictionary with combined requirements
        """
        requirements = {}
        for indicator in self.indicators.values():
            ind_req = indicator.get_requirements()
            # Merge requirements
            for key, value in ind_req.items():
                if key in requirements:
                    # Handle lookback_period: take the maximum
                    if key == 'lookback_period':
                        requirements[key] = max(requirements[key], value)
                    # Handle timeframes: merge unique values
                    elif key == 'timeframes' and isinstance(value, list):
                        requirements[key] = list(set(requirements[key] + value))
                    # For other keys, just overwrite (last wins)
                    else:
                        requirements[key] = value
                else:
                    requirements[key] = value
        
        return requirements