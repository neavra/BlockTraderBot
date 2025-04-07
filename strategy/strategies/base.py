from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, float

from strategy.indicators.base import Indicator
from shared.domain.dto.signal_dto import Signal

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
    
    @abstractmethod
    def calculate_signal_strength(self, signal_data: Dict[str, Any]) -> float:
        """
        Calculate the strength of a generated signal
        
        Args:
            signal_data: Dictionary containing signal parameters and metrics
            
        Returns:
            Signal strength as a float value between 0.0 and 1.0,
            where 0.0 represents lowest strength and 1.0 represents highest strength
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