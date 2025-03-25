# strategy/indicators/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, List

class Indicator(ABC):
    """Base class for all indicators"""
    
    def __init__(self, params: Dict[str, Any] = None):
        """Initialize the indicator with parameters"""
        self.params = params or {}
        self.name = self.__class__.__name__
        
    @abstractmethod
    async def calculate(self, data: Dict[str, Any]) -> Union[bool, float, Dict[str, Any]]:
        """
        Calculate the indicator value based on the provided data
        
        Args:
            data: Dictionary containing market data (OHLCV, etc.)
            
        Returns:
            Indicator result: boolean, float, or dictionary with detailed results
        """
        pass
    
    @abstractmethod
    def get_requirements(self) -> Dict[str, Any]:
        """
        Returns data requirements for this indicator
        
        Returns:
            Dictionary specifying required data (timeframes, length, etc.)
        """
        pass