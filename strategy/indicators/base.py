from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, TypeVar, Generic, List
from strategy.domain.dto.indicator_result_dto import IndicatorResultDto

class Indicator(ABC):
    """Base class for all indicators"""
    
    def __init__(self, params: Dict[str, Any] = None):
        """Initialize the indicator with parameters"""
        self.params = params or {}
        self.name = self.__class__.__name__
        
    @abstractmethod
    async def calculate(self, candle_data: List[Any], dependency_data: Dict[str, Any] = None) -> IndicatorResultDto:
        """
        Calculate the indicator value based on the provided data
        
        Args:
            data: Dictionary containing market data (OHLCV, etc.)
            
        Returns:
            Typed indicator result object
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