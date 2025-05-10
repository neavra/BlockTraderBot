from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, TypeVar, Generic, List, Tuple
from strategy.domain.dto.indicator_result_dto import IndicatorResultDto
from data.database.repository.base_repository import BaseRepository

class Indicator(ABC):
    """Base class for all indicators"""
    
    def __init__(self, repository: BaseRepository, params: Dict[str, Any] = None):
        """Initialize the indicator with parameters"""
        self.params = params or {}
        self.repository = repository
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

    @abstractmethod
    async def process_existing_indicators(self, indicators: List[Any], candles: List[Any]) -> Tuple[List[Any], List[Any]]:
        """
        Process existing indicator instances for updates or mitigation
        
        Args:
            instances: List of existing indicator instances from database
            candles: Recent candles to check for mitigation or updates
            
        Returns:
            Tuple of (updated_instances, valid_instances)
        """
        pass
        
    def get_relevant_price_range(self, candles: List[Any]) -> Tuple[float, float]:
        """
        Get the relevant price range from the provided candles
        Used to fetch only relevant instances from database
        
        Args:
            candles: List of recent candles
            
        Returns:
            Tuple of (min_price, max_price) to search for instances
        """
        if not candles:
            return (0, 0)
            
        # Find the highest high and lowest low in the candle set
        highest = max(candle.high for candle in candles)
        lowest = min(candle.low for candle in candles)
        
        # Add a buffer for border cases (5% buffer)
        buffer = (highest - lowest) * 0.05
        
        return (lowest - buffer, highest + buffer)