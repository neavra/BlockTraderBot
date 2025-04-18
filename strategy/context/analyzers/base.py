from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseAnalyzer(ABC):
    """Base class for all market analyzers"""
    
    @abstractmethod
    def analyze(self, candles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Perform analysis on candle data
        
        Args:
            candles: List of candle data
            
        Returns:
            Analysis results as a dictionary
        """
        pass
    
    @abstractmethod
    def update_market_context(self, context, candles: List[Dict[str, Any]]):
        """
        Update market context with analysis results
        
        Args:
            context: MarketContext object to update
            candles: List of candle data
            
        Returns:
            Updated MarketContext
        """
        pass
