from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod

class ExchangeInterface(ABC):
    """Base interface that all exchange adapters must implement"""
    
    @abstractmethod
    async def initialize(self) -> bool:
        """Initialize the exchange connection"""
        pass
    
    @abstractmethod
    async def create_order(self, 
                          symbol: str, 
                          order_type: str, 
                          side: str, 
                          amount: float, 
                          price: Optional[float] = None, 
                          params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create an order on the exchange"""
        pass
    
    # @abstractmethod
    # async def fetch_order(self, 
    #                      id: str, 
    #                      symbol: Optional[str] = None, 
    #                      params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    #     """Fetch an order's status"""
    #     pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the exchange connection"""
        pass