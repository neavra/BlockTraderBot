from typing import List, Dict, Optional, Any
from abc import ABC, abstractmethod

class ExchangeInterface(ABC):
    """Base interface that all exchange adapters must implement"""

    @property
    @abstractmethod
    def id(self) -> str:
        """Exchange identifier"""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Exchange name"""
        pass
    
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

    @abstractmethod
    async def cancel_order(self, 
                          id: str, 
                          symbol: Optional[str] = None, 
                          params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Cancel an existing order
        
        Args:
            id: Order ID
            symbol: Trading pair (may be required by some exchanges)
            params: Additional exchange-specific parameters
            
        Returns:
            Dict containing cancellation details
        """
        pass

    @abstractmethod
    async def fetch_order(self, 
                         id: str, 
                         symbol: Optional[str] = None, 
                         params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Fetch an order's status
        
        Args:
            id: Order ID
            symbol: Trading pair (may be required by some exchanges)
            params: Additional exchange-specific parameters
            
        Returns:
            Dict containing order details including status
        """
        pass
    
    @abstractmethod
    async def fetch_open_orders(self, 
                               symbol: Optional[str] = None, 
                               since: Optional[int] = None, 
                               limit: Optional[int] = None, 
                               params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Fetch all open orders
        
        Args:
            symbol: Trading pair to filter by
            since: Timestamp to fetch orders from
            limit: Maximum number of orders to fetch
            params: Additional exchange-specific parameters
            
        Returns:
            List of order details
        """
        pass
    
    @abstractmethod
    async def fetch_positions(self, 
                             symbols: Optional[List[str]] = None, 
                             params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Fetch positions (for margin/futures markets)
        
        Args:
            symbols: List of trading pairs to fetch positions for
            params: Additional exchange-specific parameters
            
        Returns:
            List of position details
        """
        pass
    
    @abstractmethod
    async def fetch_balance(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Fetch account balance
        
        Args:
            params: Additional exchange-specific parameters
            
        Returns:
            Dict containing balance information
        """
        pass
    
    @abstractmethod
    async def fetch_order(self, 
                         id: str, 
                         symbol: Optional[str] = None, 
                         params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fetch an order's status"""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the exchange connection"""
        pass