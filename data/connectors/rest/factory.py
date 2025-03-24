from typing import Dict, Type, Optional, Any

from .base import RestClient
from .binance_rest import BinanceRestClient

class RestClientFactory:
    """
    Factory for creating REST API clients.
    """
    
    # Registry of available client implementations
    _clients: Dict[str, Type[RestClient]] = {
        "binance": BinanceRestClient,
        # Add more exchanges here
    }
    
    @classmethod
    def create(cls, 
               exchange: str, 
               symbol: str, 
               interval: str, 
               **kwargs) -> RestClient:
        """
        Create a REST client for the specified exchange.
        
        Args:
            exchange: Exchange name (e.g., "binance")
            symbol: Trading pair symbol
            interval: Candlestick interval
            **kwargs: Additional exchange-specific arguments
            
        Returns:
            RestClient instance for the specified exchange
            
        Raises:
            ValueError: If the exchange is not supported
        """
        exchange = exchange.lower()
        if exchange not in cls._clients:
            raise ValueError(f"Unsupported exchange: {exchange}")
        
        client_class = cls._clients[exchange]
        return client_class(symbol=symbol, 
                            interval=interval, 
                            **kwargs)
    
    @classmethod
    def register(cls, exchange: str, client_class: Type[RestClient]):
        """
        Register a new REST client implementation.
        
        Args:
            exchange: Exchange name
            client_class: RestClient implementation class
        """
        cls._clients[exchange.lower()] = client_class