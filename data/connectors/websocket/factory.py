from typing import Dict, Type, Optional, Any
from .base import WebSocketClient
from .binance_websocket import BinanceWebSocketClient

class WebSocketClientFactory:
    """
    Factory for creating WebSocket clients.
    """
    
    # Registry of available client implementations
    _clients: Dict[str, Type[WebSocketClient]] = {
        "binance": BinanceWebSocketClient,
        # Add more exchanges here
    }
    
    @classmethod
    def create(cls, 
               exchange: str, 
               symbol: str, 
               interval: str, 
               manager: Any,
               **kwargs) -> WebSocketClient:
        """
        Create a WebSocket client for the specified exchange.
        
        Args:
            exchange: Exchange name (e.g., "binance")
            symbol: Trading pair symbol
            interval: Candlestick interval
            manager: Manager for handling candlestick data
            **kwargs: Additional exchange-specific arguments
            
        Returns:
            WebSocketClient instance for the specified exchange
            
        Raises:
            ValueError: If the exchange is not supported
        """
        exchange = exchange.lower()
        if exchange not in cls._clients:
            raise ValueError(f"Unsupported exchange: {exchange}")
        
        client_class = cls._clients[exchange]
        return client_class(symbol=symbol, 
                           interval=interval, 
                           manager=manager,
                           **kwargs)
    
    @classmethod
    def register(cls, exchange: str, client_class: Type[WebSocketClient]):
        """
        Register a new WebSocket client implementation.
        
        Args:
            exchange: Exchange name
            client_class: WebSocketClient implementation class
        """
        cls._clients[exchange.lower()] = client_class