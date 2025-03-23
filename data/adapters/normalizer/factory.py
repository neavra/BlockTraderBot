from typing import Dict, Type
from .base import Normalizer
from .websocket.binance_websocket_normalizer import BinanceWebSocketNormalizer
from .rest.binance_rest_normalizer import BinanceRestNormalizer

class NormalizerFactory:
    """
    Factory for creating normalizers based on exchange.
    """
    
    # Registry of available normalizers
    _websocket_normalizers: Dict[str, Type[Normalizer]] = {
        "binance": BinanceWebSocketNormalizer,
        # Add more exchanges here
    }
    
    _rest_normalizers: Dict[str, Type[Normalizer]] = {
        "binance": BinanceRestNormalizer,
        # Add more exchanges here
    }
    
    @classmethod
    def create_websocket_normalizer(cls, exchange: str) -> Normalizer:
        """
        Create a WebSocket normalizer for the specified exchange.
        
        Args:
            exchange: Exchange name (e.g., "binance")
            
        Returns:
            Normalizer instance for the specified exchange
            
        Raises:
            ValueError: If the exchange is not supported
        """
        exchange = exchange.lower()
        if exchange not in cls._websocket_normalizers:
            raise ValueError(f"Unsupported exchange for WebSocket normalizer: {exchange}")
        
        normalizer_class = cls._websocket_normalizers[exchange]
        return normalizer_class()
    
    @classmethod
    def create_rest_normalizer(cls, exchange: str) -> Normalizer:
        """
        Create a REST normalizer for the specified exchange.
        
        Args:
            exchange: Exchange name (e.g., "binance")
            
        Returns:
            Normalizer instance for the specified exchange
            
        Raises:
            ValueError: If the exchange is not supported
        """
        exchange = exchange.lower()
        if exchange not in cls._rest_normalizers:
            raise ValueError(f"Unsupported exchange for REST normalizer: {exchange}")
        
        normalizer_class = cls._rest_normalizers[exchange]
        return normalizer_class()
    
    @classmethod
    def register_websocket_normalizer(cls, exchange: str, normalizer_class: Type[Normalizer]):
        """
        Register a new WebSocket normalizer implementation.
        
        Args:
            exchange: Exchange name
            normalizer_class: Normalizer implementation class
        """
        cls._websocket_normalizers[exchange.lower()] = normalizer_class
    
    @classmethod
    def register_rest_normalizer(cls, exchange: str, normalizer_class: Type[Normalizer]):
        """
        Register a new REST normalizer implementation.
        
        Args:
            exchange: Exchange name
            normalizer_class: Normalizer implementation class
        """
        cls._rest_normalizers[exchange.lower()] = normalizer_class