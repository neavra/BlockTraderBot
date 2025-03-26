from abc import ABC, abstractmethod
from typing import Dict, Any

class Normalizer(ABC):
    """
    Abstract base class for normalizing exchange-specific data
    into a standardized format.
    """
    
    @abstractmethod
    async def normalize_websocket_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize WebSocket data from an exchange.
        
        Args:
            data: Raw exchange-specific data
            
        Returns:
            Normalized data in standard format
        """
        pass
    
    @abstractmethod
    async def normalize_rest_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize REST API data from an exchange.
        
        Args:
            data: Raw exchange-specific data
            
        Returns:
            Normalized data in standard format
        """
        pass

    @abstractmethod   
    def to_json(self, normalized_obj: Any) -> str:
        """
        Convert a normalized object (dataclass) to a JSON string.

        Args:
            normalized_obj: The normalized object (dataclass)

        Returns:
            JSON string representation of the object
        """
        pass