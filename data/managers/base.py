from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class BaseManager(ABC):
    """
    Abstract base class for managers.
    """
    
    @abstractmethod
    async def handle_websocket_data(self, data: Dict[str, Any], is_closed: bool) -> None:
        """
        Handle data received from a WebSocket.
        
        Args:
            data: Raw WebSocket data
            is_closed: Flag indicating if the candle is closed
        """
        pass
    
    @abstractmethod
    async def handle_rest_data(self, data: List[Dict[str, Any]]) -> None:
        """
        Handle data received from a REST API.
        
        Args:
            data: List of raw REST API data
        """
        pass