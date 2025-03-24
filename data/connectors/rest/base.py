from abc import ABC, abstractmethod
from typing import List, Optional, Any

class RestClient(ABC):
    """
    Abstract base class for all REST API clients.
    Provides common interface for fetching candlestick data.
    """
    
    @abstractmethod
    async def fetch_candlestick_data(self, **kwargs) -> List[Any]:
        """
        Fetch candlestick data from the exchange.
        
        Returns:
            List of candlestick data in normalized format
        """
        pass
    
    @abstractmethod
    def _build_url(self, **kwargs) -> str:
        """
        Build the URL for the API request.
        
        Returns:
            Fully qualified URL string
        """
        pass