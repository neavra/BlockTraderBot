from abc import ABC, abstractmethod
from typing import List, Optional, Any

class RestClient(ABC):
    """
    Abstract base class for all REST API clients.
    Provides common interface for fetching candlestick data.
    """
    def __init__(
        self, 
        symbol: str, 
        exchange: str,
        interval: str, 
        base_url: Optional[str] = None
    ):
        """
        Initialize the REST client.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Candlestick interval (e.g., "1m", "5m", "1h")
            candlestick_service: Service for handling candlestick data
            base_url: Optional override for the Binance API URL
        """
        self.base_url = base_url
        self.symbol = symbol.upper()
        self.exchange = exchange
        self.interval = interval
    
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