from abc import ABC, abstractmethod

class RestClientBase(ABC):
    """Base interface for REST API clients."""
    
    @abstractmethod
    def fetch_historical_data(self, symbol: str, timeframe: str, limit: int):
        pass
