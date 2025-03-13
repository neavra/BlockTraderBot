from abc import ABC, abstractmethod
from typing import Dict, Any, List
from .types import CandleSchema

# Base Interface for Market Data Clients
class MarketDataClient(ABC):
    @abstractmethod
    async def fetch_candlestick_data(self, symbol: str, interval: str) -> CandleSchema:
        pass