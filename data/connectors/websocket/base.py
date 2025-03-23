from abc import ABC, abstractmethod
from typing import AsyncGenerator, Tuple, Any

class WebSocketClient(ABC):
    """
    Abstract base class for all WebSocket clients.
    Provides common interface for streaming candlestick data.
    """
    
    @abstractmethod
    async def fetch_candlestick_data(self) -> AsyncGenerator[Tuple[Any, bool], None]:
        """
        Stream candlestick data from the exchange.
        
        Yields:
            Tuple containing (candlestick_data, is_candle_closed)
        """
        pass
    
    @abstractmethod
    async def listen(self):
        """
        Start listening for WebSocket messages and process them.
        """
        pass
    
    @abstractmethod
    def setup_logger(self):
        """
        Configure the logger for this WebSocket client.
        """
        pass