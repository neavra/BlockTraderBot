from abc import ABC, abstractmethod

class WebSocketClientBase(ABC):
    """Base interface for WebSocket clients."""
    
    @abstractmethod
    def connect(self):
        pass
    
    @abstractmethod
    def subscribe(self, symbol: str, timeframe: str):
        pass
    
    @abstractmethod
    def on_message(self, message: dict):
        pass
    
    @abstractmethod
    def close(self):
        pass