from abc import ABC, abstractmethod
from typing import AsyncGenerator, Tuple, Any, Callable, Optional
import asyncio
import logging

class WebSocketClient(ABC):
    """
    Abstract base class for all WebSocket clients.
    Provides common interface for streaming candlestick data and connection management.
    """
    
    def __init__(self, 
                 connection_factory: Callable[[], Any] = None,
                 max_retries: int = 10,
                 retry_delay: float = 5.0):
        """
        Initialize the WebSocket client with connection management parameters.
        
        Args:
            connection_factory: Function that creates a new WebSocket connection
            max_retries: Maximum number of reconnection attempts
            retry_delay: Delay between reconnection attempts in seconds
        """
        self.connection_factory = connection_factory
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None
        self.is_running = False
        self.retry_count = 0
        self.logger = self.setup_logger()
    
    @abstractmethod
    def setup_logger(self) -> logging.Logger:
        """
        Configure the logger for this WebSocket client.
        
        Returns:
            Configured logger instance
        """
        pass
    
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
    
    async def connect(self):
        """
        Establish a WebSocket connection with retry logic.
        
        Returns:
            The WebSocket connection
        
        Raises:
            Exception: If connection fails after max retries
        """
        if not self.connection_factory:
            raise ValueError("No connection factory provided")
            
        self.is_running = True
        self.retry_count = 0
        
        while self.is_running and self.retry_count < self.max_retries:
            try:
                self.connection = await self.connection_factory()
                self.retry_count = 0
                self.logger.info("WebSocket connection established successfully")
                return self.connection
            
            except Exception as e:
                self.retry_count += 1
                self.logger.error(f"WebSocket connection failed (attempt {self.retry_count}/{self.max_retries}): {str(e)}")
                
                if self.retry_count >= self.max_retries:
                    self.logger.critical("Maximum retry attempts reached. Giving up.")
                    self.is_running = False
                    raise
                
                await asyncio.sleep(self.retry_delay)
        
        return None
    
    async def disconnect(self):
        """
        Close the WebSocket connection.
        """
        self.is_running = False
        if self.connection and hasattr(self.connection, 'close'):
            await self.connection.close()
            self.logger.info("WebSocket connection closed")
        self.connection = None