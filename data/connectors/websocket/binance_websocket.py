import json
import ssl
import certifi
import logging
import websockets
from typing import AsyncGenerator, Tuple, Dict, Any, Optional
from datetime import datetime

from .base import WebSocketClient
from managers.candle_manager import CandleManager
from shared.domain.dto.candle_dto import CandleDto

class BinanceWebSocketClient(WebSocketClient):
    """
    Binance WebSocket client for streaming candlestick data.
    """
    
    def __init__(self, symbol: str, interval: str, manager: CandleManager = None):
        """
        Initialize the Binance WebSocket client.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Candlestick interval (e.g., "1m", "5m", "1h")
            manager: Optional manager instance to handle the data processing
        """
        self.symbol = symbol.lower()
        self.interval = interval
        self.url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.manager = manager
        
        # Create connection factory for WebSocketClient
        connection_factory = lambda: websockets.connect(self.url, ssl=self.ssl_context)
        
        # Initialize the base class with connection parameters
        super().__init__(
            connection_factory=connection_factory,
            max_retries=10,
            retry_delay=5.0
        )
    
    def setup_logger(self) -> logging.Logger:
        """Configure the logger for this WebSocket client."""
        logger = logging.getLogger(f"BinanceWS_{self.symbol}_{self.interval}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def parse_binance_kline(self, data: Dict) -> Tuple[Dict, bool]:
        """
        Parse Binance kline data from the WebSocket.
        
        Args:
            data: Raw WebSocket message data
            
        Returns:
            Tuple containing (raw_candle_data, is_candle_closed)
        """
        k = data.get('k', {})
        is_candle_closed = k.get('x', False)
        self.logger.debug("Raw Websocket Candle data:", k)
        # Return the raw kline data for the normalizer to process
        return {
            'exchange': 'binance',
            'symbol': k.get('s', self.symbol.upper()),
            'interval': k.get('i', self.interval),
            'event_time': data.get('E'),
            'start_time': k.get('t'),
            'close_time': k.get('T'),
            'open': k.get('o'),
            'high': k.get('h'),
            'low': k.get('l'),
            'close': k.get('c'),
            'volume': k.get('v'),
            'is_closed': is_candle_closed
        }, is_candle_closed
    
    async def fetch_candlestick_data(self) -> AsyncGenerator[Tuple[Dict, bool], None]:
        """
        Stream candlestick data from Binance.
        
        Yields:
            Tuple containing (raw_candle_data, is_candle_closed)
        """
        # Use the connect method from the base class
        ws = await self.connect()
        
        try:
            async for message in ws:
                data = json.loads(message)
                candle_data, is_candle_closed = self.parse_binance_kline(data)
                yield candle_data, is_candle_closed
                
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket connection closed. Reconnecting...")
            # Call self to restart the generator with a new connection
            async for result in self.fetch_candlestick_data():
                yield result
            
        except Exception as e:
            self.logger.error(f"Error in WebSocket stream: {str(e)}")
            # Call disconnect from the base class
            await self.disconnect()
            raise
    
    async def listen(self):
        """
        Start listening for WebSocket messages and forward to the manager.
        Does not directly interact with database.
        """
        self.logger.info(f"Starting WebSocket connection to {self.url}")
        
        if not self.manager:
            self.logger.error("No candle manager provided. Cannot process candle data.")
            return
        
        try:
            async for candle_data, is_candle_closed in self.fetch_candlestick_data():
                # Pass the raw data to the manager for processing
                # The manager will handle normalizing and routing the data
                await self.manager.handle_websocket_data(candle_data, is_candle_closed)
                    
        except Exception as e:
            self.logger.error(f"Error in WebSocket listener: {str(e)}")
            await self.disconnect()
            raise