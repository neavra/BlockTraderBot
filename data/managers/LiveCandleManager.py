import asyncio
from typing import Dict, List, Type
from database.services.candleservice import CandleService

class LiveCandleManager:
    """
    Handles processing of real-time candlestick updates.
    """
    def __init__(self, 
                 candle_service: CandleService, 
                 symbol: str, 
                 timeframes: List[str],
                 websocket_client_class: Type,
                 ):
        """
        Args:
            symbol (str): Trading symbol (e.g., 'BTCUSDT').
            timeframes (List[str]): List of timeframes (e.g., ['1m', '5m', '1h']).
            candle_service (CandleService): Database service for storing candles.
            websocket_client_class (Type): WebSocket client class for the exchange.
            rest_client_class (Type): REST client class for fetching historical data.
        """
        self.candle_service = candle_service
        self.symbol = symbol
        self.websocket_client_class = websocket_client_class
        self.timeframes = timeframes
        self.websocket_clients: Dict[str, any] = {}
    
    async def initialize_websockets(self):
        """Initialize websocket connections for each timeframe"""
        for timeframe in self.timeframes:
            self.websocket_clients[timeframe] = self.websocket_client_class(
                self.symbol, timeframe, self.candle_service
            )
    
    async def start_websocket_listeners(self):
        """Start all websocket listeners"""
        tasks = set()
        for timeframe, client in self.websocket_clients.items():
            tasks.add(asyncio.create_task(client.listen()))
        return tasks