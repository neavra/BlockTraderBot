import asyncio
from typing import List, Dict
from connectors.binance.BinanceClient import BinanceWebSocketClient, BinanceRestClient
from database.services.candleservice import CandleService

"""To manage the multiple timeframe websockets from the exchange"""
class CandleManager:
    def __init__(self, symbol: str, timeframes: List[str], candle_service: CandleService):
        self.symbol = symbol
        self.timeframes = timeframes
        self.candle_service = candle_service
        self.websocket_clients: Dict[str, BinanceWebSocketClient] = {}
        
    async def initialize_websockets(self):
        """Initialize websocket connections for each timeframe"""
        for timeframe in self.timeframes:
            self.websocket_clients[timeframe] = BinanceWebSocketClient(
                self.symbol, timeframe, self.candle_service
            )
    
    async def start_websocket_listeners(self):
        """Start all websocket listeners"""
        tasks = set()
        for timeframe, client in self.websocket_clients.items():
            tasks.add(asyncio.create_task(client.listen()))
        return tasks
    
    async def fetch_historical_for_timeframe(self, timeframe: str):
        """Fetch historical data for a specific timeframe"""
        while True:
            try:
                self.rest_client = BinanceRestClient(self.symbol, timeframe, self.candle_service)
                data = await self.rest_client.fetch_candlestick_data(self.symbol, timeframe, self.candle_service)
                await self.candle_service.add_candles(data)
                
                # Adjust sleep time based on timeframe to reduce unnecessary API calls
                sleep_time = self._calculate_sleep_time(timeframe)
                await asyncio.sleep(sleep_time)
            except Exception as e:
                print(f"Error fetching historical data for {timeframe}: {e}")
                await asyncio.sleep(60)  # Wait a minute before retrying
    
    def _calculate_sleep_time(self, timeframe: str) -> int:
        """Calculate appropriate sleep time based on timeframe"""
        # Parse the timeframe string (e.g., "1m", "4h", "1d")
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        
        if unit == "s":
            return value * 5  # 5x the seconds timeframe
        elif unit == "m":
            return value * 60  # Convert minutes to seconds and multiply
        elif unit == "h":
            return value * 60 * 60  # Convert hours to seconds
        elif unit == "d":
            return value * 24 * 60 * 60  # Convert days to seconds
        elif unit == "w":
            return value * 7 * 24 * 60 * 60  # Convert weeks to seconds
        else:  # "M" for month, approximate
            return 30 * 24 * 60 * 60  # Roughly a month
    
    async def start_historical_fetchers(self):
        """Start historical data fetchers for all timeframes"""
        tasks = []
        for timeframe in self.timeframes:
            tasks.append(asyncio.create_task(self.fetch_historical_for_timeframe(timeframe)))
        return tasks