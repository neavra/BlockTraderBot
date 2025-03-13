from ..marketdataclient import MarketDataClient
from ..types import CandleSchema
from ..normalizer import parse_binance_kline
from database.services.candleservice import CandleService
from typing import Dict, Any, List
import websockets
import asyncio
import aiohttp
import json
import ssl
import certifi

# WebSocket Market Data Client
class BinanceWebSocketClient(MarketDataClient):
    def __init__(self, symbol: str, interval: str, candlestick_service : CandleService):
        self.symbol = symbol.lower()
        self.interval = interval
        self.url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.candleSvc = candlestick_service
    
    async def fetch_candlestick_data(self) -> CandleSchema:
        async with websockets.connect(self.url, ssl=self.ssl_context) as ws:
            async for message in ws:
                data = json.loads(message)
                candle = parse_binance_kline(data)
                yield candle
    async def listen(self):
        """
        This method starts the WebSocket client and listens for the data.
        It fetches the data using the `fetch_candlestick_data` method.
        """
        async for candle in self.fetch_candlestick_data():
            # Process each candle as you wish, e.g., pass to a service or save to a DB
            print(f"New candle: {candle.timestamp}, Open: {candle.open}, Close: {candle.close}")
            if self.candleSvc:
                result = self.candleSvc.add_candle(candle.dict())
                if result == None:
                    self.candleSvc.update_candle(candle.dict())
            # You can also use this to update or store the data in your candlestick service
            # self.candlestick_service.process_candle(candle)

# REST Market Data Client
class BinanceRestClient(MarketDataClient):
    def __init__(self, symbol: str, interval: str, candlestick_service : CandleService):
        BASE_URL = "https://api.binance.com/api/v3/klines"
        self.symbol = symbol.upper()
        self.interval = interval
        self.url = f"{BASE_URL}?symbol={self.symbol}&interval={self.interval}&limit=100"
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.candleSvc = candlestick_service
    
    async def fetch_candlestick_data(self) -> List[CandleSchema]:
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                data = await response.json()
                candles = [
                    CandleSchema(timestamp=c[0], open=float(c[1]), high=float(c[2]),
                           low=float(c[3]), close=float(c[4]), volume=float(c[5]))
                    for c in data
                ]
                return candles