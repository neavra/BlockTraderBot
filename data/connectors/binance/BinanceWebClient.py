from datetime import datetime, timezone
from ..marketdataclient import MarketDataClient
from ..types import CandleSchema
from ..normalizer import make_aware, parse_binance_kline
from database.services.candleservice import CandleService
from typing import Dict, Any, List, Optional, Tuple
import websockets
import asyncio
import aiohttp
import json
import ssl
import certifi
import logging
import os
from dotenv import load_dotenv
# Load environment variables
load_dotenv()

# WebSocket Market Data Client
class BinanceWebSocketClient(MarketDataClient):
    def __init__(self, symbol: str, interval: str, candlestick_service : CandleService):
        self.symbol = symbol.lower()
        self.interval = interval
        self.url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.candleSvc = candlestick_service
        # Keep track of candles we've seen but aren't closed yet
        self.open_candles = {}
        # Setup logger
        self.logger = logging.getLogger(f"BinanceWS_{symbol}_{interval}")
        self.setup_logger()
    
    def setup_logger(self):
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    async def fetch_candlestick_data(self) -> Tuple[CandleSchema, bool]:
        async with websockets.connect(self.url, ssl=self.ssl_context) as ws:
            async for message in ws:
                data = json.loads(message)
                candle, isCandleClosed = parse_binance_kline(data)
                yield candle, isCandleClosed
    async def listen(self):
        """
        This method starts the WebSocket client and listens for the data.
        It fetches the data using the `fetch_candlestick_data` method.
        """
        async for candle, is_candle_closed in self.fetch_candlestick_data():
            # Closed flag from binance
            if is_candle_closed:
                # Candle is closed - write it to DB and remove from tracking
                self.logger.info(f"Closed candle: {candle.timestamp}, Symbol: {candle.symbol}, Timeframe: {candle.timeframe}, Open: {candle.open}, Close: {candle.close}")
                
                # Check if candle exists in DB before adding
                existing_candle = await self.candleSvc.get_candle(
                    symbol=candle.symbol, exchange=candle.exchange, timeframe=candle.timeframe, timestamp=candle.timestamp
                )
                candle_object : CandleSchema = CandleSchema(
                            symbol=candle.symbol, exchange=candle.exchange.lower(), timeframe=candle.timeframe,
                            timestamp=candle.timestamp, open=candle.open, high=candle.high,
                            low=candle.low, close=candle.close, volume=candle.volume
                        )
                
                if existing_candle:
                    await self.candleSvc.update_candle(candle_object)
                else:
                    await self.candleSvc.add_candles([candle_object])
                
                # Remove from tracking
                if candle.timeframe in self.open_candles:
                    del self.open_candles[candle.timeframe]
            else:
                # Candle is still open - just keep the latest version in memory
                self.logger.debug(f"Updated open candle: {candle.timestamp}, Open: {candle.open}, Close: {candle.close}")
                self.open_candles[candle.timeframe] = candle
            

# REST Market Data Client
class BinanceRestClient(MarketDataClient):
    def __init__(
        self, 
        symbol: str, 
        interval: str, 
        candlestick_service: CandleService,
    ):
        self.base_url = os.getenv("BINANCE_API_URL", "https://api.binance.com/api/v3/klines")
        self.symbol = symbol.upper()
        self.exchange = "binance"
        self.interval = interval
        self.candleSvc = candlestick_service
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

    def _build_url(
            self,
            limit: Optional[int] = None,
            startTime: Optional[int] = None,
            endTime: Optional[int] = None
            ) -> str:
        """Dynamically constructs the API request URL, including optional parameters."""
        params = {
            "symbol": self.symbol,
            "interval": self.interval,
            "limit": limit,
            "startTime": startTime,
            "endTime": endTime
        }
        # Remove any None values
        filtered_params = {k: v for k, v in params.items() if v is not None}
        query_string = "&".join(f"{key}={value}" for key, value in filtered_params.items())
        return f"{self.base_url}?{query_string}"

    async def fetch_candlestick_data(
            self,
            limit: Optional[int] = None, #default is 500, max is 1500
            startTime: Optional[int] = None,
            endTime: Optional[int] = None
            ) -> List[CandleSchema]:
        url = self._build_url(limit=limit, startTime=startTime, endTime=endTime)
        try:
            # Validate timestamps
            if startTime is not None and not isinstance(startTime, int):
                raise ValueError(f"Invalid startTime: {startTime} (should be an integer)")
            if endTime is not None and not isinstance(endTime, int):
                raise ValueError(f"Invalid endTime: {endTime} (should be an integer)")
            #print(url)
            async with aiohttp.ClientSession() as session:
                async with session.get(url, ssl=self.ssl_context) as response:
                    data = await response.json()
                    #print(data)
                    return [
                        CandleSchema(
                            symbol=self.symbol, exchange=self.exchange, timeframe=self.interval,
                            timestamp=datetime.fromtimestamp(c[6]/1000, tz=timezone.utc), open=float(c[1]), high=float(c[2]),
                            low=float(c[3]), close=float(c[4]), volume=float(c[5])
                        )
                        for c in data if isinstance(c, list) and len(c) >= 7
                    ]
        except Exception as e:
            print(f"Error fetching candlestick data: {e}")
            return []