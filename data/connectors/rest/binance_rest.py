import os
import ssl
import certifi
import aiohttp
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

from managers.candle_manager import CandleManager

from .base import RestClient
from shared.domain.dto.candle_dto import CandleDto

class BinanceRestClient(RestClient):
    """
    Binance REST API client for fetching candlestick data.
    """
    
    def __init__(
        self, 
        symbol: str,
        exchange : str, 
        interval: str, 
        base_url: Optional[str] = None
    ):
        """
        Initialize the Binance REST client.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Candlestick interval (e.g., "1m", "5m", "1h")
            candlestick_service: Service for handling candlestick data
            base_url: Optional override for the Binance API URL
        """
        self.base_url = base_url or os.getenv("BINANCE_API_URL", "https://api.binance.com/api/v3/klines")
        self.symbol = symbol.upper()
        self.exchange = exchange
        self.interval = interval
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.logger = logging.getLogger(f"BinanceREST_{symbol}_{interval}")

    def _build_url(
            self,
            limit: Optional[int] = None,
            startTime: Optional[int] = None,
            endTime: Optional[int] = None
            ) -> str:
        """
        Dynamically constructs the API request URL, including optional parameters.
        
        Args:
            limit: Maximum number of candles to fetch (default is 500, max is 1500)
            startTime: Start time in milliseconds
            endTime: End time in milliseconds
            
        Returns:
            Fully qualified URL string
        """
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
            limit: Optional[int] = None, 
            startTime: Optional[int] = None,
            endTime: Optional[int] = None
            ) -> List[CandleDto]:
        """
        Fetch candlestick data from Binance.
        
        Args:
            limit: Maximum number of candles to fetch (default is 500, max is 1500)
            startTime: Start time in milliseconds
            endTime: End time in milliseconds
            
        Returns:
            List of CandleDto objects
            
        Raises:
            ValueError: If the input parameters are invalid
        """
        url = self._build_url(limit=limit, startTime=startTime, endTime=endTime)
        try:
            # Validate timestamps
            if startTime is not None and not isinstance(startTime, int):
                raise ValueError(f"Invalid startTime: {startTime} (should be an integer)")
            if endTime is not None and not isinstance(endTime, int):
                raise ValueError(f"Invalid endTime: {endTime} (should be an integer)")
                
            # self.logger.debug(f"Fetching candlestick data from: {url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, ssl=self.ssl_context) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        self.logger.error(f"API error: {response.status} - {error_text}")
                        return []
                    
                    data = await response.json()
                    
                    """ # Parse and convert to CandleDto objects
                    candles = [
                        CandleDto(
                            symbol=self.symbol, 
                            exchange=self.exchange, 
                            timeframe=self.interval,
                            timestamp=datetime.fromtimestamp(c[6]/1000, tz=timezone.utc), 
                            open=float(c[1]), 
                            high=float(c[2]),
                            low=float(c[3]), 
                            close=float(c[4]), 
                            volume=float(c[5])
                        )
                        for c in data if isinstance(c, list) and len(c) >= 7
                    ] """
                    
                    self.logger.info(f"Fetched {len(data)} candles for {self.symbol}/{self.interval}")
                    return data
                    
        except Exception as e:
            self.logger.error(f"Error fetching candlestick data: {e}")
            return []