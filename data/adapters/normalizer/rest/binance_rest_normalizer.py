from datetime import datetime, timezone
from typing import Dict, Any, List

from ..base import Normalizer
from domain.models.candle import CandleData

class BinanceRestNormalizer(Normalizer):
    """
    Normalizer for Binance REST API data.
    """
    
    async def normalize_websocket_data(self, data: Dict[str, Any]) -> CandleData:
        """
        This method is implemented to satisfy the abstract base class,
        but for Binance we have a separate WebSocket normalizer.
        """
        raise NotImplementedError("Use BinanceWebSocketNormalizer for WebSocket data")
    
    async def normalize_rest_data(self, data: List, exchange: str, symbol: str, interval: str) -> CandleData:
        """
        Normalize Binance REST API data to standard format.
        
        Args:
            data: Raw Binance REST API data (one kline entry)
            
        Returns:
            Normalized candle data
        """
        # Binance REST API returns an array with specific positions:
        # [
        #     0: Open time,
        #     1: Open,
        #     2: High,
        #     3: Low,
        #     4: Close,
        #     5: Volume,
        #     6: Close time,
        #     ...additional fields...
        # ]
        
        if not isinstance(data, list) or len(data) < 7:
            print("error:",data)
            raise ValueError("Invalid Binance REST kline data format")
        
        # Create a normalized candle representation
        normalized_data = CandleData(
            symbol=symbol.upper(),
            exchange=exchange, 
            timeframe=interval,
            timestamp=datetime.fromtimestamp(data[6]/1000, tz=timezone.utc), 
            open=float(data[1]), 
            high=float(data[2]),
            low=float(data[3]), 
            close=float(data[4]), 
            volume=float(data[5]),
            is_closed=True, # REST API always returns completed candles
            #raw_data=data # Store original data for reference
        )
        
        return normalized_data