from dataclasses import asdict
from datetime import datetime, timezone
import json
from typing import Dict, Any, List

from utils.helper import DateTimeEncoder

from ..base import Normalizer
from shared.domain.dto.candle_dto import CandleDto

class BinanceRestNormalizer(Normalizer):
    """
    Normalizer for Binance REST API data.
    """
    
    async def normalize_websocket_data(self, data: Dict[str, Any]) -> CandleDto:
        """
        This method is implemented to satisfy the abstract base class,
        but for Binance we have a separate WebSocket normalizer.
        """
        raise NotImplementedError("Use BinanceWebSocketNormalizer for WebSocket data")
    
    async def normalize_rest_data(self, data: List, exchange: str, symbol: str, interval: str) -> CandleDto:
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
        
        current_timestamp = datetime.now().timestamp() * 1000
        # Create a normalized candle representation
        normalized_data = CandleDto(
            symbol=symbol.upper(),
            exchange=exchange, 
            timeframe=interval,
            timestamp=datetime.fromtimestamp(data[6]/1000, tz=timezone.utc), 
            open=float(data[1]), 
            high=float(data[2]),
            low=float(data[3]), 
            close=float(data[4]), 
            volume=float(data[5]),
            is_closed=data[6] < current_timestamp, # If less than current time then its closed
        )
        
        return normalized_data
    
    def to_json(self, normalized_candle : CandleDto) -> str:
        return json.dumps(asdict(normalized_candle), cls=DateTimeEncoder)