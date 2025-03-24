from dataclasses import asdict
from datetime import datetime, timezone
import json
from typing import Dict, Any

from utils.helper import DateTimeEncoder
from domain.models.candle import CandleData

from ..base import Normalizer

class BinanceWebSocketNormalizer(Normalizer):
    """
    Normalizer for Binance WebSocket data.
    """
    
    async def normalize_websocket_data(self, data: Dict[str, Any]) -> CandleData:
        """
        Normalize Binance WebSocket data to standard format.
        
        Args:
            data: Raw Binance WebSocket data
            
        Returns:
            Normalized candle data
        """
        
        # Create a normalized candle representation
        normalized_data = CandleData(
            symbol= data.get("symbol", "").upper(),
            exchange="binance",
            timeframe= data.get("interval", ""),
            timestamp= datetime.fromtimestamp(data.get("close_time", 0) / 1000, tz=timezone.utc),
            open= float(data.get("open", 0)),
            high= float(data.get("high", 0)),
            low= float(data.get("low", 0)),
            close=float(data.get("close", 0)),
            volume= float(data.get("volume", 0)),
            is_closed= data.get("is_closed", False),
            #"raw_data": data.get("raw_data", {})  # Store original data for reference
        )
        
        return normalized_data
    
    async def normalize_rest_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        This method is implemented to satisfy the abstract base class,
        but for Binance we have a separate REST normalizer.
        """
        raise NotImplementedError("Use BinanceRestNormalizer for REST data")
    
    def to_json(self, normalized_candle : CandleData) -> str:
        return json.dumps(asdict(normalized_candle), cls=DateTimeEncoder)