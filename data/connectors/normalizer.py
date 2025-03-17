from .types import CandleSchema
from datetime import datetime, timezone
from decimal import Decimal
from typing import Tuple

# Convert naive datetime to aware datetime
def make_aware(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

def parse_binance_kline(data: dict, exchange: str = "Binance") -> Tuple[CandleSchema, bool]:
    """
    Converts Binance kline WebSocket data to a CandleSchema object.
    """
    kline = data["k"]
    isCandleClosed = kline["x"]
    
    return CandleSchema(
        symbol=data["s"],
        exchange=exchange,
        timeframe=kline["i"],
        timestamp=datetime.fromtimestamp(kline["T"] / 1000, tz=timezone.utc),  # Convert ms to seconds
        open=Decimal(kline["o"]),
        high=Decimal(kline["h"]),
        low=Decimal(kline["l"]),
        close=Decimal(kline["c"]),
        volume=Decimal(kline["v"]),
    ), isCandleClosed