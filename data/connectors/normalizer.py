from .types import CandleSchema
from datetime import datetime
from decimal import Decimal

def parse_binance_kline(data: dict, exchange: str = "Binance") -> CandleSchema:
    """
    Converts Binance kline WebSocket data to a CandleSchema object.
    """
    kline = data["k"]
    
    return CandleSchema(
        symbol=data["s"],
        exchange=exchange,
        timeframe=kline["i"],
        timestamp=datetime.utcfromtimestamp(kline["t"] / 1000),  # Convert ms to seconds
        open=Decimal(kline["o"]),
        high=Decimal(kline["h"]),
        low=Decimal(kline["l"]),
        close=Decimal(kline["c"]),
        volume=Decimal(kline["v"]),
    )