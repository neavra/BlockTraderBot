from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass
class CandleData:
    """
    Domain model for candlestick data.
    """
    symbol: str
    exchange: str
    timeframe: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool = True
    raw_data: Optional[Dict[str, Any]] = None
    
    def __str__(self) -> str:
        return (
            f"{self.exchange.upper()}:{self.symbol} {self.timeframe} "
            f"[{self.timestamp.isoformat()}] "
            f"O:{self.open} H:{self.high} L:{self.low} C:{self.close} V:{self.volume}"
        )