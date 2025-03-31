from datetime import datetime
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from dateutil import parser  # For parsing ISO date strings

@dataclass
class CandleData:
    """
    Domain model for candlestick data.
    """
    
    symbol: str
    exchange: str
    timeframe: str
    timestamp: Union[datetime ,str]
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool 
    raw_data: Optional[Dict[str, Any]] = None
    id: Optional[int] = None

    def __post_init__(self):
        """Ensure timestamp is always a datetime object."""
        if isinstance(self.timestamp, str):
            self.timestamp = parser.isoparse(self.timestamp)  # Convert ISO string to datetime

    def __str__(self) -> str:
        return (
            f"{self.exchange.upper()}:{self.symbol} {self.timeframe} "
            f"[{self.timestamp.isoformat()}] "
            f"O:{self.open} H:{self.high} L:{self.low} C:{self.close} V:{self.volume}"
        )