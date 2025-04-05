from dataclasses import dataclass
from typing import Union
from datetime import datetime
from shared.domain.dto.candle_dto import CandleDto

from .base_event import BaseEvent

@dataclass(kw_only=True)
class CandleClosedEvent(BaseEvent):
    """
    Event published when a candle is closed.
    """
    symbol: str
    exchange: str
    timeframe: str
    timestamp: Union[datetime ,str]

    @classmethod
    def to_event(candle_dto: CandleDto, data_type: str) -> 'CandleClosedEvent':
        """
        Convert a CandleDto to a CandleClosedEvent.
        
        Args:
            candle_dto: The candle data transfer object to convert
            data_type: The type of data source ("historical" or "live")
            
        Returns:
            A CandleClosedEvent object
        """
        return CandleClosedEvent(
            symbol=candle_dto.symbol,
            exchange=candle_dto.exchange,
            timeframe=candle_dto.timeframe,
            timestamp=candle_dto.timestamp,
            source=data_type
        )
