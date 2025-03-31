from dataclasses import dataclass, field
from shared.domain.dto.candle import CandleData
from .base_event import BaseEvent

@dataclass(kw_only=True)
class CandleClosedEvent(BaseEvent):
    """
    Event published when a candle is closed.
    """
    candle: CandleData

@dataclass(kw_only=True)
class CandleUpdatedEvent(BaseEvent):
    """
    Event published when an open candle is updated.
    """
    candle: CandleData