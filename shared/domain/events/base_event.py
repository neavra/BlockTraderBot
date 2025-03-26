from abc import ABC
from datetime import datetime
from dataclasses import dataclass, field
from uuid import uuid4

@dataclass
class BaseEvent(ABC):
    """
    Base class for all domain events.
    """
    id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id}, timestamp={self.timestamp})"