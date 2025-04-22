from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any


@dataclass
class IndicatorResultDto:
    """
    Base class for all indicator results.
    Provides common fields and functionality for all indicator DTOs.
    """
    timestamp: datetime
    indicator_name: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for compatibility with existing code"""
        return {k: v for k, v in vars(self).items() if v is not None}