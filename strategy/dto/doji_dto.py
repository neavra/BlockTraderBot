from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from strategy.dto.indicator_result_dto import IndicatorResultDto


@dataclass
class DojiDto:
    """Doji candle pattern data transfer object"""
    index: int
    body_to_range_ratio: float
    total_wick_size: float
    strength: float
    candle: Dict[str, Any]
    timestamp: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for compatibility with existing code"""
        return {k: v for k, v in vars(self).items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DojiDto':
        """Create a Doji DTO from a dictionary"""
        return cls(
            index=data.get('index', 0),
            body_to_range_ratio=data.get('body_to_range_ratio', 0.0),
            total_wick_size=data.get('total_wick_size', 0.0),
            strength=data.get('strength', 0.0),
            candle=data.get('candle', {}),
            timestamp=data.get('timestamp')
        )


@dataclass
class DojiResultDto(IndicatorResultDto):
    """Doji candle indicator result data transfer object"""
    dojis: List[DojiDto]
    
    @property
    def has_doji(self) -> bool:
        """Check if result contains any doji candles"""
        return len(self.dojis) > 0
    
    @property
    def latest_doji(self) -> Optional[DojiDto]:
        """Get the most recent doji if available"""
        return self.dojis[0] if self.dojis else None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DojiResultDto':
        """Create a Doji result DTO from a dictionary"""
        dojis = [DojiDto.from_dict(doji) for doji in data.get('dojis', [])]
        
        return cls(
            timestamp=data.get('timestamp', datetime.now()),
            indicator_name=data.get('indicator_name', 'Doji'),
            dojis=dojis
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary including nested dojis"""
        result = super().to_dict()
        result['dojis'] = [doji.to_dict() for doji in self.dojis]
        return result