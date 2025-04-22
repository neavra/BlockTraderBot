from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from strategy.domain.dto.indicator_result_dto import IndicatorResultDto
from shared.domain.dto.candle_dto import CandleDto

@dataclass
class FvgDto:
    """Fair Value Gap (FVG) data transfer object"""
    type: str  # 'bullish' or 'bearish'
    top: float
    bottom: float
    size: float
    size_percent: float
    candle_index: int
    candle: CandleDto
    filled: bool = False
    timestamp: Optional[datetime] = None
    
    
    @property
    def is_bullish(self) -> bool:
        """Check if this is a bullish FVG"""
        return self.type == 'bullish'
    
    @property
    def is_bearish(self) -> bool:
        """Check if this is a bearish FVG"""
        return self.type == 'bearish'
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for compatibility with existing code"""
        return {k: v for k, v in vars(self).items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FvgDto':
        """Create an FVG DTO from a dictionary"""
        return cls(
            type=data.get('type', ''),
            top=data.get('top', 0.0),
            bottom=data.get('bottom', 0.0),
            size=data.get('size', 0.0),
            size_percent=data.get('size_percent', 0.0),
            candle_index=data.get('candle_index', 0),
            filled=data.get('filled', False),
            timestamp=data.get('timestamp'),
            candle=data.get('candle')
        )


@dataclass
class FvgResultDto(IndicatorResultDto):
    """Fair Value Gap (FVG) indicator result data transfer object"""
    bullish_fvgs: List[FvgDto]
    bearish_fvgs: List[FvgDto]
    
    @property
    def has_bullish(self) -> bool:
        """Check if result contains any bullish FVGs"""
        return len(self.bullish_fvgs) > 0
        
    @property
    def has_bearish(self) -> bool:
        """Check if result contains any bearish FVGs"""
        return len(self.bearish_fvgs) > 0
    
    @property
    def latest_bullish(self) -> Optional[FvgDto]:
        """Get the most recent bullish FVG if available"""
        return self.bullish_fvgs[0] if self.bullish_fvgs else None
    
    @property
    def latest_bearish(self) -> Optional[FvgDto]:
        """Get the most recent bearish FVG if available"""
        return self.bearish_fvgs[0] if self.bearish_fvgs else None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FvgResultDto':
        """Create an FVG result DTO from a dictionary"""
        bullish_fvgs = [FvgDto.from_dict(fvg) for fvg in data.get('bullish_fvgs', [])]
        bearish_fvgs = [FvgDto.from_dict(fvg) for fvg in data.get('bearish_fvgs', [])]
        
        return cls(
            timestamp=data.get('timestamp', datetime.now()),
            indicator_name=data.get('indicator_name', 'FVG'),
            bullish_fvgs=bullish_fvgs,
            bearish_fvgs=bearish_fvgs
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary including nested FVGs"""
        result = super().to_dict()
        result['bullish_fvgs'] = [fvg.to_dict() for fvg in self.bullish_fvgs]
        result['bearish_fvgs'] = [fvg.to_dict() for fvg in self.bearish_fvgs]
        return result