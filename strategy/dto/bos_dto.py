from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from strategy.dto.indicator_result_dto import IndicatorResultDto
from shared.domain.dto.candle_dto import CandleDto


@dataclass
class StructureBreakDto:
    """Breaking of Structure (BOS) data transfer object"""
    index: int
    break_type: str  # 'higher_high', 'lower_low', 'higher_low', 'lower_high'
    break_value: float
    break_percentage: float
    swing_reference: float
    candle: CandleDto
    timestamp: Optional[datetime] = None
    
    @property
    def is_bullish(self) -> bool:
        """Check if this is a bullish structure break"""
        return self.break_type in ['higher_high', 'higher_low']
    
    @property
    def is_bearish(self) -> bool:
        """Check if this is a bearish structure break"""
        return self.break_type in ['lower_low', 'lower_high']
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for compatibility with existing code"""
        return {k: v for k, v in vars(self).items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StructureBreakDto':
        """Create a Structure Break DTO from a dictionary"""
        return cls(
            index=data.get('index', 0),
            break_type=data.get('break_type', ''),
            break_value=data.get('break_value', 0.0),
            break_percentage=data.get('break_percentage', 0.0),
            swing_reference=data.get('swing_reference', 0.0),
            candle=data.get('candle', {}),
            timestamp=data.get('timestamp')
        )


@dataclass
class StructureBreakResultDto(IndicatorResultDto):
    """Breaking of Structure (BOS) indicator result data transfer object"""
    bullish_breaks: List[StructureBreakDto]
    bearish_breaks: List[StructureBreakDto]
    
    @property
    def has_bullish_break(self) -> bool:
        """Check if result contains any bullish breaks"""
        return len(self.bullish_breaks) > 0
    
    @property
    def has_bearish_break(self) -> bool:
        """Check if result contains any bearish breaks"""
        return len(self.bearish_breaks) > 0
    
    @property
    def all_breaks(self) -> List[StructureBreakDto]:
        """Get all breaks (both bullish and bearish)"""
        return self.bullish_breaks + self.bearish_breaks
    
    @property
    def latest_break(self) -> Optional[StructureBreakDto]:
        """Get the most recent structure break if available"""
        # Combine and sort all breaks by index (descending)
        sorted_breaks = sorted(
            self.all_breaks, 
            key=lambda x: x.index, 
            reverse=True
        )
        return sorted_breaks[0] if sorted_breaks else None
    
    @property
    def latest_bullish_break(self) -> Optional[StructureBreakDto]:
        """Get the most recent bullish break if available"""
        return self.bullish_breaks[0] if self.bullish_breaks else None
    
    @property
    def latest_bearish_break(self) -> Optional[StructureBreakDto]:
        """Get the most recent bearish break if available"""
        return self.bearish_breaks[0] if self.bearish_breaks else None
    
    @property
    def higher_highs(self) -> List[StructureBreakDto]:
        """Get all higher high breaks"""
        return [b for b in self.bullish_breaks if b.break_type == 'higher_high']
    
    @property
    def higher_lows(self) -> List[StructureBreakDto]:
        """Get all higher low breaks"""
        return [b for b in self.bullish_breaks if b.break_type == 'higher_low']
    
    @property
    def lower_lows(self) -> List[StructureBreakDto]:
        """Get all lower low breaks"""
        return [b for b in self.bearish_breaks if b.break_type == 'lower_low']
    
    @property
    def lower_highs(self) -> List[StructureBreakDto]:
        """Get all lower high breaks"""
        return [b for b in self.bearish_breaks if b.break_type == 'lower_high']
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StructureBreakResultDto':
        """Create a Structure Break result DTO from a dictionary"""
        # Handle old format with a single 'breaks' list
        if 'breaks' in data:
            all_breaks = [StructureBreakDto.from_dict(b) for b in data.get('breaks', [])]
            bullish_breaks = [b for b in all_breaks if b.is_bullish]
            bearish_breaks = [b for b in all_breaks if b.is_bearish]
        else:
            # Handle new format with separate bullish and bearish lists
            bullish_breaks = [StructureBreakDto.from_dict(b) for b in data.get('bullish_breaks', [])]
            bearish_breaks = [StructureBreakDto.from_dict(b) for b in data.get('bearish_breaks', [])]
        
        return cls(
            timestamp=data.get('timestamp', datetime.now()),
            indicator_name=data.get('indicator_name', 'StructureBreak'),
            bullish_breaks=bullish_breaks,
            bearish_breaks=bearish_breaks
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary including nested structure breaks"""
        result = super().to_dict()
        result['bullish_breaks'] = [b.to_dict() for b in self.bullish_breaks]
        result['bearish_breaks'] = [b.to_dict() for b in self.bearish_breaks]
        return result