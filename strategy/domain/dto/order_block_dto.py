from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from strategy.domain.dto.indicator_result_dto import IndicatorResultDto
from shared.domain.dto.candle_dto import CandleDto
from strategy.domain.dto.bos_dto import StructureBreakDto
from strategy.domain.dto.fvg_dto import FvgDto
from strategy.domain.dto.doji_dto import DojiDto


@dataclass
class OrderBlockDto:
    """Order Block data transfer object for a single order block"""
    timeframe: str
    symbol: str
    exchange: str
    type: str  # 'demand' or 'supply'
    price_high: float
    price_low: float
    index: int
    candle: CandleDto
    related_fvg: FvgDto
    is_doji: bool
    timestamp: datetime
    doji_data: DojiDto
    bos_data: StructureBreakDto
    status: str # 'active', 'mitigated'
    touched: bool
    mitigation_percentage: float
    created_at:datetime
    strength: Optional[float] = 0.0
    
    @property
    def is_demand(self) -> bool:
        """Check if this is a demand order block"""
        return self.type == 'demand'
    
    @property
    def is_supply(self) -> bool:
        """Check if this is a supply order block"""
        return self.type == 'supply'
    
    @property
    def mid_price(self) -> float:
        """Get the mid price of the order block"""
        return (self.price_high + self.price_low) / 2
    
    @property
    def size(self) -> float:
        """Get the size (range) of the order block"""
        return self.price_high - self.price_low
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for compatibility with existing code"""
        return {k: v for k, v in vars(self).items() if v is not None}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OrderBlockDto':
        """Create an OrderBlock DTO from a dictionary"""
        timestamp = data.get('timestamp')
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                timestamp = datetime.now(timezone.utc)
        return cls(
            timeframe=data['timeframe'],
            symbol=data['symbol'],
            exchange=data['exchange'],
            type=data['type'],
            price_high=data['price_high'],
            price_low=data['price_low'],
            index=data['index'],
            candle=data['candle'],
            related_fvg=data['related_fvg'],
            is_doji=data['is_doji'],
            timestamp=timestamp,
            doji_data=data['doji_data'],
            bos_data=data['bos_data'],
            status=data['status'],
            touched=data['touched'],
            mitigation_percentage=data['mitigation_percentage'],
            strength=data['strength'],
            created_at=data['created_at']
        )


@dataclass
class OrderBlockResultDto(IndicatorResultDto):
    """Order Block indicator result data transfer object"""
    demand_blocks: List[OrderBlockDto]
    supply_blocks: List[OrderBlockDto]
    
    @property
    def has_demand_block(self) -> bool:
        """Check if result contains any demand order blocks"""
        return len(self.demand_blocks) > 0
    
    @property
    def has_supply_block(self) -> bool:
        """Check if result contains any supply order blocks"""
        return len(self.supply_blocks) > 0
    
    @property
    def all_blocks(self) -> List[OrderBlockDto]:
        """Get all order blocks (both demand and supply)"""
        return self.demand_blocks + self.supply_blocks
    
    @property
    def latest_block(self) -> Optional[OrderBlockDto]:
        """Get the most recent order block if available"""
        # Combine and sort all blocks by index (descending)
        sorted_blocks = sorted(
            self.all_blocks, 
            key=lambda x: x.index, 
            reverse=True
        )
        return sorted_blocks[0] if sorted_blocks else None
    
    @property
    def latest_demand_block(self) -> Optional[OrderBlockDto]:
        """Get the most recent demand block if available"""
        return self.demand_blocks[0] if self.demand_blocks else None
    
    @property
    def latest_supply_block(self) -> Optional[OrderBlockDto]:
        """Get the most recent supply block if available"""
        return self.supply_blocks[0] if self.supply_blocks else None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OrderBlockResultDto':
        """Create an OrderBlock result DTO from a dictionary"""
        # Handle old format with a single 'blocks' list and separate demand/supply lists
        if 'blocks' in data:
            all_blocks = [OrderBlockDto.from_dict(b) for b in data.get('blocks', [])]
            demand_blocks = [b for b in all_blocks if b.is_demand]
            supply_blocks = [b for b in all_blocks if b.is_supply]
        else:
            # Handle new format with separate demand and supply lists
            demand_blocks = [OrderBlockDto.from_dict(b) for b in data.get('demand_blocks', [])]
            supply_blocks = [OrderBlockDto.from_dict(b) for b in data.get('supply_blocks', [])]
        
        return cls(
            timestamp=data.get('timestamp', datetime.now()),
            indicator_name=data.get('indicator_name', 'OrderBlock'),
            demand_blocks=demand_blocks,
            supply_blocks=supply_blocks
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary including nested order blocks"""
        result = super().to_dict()
        result['demand_blocks'] = [b.to_dict() for b in self.demand_blocks]
        result['supply_blocks'] = [b.to_dict() for b in self.supply_blocks]
        return result