from datetime import datetime, UTC
from sqlalchemy import Column, String, Integer, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from .base import BaseModel


class FvgModel(BaseModel):
    """
    SQLAlchemy model for storing Fair Value Gap (FVG) instances.
    Based on the FvgDto structure in strategy/domain/dto/fvg_dto.py
    """
    
    __tablename__ = "fair_value_gaps"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Reference to indicator registry
    indicator_id = Column(Integer, ForeignKey('indicators.id'), nullable=False)
    indicator = relationship("IndicatorModel")
    
    # Exchange and market information
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(5), nullable=False, index=True)
    
    # FVG type
    type = Column(String(10), nullable=False)  # 'bullish' or 'bearish'
    
    # FVG boundaries
    top = Column(DECIMAL(20, 8), nullable=False)  # Upper boundary of the FVG
    bottom = Column(DECIMAL(20, 8), nullable=False)  # Lower boundary of the FVG
    
    # FVG metrics
    size = Column(DECIMAL(20, 8), nullable=False)  # Size of the gap
    size_percent = Column(DECIMAL(10, 4), nullable=False)  # Size as percentage
    strength = Column(DECIMAL(5, 2), nullable=True)  # Strength score (0-1)
    
    # Status
    filled = Column(Boolean, default=False, nullable=False)  # Whether the FVG has been filled
    
    # Timing information
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)  # When the FVG formed
    candle_index = Column(Integer, nullable=True)  # Index of the candle where FVG formed
    
    # Candle data (stored as JSON)
    candle_data = Column(JSONB, nullable=True)  # The candle data where FVG formed
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    
    # Additional metadata
    metadata_ = Column(JSONB, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_fvg_lookup', 'exchange', 'symbol', 'timeframe'),
        Index('idx_fvg_type', 'type'),
        Index('idx_fvg_filled', 'filled'),
        Index('idx_fvg_timestamp', 'timestamp'),
    )
    
    def __repr__(self) -> str:
        return (
            f"FVG({self.exchange}:{self.symbol}:{self.timeframe}, "
            f"type={self.type}, "
            f"size={self.size}, "
            f"filled={self.filled})"
        )
    
    def to_dict(self):
        """Convert FVG model to dictionary"""
        return {
            "id": self.id,
            "indicator_id": self.indicator_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "type": self.type,
            "top": float(self.top) if self.top else None,
            "bottom": float(self.bottom) if self.bottom else None,
            "size": float(self.size) if self.size else None,
            "size_percent": float(self.size_percent) if self.size_percent else None,
            "strength": float(self.strength) if self.strength else None,
            "filled": self.filled,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "candle_index": self.candle_index,
            "candle_data": self.candle_data,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata_": self.metadata_
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create FVG model from dictionary"""
        return cls(
            id=data.get("id"),
            indicator_id=data.get("indicator_id"),
            exchange=data.get("exchange"),
            symbol=data.get("symbol"),
            timeframe=data.get("timeframe"),
            type=data.get("type"),
            top=data.get("top"),
            bottom=data.get("bottom"),
            size=data.get("size"),
            size_percent=data.get("size_percent"),
            strength=data.get("strength"),
            filled=data.get("filled", False),
            timestamp=data.get("timestamp"),
            candle_index=data.get("candle_index"),
            candle_data=data.get("candle_data"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata_=data.get("metadata_", {})
        )