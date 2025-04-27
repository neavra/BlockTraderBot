from datetime import datetime, UTC
from sqlalchemy import Column, String, Integer, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from .base import BaseModel


class BosModel(BaseModel):
    """
    SQLAlchemy model for storing Breaking of Structure (BOS) instances.
    Based on the StructureBreakDto in strategy/domain/dto/bos_dto.py
    """
    
    __tablename__ = "structure_breaks"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Reference to indicator registry
    indicator_id = Column(Integer, ForeignKey('indicators.id'), nullable=False)
    indicator = relationship("IndicatorModel")
    
    # Exchange and market information
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(5), nullable=False, index=True)
    
    # BOS type and direction
    break_type = Column(String(20), nullable=False)  # 'higher_high', 'lower_low', 'higher_low', 'lower_high'
    direction = Column(String(10), nullable=False)  # 'bullish' or 'bearish'
    
    # Break metrics
    break_value = Column(DECIMAL(20, 8), nullable=False)  # Amount of the break
    break_percentage = Column(DECIMAL(10, 4), nullable=False)  # Break as percentage
    swing_reference = Column(DECIMAL(20, 8), nullable=False)  # Reference swing high/low that was broken
    strength = Column(DECIMAL(5, 2), nullable=True)  # Strength score (0-1)
    
    # Location and timing
    candle_index = Column(Integer, nullable=True)  # Index of the candle where break occurred
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)  # When the break occurred
    
    # Confirmation details
    confirmed = Column(Boolean, default=False, nullable=False)  # Whether the break was confirmed
    confirmation_candles = Column(Integer, nullable=True)  # Number of confirming candles
    
    # Candle data (stored as JSON)
    candle_data = Column(JSONB, nullable=True)  # The candle data where break occurred
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    
    # Additional metadata
    metadata_ = Column(JSONB, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_bos_lookup', 'exchange', 'symbol', 'timeframe'),
        Index('idx_bos_type', 'break_type'),
        Index('idx_bos_direction', 'direction'),
        Index('idx_bos_timestamp', 'timestamp'),
        Index('idx_bos_confirmed', 'confirmed'),
    )
    
    def __repr__(self) -> str:
        return (
            f"BOS({self.exchange}:{self.symbol}:{self.timeframe}, "
            f"type={self.break_type}, "
            f"direction={self.direction}, "
            f"confirmed={self.confirmed})"
        )
    
    def to_dict(self):
        """Convert BOS model to dictionary"""
        return {
            "id": self.id,
            "indicator_id": self.indicator_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "break_type": self.break_type,
            "direction": self.direction,
            "break_value": float(self.break_value) if self.break_value else None,
            "break_percentage": float(self.break_percentage) if self.break_percentage else None,
            "swing_reference": float(self.swing_reference) if self.swing_reference else None,
            "strength": float(self.strength) if self.strength else None,
            "candle_index": self.candle_index,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "confirmed": self.confirmed,
            "confirmation_candles": self.confirmation_candles,
            "candle_data": self.candle_data,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata_": self.metadata_
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create BOS model from dictionary"""
        return cls(
            id=data.get("id"),
            indicator_id=data.get("indicator_id"),
            exchange=data.get("exchange"),
            symbol=data.get("symbol"),
            timeframe=data.get("timeframe"),
            break_type=data.get("break_type"),
            direction=data.get("direction"),
            break_value=data.get("break_value"),
            break_percentage=data.get("break_percentage"),
            swing_reference=data.get("swing_reference"),
            strength=data.get("strength"),
            candle_index=data.get("candle_index"),
            timestamp=data.get("timestamp"),
            confirmed=data.get("confirmed", False),
            confirmation_candles=data.get("confirmation_candles"),
            candle_data=data.get("candle_data"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata_=data.get("metadata_", {})
        )