from datetime import datetime, UTC
from sqlalchemy import Column, String, Integer, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from .base import BaseModel


class DojiModel(BaseModel):
    """
    SQLAlchemy model for storing Doji candle pattern instances.
    Based on the DojiDto structure in strategy/domain/dto/doji_dto.py
    """
    
    __tablename__ = "doji_candles"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Reference to indicator registry
    indicator_id = Column(Integer, ForeignKey('indicators.id'), nullable=False)
    indicator = relationship("IndicatorModel")
    
    # Exchange and market information
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(5), nullable=False, index=True)
    
    # Doji characteristics
    body_to_range_ratio = Column(DECIMAL(10, 4), nullable=False)  # Ratio of body to total range
    total_wick_size = Column(DECIMAL(20, 8), nullable=False)  # Combined size of both wicks
    strength = Column(DECIMAL(5, 2), nullable=False)  # Strength score (0-1)
    
    # Location in candle series
    candle_index = Column(Integer, nullable=True)  # Index in the candle series where doji was found
    
    # Timing information
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)  # Timestamp of the doji candle
    
    # Candle data (stored as JSON)
    candle_data = Column(JSONB, nullable=True)  # The actual candle data
    
    # Analysis metadata
    analyzed_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    
    # Additional metadata
    metadata_ = Column(JSONB, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_doji_lookup', 'exchange', 'symbol', 'timeframe'),
        Index('idx_doji_timestamp', 'timestamp'),
        Index('idx_doji_strength', 'strength'),
        Index('idx_doji_ratio', 'body_to_range_ratio'),
    )
    
    def __repr__(self) -> str:
        return (
            f"Doji({self.exchange}:{self.symbol}:{self.timeframe}, "
            f"timestamp={self.timestamp}, "
            f"strength={self.strength})"
        )
    
    def to_dict(self):
        """Convert Doji model to dictionary"""
        return {
            "id": self.id,
            "indicator_id": self.indicator_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "body_to_range_ratio": float(self.body_to_range_ratio) if self.body_to_range_ratio else None,
            "total_wick_size": float(self.total_wick_size) if self.total_wick_size else None,
            "strength": float(self.strength) if self.strength else None,
            "candle_index": self.candle_index,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "candle_data": self.candle_data,
            "analyzed_at": self.analyzed_at.isoformat() if self.analyzed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata_": self.metadata_
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create Doji model from dictionary"""
        return cls(
            id=data.get("id"),
            indicator_id=data.get("indicator_id"),
            exchange=data.get("exchange"),
            symbol=data.get("symbol"),
            timeframe=data.get("timeframe"),
            body_to_range_ratio=data.get("body_to_range_ratio"),
            total_wick_size=data.get("total_wick_size"),
            strength=data.get("strength"),
            candle_index=data.get("candle_index"),
            timestamp=data.get("timestamp"),
            candle_data=data.get("candle_data"),
            analyzed_at=data.get("analyzed_at"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata_=data.get("metadata_", {})
        )