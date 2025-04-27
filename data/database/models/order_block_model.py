from datetime import datetime, UTC
from sqlalchemy import Column, String, Integer, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from .base import BaseModel


class OrderBlockModel(BaseModel):
    """
    SQLAlchemy model for storing Order Block instances.
    Based on the OrderBlockDto structure in strategy/domain/dto/order_block_dto.py
    """
    
    __tablename__ = "order_blocks"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Reference to indicator registry
    indicator_id = Column(Integer, ForeignKey('indicators.id'), nullable=False)
    indicator = relationship("IndicatorModel")
    
    # Exchange and market information
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(5), nullable=False, index=True)
    
    # Order block type
    type = Column(String(10), nullable=False)  # 'demand' or 'supply'
    
    # Order block boundaries
    price_high = Column(DECIMAL(20, 8), nullable=False)  # Upper boundary of the order block
    price_low = Column(DECIMAL(20, 8), nullable=False)  # Lower boundary of the order block
    
    # Order block characteristics
    is_doji = Column(Boolean, default=False, nullable=False)  # Whether the order block is based on a doji candle
    strength = Column(DECIMAL(5, 2), nullable=True)  # Strength score (0-1)
    
    # Location and timing
    candle_index = Column(Integer, nullable=True)  # Index of the candle forming the order block
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)  # When the order block formed
    
    # Status and mitigation
    status = Column(String(20), default='active', nullable=False)  # 'active', 'mitigated', 'invalidated'
    touched = Column(Boolean, default=False, nullable=False)  # Whether price has touched the order block
    mitigation_percentage = Column(DECIMAL(5, 2), default=0.0, nullable=False)  # Percentage of the order block mitigated
    invalidated_at = Column(TIMESTAMP(timezone=True), nullable=True)  # When the order block was invalidated
    
    # Candle data (stored as JSON)
    candle_data = Column(JSONB, nullable=True)  # The candle data forming the order block
    
    # Related data references (stored as JSON)
    doji_data = Column(JSONB, nullable=True)  # Related doji candle data
    related_fvg = Column(JSONB, nullable=True)  # Related FVG data
    bos_data = Column(JSONB, nullable=True)  # Related BOS data
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    
    # Additional metadata
    metadata_ = Column(JSONB, nullable=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_order_blocks_lookup', 'exchange', 'symbol', 'timeframe'),
        Index('idx_order_blocks_type', 'type'),
        Index('idx_order_blocks_status', 'status'),
        Index('idx_order_blocks_timestamp', 'timestamp'),
        Index('idx_order_blocks_touched', 'touched'),
    )
    
    def __repr__(self) -> str:
        return (
            f"OrderBlock({self.exchange}:{self.symbol}:{self.timeframe}, "
            f"type={self.type}, "
            f"status={self.status}, "
            f"mitigation={self.mitigation_percentage}%)"
        )
    
    def to_dict(self):
        """Convert order block model to dictionary"""
        return {
            "id": self.id,
            "indicator_id": self.indicator_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "type": self.type,
            "price_high": float(self.price_high) if self.price_high else None,
            "price_low": float(self.price_low) if self.price_low else None,
            "is_doji": self.is_doji,
            "strength": float(self.strength) if self.strength else None,
            "candle_index": self.candle_index,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "status": self.status,
            "touched": self.touched,
            "mitigation_percentage": float(self.mitigation_percentage) if self.mitigation_percentage else None,
            "invalidated_at": self.invalidated_at.isoformat() if self.invalidated_at else None,
            "candle_data": self.candle_data,
            "doji_data": self.doji_data,
            "related_fvg": self.related_fvg,
            "bos_data": self.bos_data,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata_": self.metadata_
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create order block model from dictionary"""
        return cls(
            id=data.get("id"),
            indicator_id=data.get("indicator_id"),
            exchange=data.get("exchange"),
            symbol=data.get("symbol"),
            timeframe=data.get("timeframe"),
            type=data.get("type"),
            price_high=data.get("price_high"),
            price_low=data.get("price_low"),
            is_doji=data.get("is_doji", False),
            strength=data.get("strength"),
            candle_index=data.get("candle_index"),
            timestamp=data.get("timestamp"),
            status=data.get("status", "active"),
            touched=data.get("touched", False),
            mitigation_percentage=data.get("mitigation_percentage", 0.0),
            invalidated_at=data.get("invalidated_at"),
            candle_data=data.get("candle_data"),
            doji_data=data.get("doji_data"),
            related_fvg=data.get("related_fvg"),
            bos_data=data.get("bos_data"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata_=data.get("metadata_", {})
        )