from datetime import datetime, UTC
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index, UniqueConstraint, JSON
from sqlalchemy.orm import relationship

from .base import BaseModel


class SignalModel(BaseModel):
    """
    SQLAlchemy model for storing trading signals.
    """
    
    __tablename__ = "signals"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Signal information
    strategy_name = Column(String(50), nullable=False)
    exchange = Column(String(50), nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False)
    
    # Signal details
    direction = Column(String(10), nullable=False)  # long, short
    signal_type = Column(String(20), nullable=False)  # entry, exit, adjust
    price_target = Column(DECIMAL(20, 8), nullable=True)
    stop_loss = Column(DECIMAL(20, 8), nullable=True)
    take_profit = Column(DECIMAL(20, 8), nullable=True)
    
    # Risk metrics
    risk_reward_ratio = Column(DECIMAL(5, 2), nullable=True)
    confidence_score = Column(DECIMAL(5, 2), nullable=True)
    
    # Signal state
    execution_status = Column(String(20), default='pending', nullable=False, index=True)
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    
    # Additional metadata
    metadata_ = Column(JSON, nullable=True)
    
    # Relationships - this doesn't require orders to exist
    orders = relationship("OrderModel", back_populates="signal")
    
    indicator_id = Column(Integer, nullable=True)
    # Optional reference to an indicator (like an order block)
    #indicator_id = Column(Integer, ForeignKey("indicators.id"), nullable=True)
    #indicator = relationship("IndicatorModel")
    
    # Indexes
    __table_args__ = (
        Index('idx_signals_lookup', 'exchange', 'symbol', 'execution_status'),
        Index('idx_signals_created', 'created_at'),
    )
    
    def __repr__(self) -> str:
        return (
            f"Signal({self.exchange}:{self.symbol}, "
            f"id={self.id}, "
            f"strategy={self.strategy_name}, "
            f"direction={self.direction}, "
            f"type={self.signal_type}, "
            f"status={self.execution_status})"
        )
    
    def to_dict(self):
        """Convert signal model to dictionary"""
        return {
            "id": self.id,
            "strategy_name": self.strategy_name,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "direction": self.direction,
            "signal_type": self.signal_type,
            "price_target": float(self.price_target) if self.price_target else None,
            "stop_loss": float(self.stop_loss) if self.stop_loss else None,
            "take_profit": float(self.take_profit) if self.take_profit else None,
            "risk_reward_ratio": float(self.risk_reward_ratio) if self.risk_reward_ratio else None,
            "confidence_score": float(self.confidence_score) if self.confidence_score else None,
            "execution_status": self.execution_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata_": self.metadata_,
            "indicator_id": self.indicator_id
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create signal model from dictionary"""
        return cls(
            id=data.get("id"),
            strategy_name=data.get("strategy_name"),
            exchange=data.get("exchange"),
            symbol=data.get("symbol"),
            timeframe=data.get("timeframe"),
            direction=data.get("direction"),
            signal_type=data.get("signal_type"),
            price_target=data.get("price_target"),
            stop_loss=data.get("stop_loss"),
            take_profit=data.get("take_profit"),
            risk_reward_ratio=data.get("risk_reward_ratio"),
            confidence_score=data.get("confidence_score"),
            execution_status=data.get("execution_status", "pending"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata_=data.get("metadata_", {}),
            indicator_id=data.get("indicator_id")
        )
