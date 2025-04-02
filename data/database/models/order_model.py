from datetime import datetime, UTC
from sqlalchemy import Column, String, Integer, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index, UniqueConstraint, JSON
from sqlalchemy.orm import relationship

from .base import BaseModel


class OrderModel(BaseModel):
    """
    SQLAlchemy model for storing order data.
    """
    
    __tablename__ = "orders"

    # Primary Key - Using exchange's order ID as primary key
    id = Column(String(100), primary_key=True)
    
    # Foreign key to signal that generated this order (if applicable)
    signal_id = Column(Integer, ForeignKey("signals.id"), nullable=True, index=True)
    
    # Exchange and market information
    exchange = Column(String(50), nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    
    # Order details
    order_type = Column(String(20), nullable=False)  # market, limit, stop, etc.
    side = Column(String(10), nullable=False)  # buy, sell
    price = Column(DECIMAL(20, 8), nullable=True)  # Null for market orders
    size = Column(DECIMAL(20, 8), nullable=False)
    value = Column(DECIMAL(20, 8), nullable=True)  # Price * Size
    
    # Order state
    status = Column(String(20), nullable=False, index=True)  # open, filled, cancelled, rejected
    filled_size = Column(DECIMAL(20, 8), nullable=False, default=0)
    average_fill_price = Column(DECIMAL(20, 8), nullable=True)
    fee = Column(DECIMAL(20, 8), nullable=True)
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    
    # Additional metadata stored as JSON
    metadata = Column(JSON, nullable=True)
    
    # Relationships
    signal = relationship("SignalModel", back_populates="orders")
    position = relationship("PositionModel", back_populates="orders")
    
    # Indexes and constraints
    __table_args__ = (
        # Composite indexes for common queries
        Index('idx_orders_exchange_symbol', 'exchange', 'symbol'),
        Index('idx_orders_status', 'status'),
        Index('idx_orders_created', 'created_at'),
    )
    
    def __repr__(self) -> str:
        return (
            f"Order({self.exchange}:{self.symbol}, "
            f"id={self.id}, "
            f"type={self.order_type}, "
            f"side={self.side}, "
            f"status={self.status}, "
            f"price={self.price}, "
            f"size={self.size})"
        )
    
    def to_dict(self):
        """Convert order model to dictionary"""
        return {
            "id": self.id,
            "signal_id": self.signal_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "order_type": self.order_type,
            "side": self.side,
            "price": float(self.price) if self.price else None,
            "size": float(self.size),
            "value": float(self.value) if self.value else None,
            "status": self.status,
            "filled_size": float(self.filled_size),
            "average_fill_price": float(self.average_fill_price) if self.average_fill_price else None,
            "fee": float(self.fee) if self.fee else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create order model from dictionary"""
        return cls(
            id=data.get("id"),
            signal_id=data.get("signal_id"),
            exchange=data.get("exchange"),
            symbol=data.get("symbol"),
            order_type=data.get("order_type"),
            side=data.get("side"),
            price=data.get("price"),
            size=data.get("size"),
            value=data.get("value"),
            status=data.get("status", "open"),
            filled_size=data.get("filled_size", 0),
            average_fill_price=data.get("average_fill_price"),
            fee=data.get("fee"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata=data.get("metadata", {})
        )
