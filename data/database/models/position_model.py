from datetime import datetime, UTC
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, Boolean, ForeignKey, Index, UniqueConstraint, JSON
from sqlalchemy.orm import relationship

from .base import BaseModel


class PositionModel(BaseModel):
    """
    SQLAlchemy model for storing position data.
    """
    
    __tablename__ = "positions"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Exchange and market information
    exchange = Column(String(50), nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    
    # Position details
    side = Column(String(10), nullable=False)  # long, short
    size = Column(DECIMAL(20, 8), nullable=False)
    entry_price = Column(DECIMAL(20, 8), nullable=False)
    current_price = Column(DECIMAL(20, 8), nullable=True)
    liquidation_price = Column(DECIMAL(20, 8), nullable=True)
    
    # Risk management
    stop_loss = Column(DECIMAL(20, 8), nullable=True)
    take_profit = Column(DECIMAL(20, 8), nullable=True)
    
    # Performance metrics
    pnl = Column(DECIMAL(20, 8), nullable=True)
    pnl_percent = Column(DECIMAL(10, 2), nullable=True)
    
    # Position state
    status = Column(String(20), nullable=False, index=True)  # open, closed
    leverage = Column(DECIMAL(10, 2), default=1.0, nullable=False)
    
    # Timestamps
    entry_time = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    exit_time = Column(TIMESTAMP(timezone=True), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC), nullable=False)
    
    # Additional metadata stored as JSON
    metadata_ = Column(JSON, nullable=True)
    
    # Relationships
    # orders = relationship("OrderModel", back_populates="position")
    
    # Indexes and constraints
    __table_args__ = (
        # Composite indexes for common queries
        Index('idx_positions_exchange_symbol', 'exchange', 'symbol'),
        Index('idx_positions_status', 'status'),
        Index('idx_positions_entry_time', 'entry_time'),
    )
    
    def __repr__(self) -> str:
        return (
            f"Position({self.exchange}:{self.symbol}, "
            f"id={self.id}, "
            f"side={self.side}, "
            f"size={self.size}, "
            f"status={self.status}, "
            f"pnl={self.pnl})"
        )
    
    def to_dict(self):
        """Convert position model to dictionary"""
        return {
            "id": self.id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "side": self.side,
            "size": float(self.size),
            "entry_price": float(self.entry_price),
            "current_price": float(self.current_price) if self.current_price else None,
            "liquidation_price": float(self.liquidation_price) if self.liquidation_price else None,
            "stop_loss": float(self.stop_loss) if self.stop_loss else None,
            "take_profit": float(self.take_profit) if self.take_profit else None,
            "pnl": float(self.pnl) if self.pnl else None,
            "pnl_percent": float(self.pnl_percent) if self.pnl_percent else None,
            "status": self.status,
            "leverage": float(self.leverage),
            "entry_time": self.entry_time.isoformat() if self.entry_time else None,
            "exit_time": self.exit_time.isoformat() if self.exit_time else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "metadata_": self.metadata_
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create position model from dictionary"""
        return cls(
            id=data.get("id"),
            exchange=data.get("exchange"),
            symbol=data.get("symbol"),
            side=data.get("side"),
            size=data.get("size"),
            entry_price=data.get("entry_price"),
            current_price=data.get("current_price"),
            liquidation_price=data.get("liquidation_price"),
            stop_loss=data.get("stop_loss"),
            take_profit=data.get("take_profit"),
            pnl=data.get("pnl"),
            pnl_percent=data.get("pnl_percent"),
            status=data.get("status", "open"),
            leverage=data.get("leverage", 1.0),
            entry_time=data.get("entry_time"),
            exit_time=data.get("exit_time"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata_=data.get("metadata_", {})
        )
    
    def update_pnl(self, current_price):
        """Update the position's PnL based on current price"""
        self.current_price = current_price
        
        # Calculate PnL
        if self.side == "long":
            self.pnl = float(self.size) * (float(current_price) - float(self.entry_price))
        else:  # short
            self.pnl = float(self.size) * (float(self.entry_price) - float(current_price))
        
        # Calculate PnL percentage
        self.pnl_percent = (float(self.pnl) / (float(self.entry_price) * float(self.size))) * 100
        
        return self.pnl
    
    def is_open(self):
        """Check if the position is open"""
        return self.status == "open"
    
    def close(self, exit_price):
        """Close the position at a specified exit price"""
        self.current_price = exit_price
        self.update_pnl(exit_price)
        self.status = "closed"
        self.exit_time = datetime.now(UTC)
        return self
