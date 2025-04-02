from datetime import datetime, UTC
from sqlalchemy import Column, String, Float, Integer, DECIMAL, TIMESTAMP, Boolean, Index, UniqueConstraint

from .base import BaseModel


class CandleModel(BaseModel):
    """
    SQLAlchemy model for storing candle data.
    """
    
    __tablename__ = "candles"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Exchange and market information
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(5), nullable=False, index=True)
    
    # Time
    timestamp = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False, index=True)
    
    # OHLCV data
    open = Column(DECIMAL(20, 8), nullable=False)
    high = Column(DECIMAL(20, 8), nullable=False)
    low = Column(DECIMAL(20, 8), nullable=False)
    close = Column(DECIMAL(20, 8), nullable=False)
    volume = Column(DECIMAL(20, 8), nullable=False)
    
    # Additional metadata
    is_closed = Column(Boolean, nullable=False)
    # trades = Column(Integer, nullable=True)  # Number of trades, if available
    
    # Indexes and constraints
    __table_args__ = (
        # Composite index for common queries
        Index('idx_exchange_symbol_timeframe', 'exchange', 'symbol', 'timeframe'),
        
        # Ensure unique candles
        UniqueConstraint('exchange', 'symbol', 'timeframe', 'timestamp', name='uq_candle'),
    )
    
    def __repr__(self) -> str:
        return (
            f"Candle({self.exchange}:{self.symbol}:{self.timeframe}, "
            f"time={self.timestamp}, "
            f"OHLC={self.open}/{self.high}/{self.low}/{self.close}, "
            f"vol={self.volume})"
        )