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
    
    # Custom timeframe specific fields
    is_custom_timeframe = Column(Boolean, default=False, nullable=False, index=True)
    is_complete = Column(Boolean, default=True, nullable=False)  # Only relevant for custom timeframes
    source_timeframe = Column(String(5), nullable=True)  # Base timeframe used for custom timeframe

    # Indexes and constraints
    __table_args__ = (
        # Composite index for common queries
        Index('idx_exchange_symbol_timeframe', 'exchange', 'symbol', 'timeframe'),
        
        # Custom timeframe specific index
        Index('idx_custom_timeframe', 'is_custom_timeframe', 'is_complete'),
        
        # Ensure unique candles
        UniqueConstraint('exchange', 'symbol', 'timeframe', 'timestamp', name='uq_candle'),
    )
    
    def __repr__(self) -> str:
        custom_flag = " (Custom)" if self.is_custom_timeframe else ""
        complete_flag = "" if self.is_complete else " (Partial)"
        return (
            f"Candle({self.exchange}:{self.symbol}:{self.timeframe}{custom_flag}{complete_flag}, "
            f"time={self.timestamp}, "
            f"OHLC={self.open}/{self.high}/{self.low}/{self.close}, "
            f"vol={self.volume})"
        )