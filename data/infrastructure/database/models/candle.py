from sqlalchemy import Column, String, Float, Integer, Index, UniqueConstraint

from ..base import BaseModel


class CandleModel(BaseModel):
    """
    SQLAlchemy model for storing candle data.
    """
    
    __tablename__ = "candles"
    
    # Exchange and market information
    exchange = Column(String(50), nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    timeframe = Column(String(20), nullable=False, index=True)
    
    # Time
    timestamp = Column(Integer, nullable=False, index=True)  # Unix timestamp in milliseconds
    
    # OHLCV data
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    
    # Additional metadata
    trades = Column(Integer, nullable=True)  # Number of trades, if available
    
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