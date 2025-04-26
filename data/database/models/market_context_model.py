from datetime import datetime, UTC
from sqlalchemy import Column, String, Integer, DECIMAL, TIMESTAMP, Boolean, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB

from .base import BaseModel


class MarketContextModel(BaseModel):
    """
    SQLAlchemy model for storing market context data.
    This table holds market state information for specific symbol/timeframe combinations.
    """
    
    __tablename__ = "market_context"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Exchange and market information
    exchange = Column(String(20), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(5), nullable=False, index=True)
    
    # Timing information
    timestamp = Column(TIMESTAMP(timezone=True), nullable=True)
    last_updated = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(UTC), nullable=False)
    
    # Price data
    current_price = Column(DECIMAL(20, 8), nullable=True)
    
    # Swing points (stored as JSON)
    swing_high = Column(JSONB, nullable=True)
    swing_low = Column(JSONB, nullable=True)
    
    # Trend information
    trend = Column(String(10), nullable=True)
    
    # Range information
    range_high = Column(DECIMAL(20, 8), nullable=True)
    range_low = Column(DECIMAL(20, 8), nullable=True)
    range_equilibrium = Column(DECIMAL(20, 8), nullable=True)
    range_size = Column(DECIMAL(10, 5), nullable=True)
    range_strength = Column(DECIMAL(10, 5), nullable=True)
    range_detected_at = Column(TIMESTAMP(timezone=True), nullable=True)
    is_in_range = Column(Boolean, nullable=True)
    
    # Fibonacci levels (stored as JSON)
    fib_levels = Column(JSONB, nullable=True)
    
    # Timeframe category
    timeframe_category = Column(String(10), nullable=True)
    
    # Indexes and constraints
    __table_args__ = (
        # Composite index for common queries
        Index('idx_market_context_symbol_tf', 'exchange', 'symbol', 'timeframe'),
        
        # Ensure unique market context per exchange/symbol/timeframe combination
        UniqueConstraint('exchange', 'symbol', 'timeframe', name='uq_market_context'),
    )
    
    def __repr__(self) -> str:
        return (
            f"MarketContext({self.exchange}:{self.symbol}:{self.timeframe}, "
            f"price={self.current_price}, "
            f"trend={self.trend}, "
            f"range={self.is_in_range})"
        )