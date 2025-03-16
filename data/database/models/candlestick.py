from datetime import datetime
from sqlalchemy import Column, Integer, Float, String, DECIMAL, TIMESTAMP, UniqueConstraint, Index
from ..base import Base

class Candlestick(Base):
    __tablename__ = "candles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    exchange = Column(String(20), nullable=False)
    timeframe = Column(String(5), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False)
    open = Column(DECIMAL(20, 8), nullable=False)
    high = Column(DECIMAL(20, 8), nullable=False)
    low = Column(DECIMAL(20, 8), nullable=False)
    close = Column(DECIMAL(20, 8), nullable=False)
    volume = Column(DECIMAL(20, 8), nullable=False)

    __table_args__ = (
        UniqueConstraint("symbol", "exchange", "timeframe", "timestamp", name="uq_candles_symbol_exchange_timeframe_timestamp"),
        Index("idx_candles_symbol_tf_ts", "symbol", "timeframe", "timestamp"),
    )