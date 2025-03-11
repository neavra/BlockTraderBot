from sqlalchemy import Column, Integer, Float, String, DateTime
from ..base import Base

class Candlestick(Base):
    __tablename__ = "candlesticks"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    timeframe = Column(String, index=True)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    timestamp = Column(DateTime, index=True, nullable=False)
