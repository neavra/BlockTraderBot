from pydantic import BaseModel, Field
from datetime import datetime
from decimal import Decimal


class CandleSchema(BaseModel):
    symbol: str = Field(..., max_length=20, description="Trading pair symbol (e.g., BTCUSDT)")
    exchange: str = Field(..., max_length=20, description="Exchange name (e.g., Binance)")
    timeframe: str = Field(..., max_length=5, description="Timeframe (e.g., 1m, 5m, 1h)")
    timestamp: datetime = Field(..., description="Timestamp of the candlestick")
    open: Decimal = Field(..., description="Open price of the candlestick")
    high: Decimal = Field(..., description="High price of the candlestick")
    low: Decimal = Field(..., description="Low price of the candlestick")
    close: Decimal = Field(..., description="Close price of the candlestick")
    volume: Decimal = Field(..., description="Trading volume during the candlestick")

    class Config:
        orm_mode = True  # Allows conversion from SQLAlchemy models

