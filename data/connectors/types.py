from pydantic import BaseModel
# Data model using Pydantic
class Candle(BaseModel):
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float