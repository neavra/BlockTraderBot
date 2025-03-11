from sqlalchemy.orm import Session
from ..models.candlestick import Candlestick

class CandleService:
    def __init__(self, db: Session):
        self.db = db

    def create_candlestick(self, candle_data):
        new_candle = Candlestick(**candle_data)
        self.db.add(new_candle)
        self.db.commit()
        self.db.refresh(new_candle)
        return new_candle

    def get_candlesticks(self, symbol: str, timeframe: str, limit: int = 100):
        return (
            self.db.query(Candlestick)
            .filter(Candlestick.symbol == symbol, Candlestick.timeframe == timeframe)
            .order_by(Candlestick.timestamp.desc())
            .limit(limit)
            .all()
        )
    
    def delete_old_candlesticks(self, symbol: str, timeframe: str, max_age):
        self.db.query(Candlestick).filter(
            Candlestick.symbol == symbol, Candlestick.timeframe == timeframe, 
            Candlestick.timestamp < max_age
        ).delete()
        self.db.commit()
