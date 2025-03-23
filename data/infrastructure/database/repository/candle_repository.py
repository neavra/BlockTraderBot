from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc

from ..models.candle import CandleModel
from .base_repository import BaseRepository
from domain.models.candle import CandleData


class CandleRepository(BaseRepository[CandleModel]):
    """
    Repository for candle data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the candle repository.
        
        Args:
            session: The database session
        """
        super().__init__(CandleModel, session)
    
    def find_by_exchange_symbol_timeframe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[CandleData]:
        """
        Find candles for a specific exchange, symbol and timeframe.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timeframe: Candle timeframe
            start_time: Optional start timestamp
            end_time: Optional end timestamp
            limit: Optional limit on number of results
            
        Returns:
            List of Candle domain models
        """
        query = self.session.query(self.model_class).filter(
            and_(
                self.model_class.exchange == exchange,
                self.model_class.symbol == symbol,
                self.model_class.timeframe == timeframe
            )
        )
        
        if start_time is not None:
            query = query.filter(self.model_class.timestamp >= start_time)
        
        if end_time is not None:
            query = query.filter(self.model_class.timestamp <= end_time)
        
        query = query.order_by(self.model_class.timestamp)
        
        if limit is not None:
            query = query.limit(limit)
        
        db_candles = query.all()
        return [self._to_domain_model(c) for c in db_candles]
    
    def get_latest_candle(
        self,
        exchange: str,
        symbol: str,
        timeframe: str
    ) -> Optional[CandleData]:
        """
        Get the latest candle for a specific exchange, symbol and timeframe.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timeframe: Candle timeframe
            
        Returns:
            Latest Candle domain model if found, None otherwise
        """
        db_candle = self.session.query(self.model_class).filter(
            and_(
                self.model_class.exchange == exchange,
                self.model_class.symbol == symbol,
                self.model_class.timeframe == timeframe
            )
        ).order_by(desc(self.model_class.timestamp)).first()
        
        if db_candle:
            return self._to_domain_model(db_candle)
        return None
    
    def save_candle(self, candle: CandleData) -> CandleData:
        """
        Save a candle to the database.
        
        Args:
            candle: Candle domain model
            
        Returns:
            Saved Candle domain model
        """
        existing = self.session.query(self.model_class).filter(
            and_(
                self.model_class.exchange == candle.exchange,
                self.model_class.symbol == candle.symbol,
                self.model_class.timeframe == candle.timeframe,
                self.model_class.timestamp == candle.timestamp
            )
        ).first()
        
        if existing:
            # Update existing candle
            existing.open = candle.open
            existing.high = candle.high
            existing.low = candle.low
            existing.close = candle.close
            existing.volume = candle.volume
            existing.trades = candle.trades
            existing.updated_at = datetime.utcnow()
            self.session.commit()
            self.session.refresh(existing)
            return self._to_domain_model(existing)
        else:
            # Create new candle
            db_candle = CandleModel(
                exchange=candle.exchange,
                symbol=candle.symbol,
                timeframe=candle.timeframe,
                timestamp=candle.timestamp,
                open=candle.open,
                high=candle.high,
                low=candle.low,
                close=candle.close,
                volume=candle.volume,
                trades=candle.trades
            )
            self.session.add(db_candle)
            self.session.commit()
            self.session.refresh(db_candle)
            return self._to_domain_model(db_candle)
    
    def save_candles(self, candles: List[CandleData]) -> List[CandleData]:
        """
        Save multiple candles to the database.
        
        Args:
            candles: List of Candle domain models
            
        Returns:
            List of saved Candle domain models
        """
        saved_candles = []
        for candle in candles:
            saved_candle = self.save_candle(candle)
            saved_candles.append(saved_candle)
        return saved_candles
    
    def delete_candles_for_symbol(self, exchange: str, symbol: str) -> int:
        """
        Delete all candles for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            Number of deleted candles
        """
        deleted_count = self.session.query(self.model_class).filter(
            and_(
                self.model_class.exchange == exchange,
                self.model_class.symbol == symbol
            )
        ).delete(synchronize_session=False)
        
        self.session.commit()
        return deleted_count
    
    def _to_domain_model(self, db_model: CandleModel) -> CandleData:
        """
        Convert a database model to a domain model.
        
        Args:
            db_model: Database model instance
            
        Returns:
            Corresponding domain model
        """
        return CandleData(
            exchange=db_model.exchange,
            symbol=db_model.symbol,
            timeframe=db_model.timeframe,
            timestamp=db_model.timestamp,
            open=db_model.open,
            high=db_model.high,
            low=db_model.low,
            close=db_model.close,
            volume=db_model.volume,
            trades=db_model.trades
        )