from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from sqlalchemy.exc import SQLAlchemyError

from ..models.candle import CandleModel
from .base_repository import BaseRepository
from shared.domain.models.candle import CandleData


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
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol_timeframe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        timestamp: Optional[datetime] = None,
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
            timestamp: Optional specific timestamp
            start_time: Optional start timestamp
            end_time: Optional end timestamp
            limit: Optional limit on number of results
            
        Returns:
            List of Candle domain models
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.timeframe == timeframe
                )
            )
            
            if timestamp is not None:
                query = query.filter(self.model_class.timestamp == timestamp)
            else:
                if start_time is not None:
                    query = query.filter(int(self.model_class.timestamp.timestamp() * 1000) >= start_time)
                
                if end_time is not None:
                    query = query.filter(int(self.model_class.timestamp.timestamp() * 1000) <= end_time)
            
                query = query.order_by(self.model_class.timestamp)
            
            if limit is not None:
                query = query.limit(limit)
            
            db_candles = query.all()
            return [self._to_domain(c) for c in db_candles]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding candles for {exchange}/{symbol}/{timeframe}: {str(e)}"
            )
            return []
    
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
        try:
            db_candle = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.timeframe == timeframe
                )
            ).order_by(desc(self.model_class.timestamp)).first()
            
            if db_candle:
                return self._to_domain(db_candle)
            return None
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error getting latest candle for {exchange}/{symbol}/{timeframe}: {str(e)}"
            )
            return None
    
    def delete_candles_for_symbol(self, exchange: str, symbol: str) -> int:
        """
        Delete all candles for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            Number of deleted candles
        """
        try:
            deleted_count = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol
                )
            ).delete(synchronize_session=False)
            
            self.session.commit()
            return deleted_count
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(
                f"Error deleting candles for {exchange}/{symbol}: {str(e)}"
            )
            return 0
    
    def bulk_upsert_candles(self, candles: List[CandleData]) -> int:
        """
        Insert or update multiple candles in a single transaction.
        
        Args:
            candles: List of candle domain objects to upsert
            
        Returns:
            Number of successfully upserted candles
        """
        try:
            upserted_count = 0
            for candle in candles:
                # Check if candle exists
                existing = self.session.query(self.model_class).filter(
                    and_(
                        self.model_class.exchange == candle.exchange,
                        self.model_class.symbol == candle.symbol,
                        self.model_class.timeframe == candle.timeframe,
                        self.model_class.timestamp == candle.timestamp
                    )
                ).first()
                
                if existing:
                    # Update existing
                    existing.open = candle.open
                    existing.high = candle.high
                    existing.low = candle.low
                    existing.close = candle.close
                    existing.volume = candle.volume
                else:
                    # Create new
                    db_obj = self._to_db(candle)
                    self.session.add(db_obj)
                
                upserted_count += 1
            
            self.session.commit()
            return upserted_count
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error bulk upserting candles: {str(e)}")
            return 0
    
    def _to_domain(self, db_obj: CandleModel) -> CandleData:
        """
        Convert a database model to a domain model.
        
        Args:
            db_model: Database model instance
            
        Returns:
            Corresponding domain model
        """
        try:
            return CandleData(
                id=db_obj.id,
                symbol=db_obj.symbol,
                exchange=db_obj.exchange,
                timeframe=db_obj.timeframe,
                timestamp=db_obj.timestamp,
                open=db_obj.open,
                high=db_obj.high,
                low=db_obj.low,
                close=db_obj.close,
                volume=db_obj.volume,
                is_closed=db_obj.is_closed
            )
        except Exception as e:
            self.logger.error(f"Error converting DB model to domain model: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: CandleData) -> CandleModel:
        """
        Convert a domain model to a database model.
        
        Args:
            domain_obj: Domain model instance
            
        Returns:
            Corresponding Database model
        """
        try:
            return CandleModel(
                id=domain_obj.id,
                symbol=domain_obj.symbol,
                exchange=domain_obj.exchange,
                timeframe=domain_obj.timeframe,
                timestamp=domain_obj.timestamp,  # Convert to UNIX ms
                open=domain_obj.open,
                high=domain_obj.high,
                low=domain_obj.low,
                close=domain_obj.close,
                volume=domain_obj.volume,
                is_closed=domain_obj.is_closed
            )
        except Exception as e:
            self.logger.error(f"Error converting domain model to DB model: {str(e)}")
            raise