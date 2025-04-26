from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from sqlalchemy.exc import SQLAlchemyError

from data.database.models.market_context_model import MarketContextModel
from data.database.repository.base_repository import BaseRepository
from strategy.domain.models.market_context import MarketContext


class MarketContextRepository(BaseRepository[MarketContextModel]):
    """
    Repository for market context data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the market context repository.
        
        Args:
            session: The database session
        """
        super().__init__(MarketContextModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol_timeframe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str
    ) -> Optional[MarketContext]:
        """
        Find market context for a specific exchange, symbol and timeframe.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timeframe: Market timeframe
            
        Returns:
            MarketContext domain model if found, None otherwise
        """
        try:
            db_context = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.timeframe == timeframe
                )
            ).first()
            
            if db_context:
                return self._to_domain(db_context)
            return None
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding market context for {exchange}/{symbol}/{timeframe}: {str(e)}"
            )
            return None
    
    def get_latest_contexts(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        limit: int = 10
    ) -> List[MarketContext]:
        """
        Get the most recently updated market contexts.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            limit: Maximum number of results
            
        Returns:
            List of MarketContext domain models
        """
        try:
            query = self.session.query(self.model_class)
            
            if exchange:
                query = query.filter(self.model_class.exchange == exchange)
            
            if symbol:
                query = query.filter(self.model_class.symbol == symbol)
            
            query = query.order_by(desc(self.model_class.last_updated)).limit(limit)
            
            db_contexts = query.all()
            return [self._to_domain(c) for c in db_contexts]
        except SQLAlchemyError as e:
            self.logger.error(f"Error getting latest market contexts: {str(e)}")
            return []
    
    def upsert_market_context(self, market_context: MarketContext) -> Optional[MarketContext]:
        """
        Insert or update a market context based on exchange/symbol/timeframe.
        
        Args:
            market_context: MarketContext domain object to upsert
            
        Returns:
            Updated MarketContext domain model if successful, None otherwise
        """
        try:
            # Check if context exists
            existing = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == market_context.exchange,
                    self.model_class.symbol == market_context.symbol,
                    self.model_class.timeframe == market_context.timeframe
                )
            ).first()
            
            if existing:
                # Update existing context
                for key, value in vars(market_context).items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                
                # Update the last_updated timestamp
                existing.last_updated = datetime.now(timezone.utc)
            else:
                # Create new context
                existing = self._to_db(market_context)
                self.session.add(existing)
            
            self.session.commit()
            self.session.refresh(existing)
            return self._to_domain(existing)
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error upserting market context: {str(e)}")
            return None
    
    def _to_domain(self, db_obj: MarketContextModel) -> MarketContext:
        """
        Convert a database model to a domain model.
        
        Args:
            db_obj: Database model instance
            
        Returns:
            Corresponding domain model
        """
        try:
            return MarketContext(
                id=db_obj.id,
                exchange=db_obj.exchange,
                symbol=db_obj.symbol,
                timeframe=db_obj.timeframe,
                timestamp=db_obj.timestamp,
                last_updated=db_obj.last_updated,
                current_price=db_obj.current_price,
                swing_high=db_obj.swing_high,
                swing_low=db_obj.swing_low,
                trend=db_obj.trend,
                range_high=db_obj.range_high,
                range_low=db_obj.range_low,
                range_equilibrium=db_obj.range_equilibrium,
                range_size=db_obj.range_size,
                range_strength=db_obj.range_strength,
                range_detected_at=db_obj.range_detected_at,
                is_in_range=db_obj.is_in_range,
                fib_levels=db_obj.fib_levels,
                timeframe_category=db_obj.timeframe_category
            )
        except Exception as e:
            self.logger.error(f"Error converting DB model to domain model: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: MarketContext) -> MarketContextModel:
        """
        Convert a domain model to a database model.
        
        Args:
            domain_obj: Domain model instance
            
        Returns:
            Corresponding Database model
        """
        try:
            return MarketContextModel(
                id=domain_obj.id,
                exchange=domain_obj.exchange,
                symbol=domain_obj.symbol,
                timeframe=domain_obj.timeframe,
                timestamp=domain_obj.timestamp,
                last_updated=domain_obj.last_updated,
                current_price=domain_obj.current_price,
                swing_high=domain_obj.swing_high,
                swing_low=domain_obj.swing_low,
                trend=domain_obj.trend,
                range_high=domain_obj.range_high,
                range_low=domain_obj.range_low,
                range_equilibrium=domain_obj.range_equilibrium,
                range_size=domain_obj.range_size,
                range_strength=domain_obj.range_strength,
                range_detected_at=domain_obj.range_detected_at,
                is_in_range=domain_obj.is_in_range,
                fib_levels=domain_obj.fib_levels,
                timeframe_category=domain_obj.timeframe_category
            )
        except Exception as e:
            self.logger.error(f"Error converting domain model to DB model: {str(e)}")
            raise