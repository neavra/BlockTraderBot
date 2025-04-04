from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from sqlalchemy.exc import SQLAlchemyError

from ..models.position_model import PositionModel
from .base_repository import BaseRepository
from shared.domain.dto.position_dto import PositionDto


class PositionRepository(BaseRepository[PositionModel]):
    """
    Repository for position data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the position repository.
        
        Args:
            session: The database session
        """
        super().__init__(PositionModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol(
        self,
        exchange: str,
        symbol: str,
        status: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[PositionDto]:
        """
        Find positions for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            status: Optional position status filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            limit: Optional limit on number of results
            
        Returns:
            List of Position domain models
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol
                )
            )
            
            if status is not None:
                query = query.filter(self.model_class.status == status)
            
            if start_time is not None:
                query = query.filter(self.model_class.entry_time >= start_time)
            
            if end_time is not None:
                query = query.filter(self.model_class.entry_time <= end_time)
            
            query = query.order_by(desc(self.model_class.entry_time))
            
            if limit is not None:
                query = query.limit(limit)
            
            db_positions = query.all()
            return [self._to_domain(p) for p in db_positions]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding positions for {exchange}/{symbol}: {str(e)}"
            )
            return []
    
    def find_open_positions(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None
    ) -> List[PositionDto]:
        """
        Find all open positions, optionally filtered by exchange and symbol.
        
        Args:
            exchange: Optional exchange name filter
            symbol: Optional trading symbol filter
            
        Returns:
            List of Position domain models
        """
        try:
            query = self.session.query(self.model_class).filter(
                self.model_class.status == "open"
            )
            
            if exchange is not None:
                query = query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                query = query.filter(self.model_class.symbol == symbol)
            
            query = query.order_by(desc(self.model_class.entry_time))
            
            db_positions = query.all()
            return [self._to_domain(p) for p in db_positions]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding open positions: {str(e)}")
            return []
    
    def get_latest_position(
        self,
        exchange: str,
        symbol: str,
        status: Optional[str] = None
    ) -> Optional[PositionDto]:
        """
        Get the latest position for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            status: Optional position status filter
            
        Returns:
            Latest Position domain model if found, None otherwise
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol
                )
            )
            
            if status is not None:
                query = query.filter(self.model_class.status == status)
            
            db_position = query.order_by(desc(self.model_class.entry_time)).first()
            
            if db_position:
                return self._to_domain(db_position)
            return None
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error getting latest position for {exchange}/{symbol}: {str(e)}"
            )
            return None
    
    def bulk_upsert_positions(self, positions: List[PositionDto]) -> int:
        """
        Insert or update multiple positions in a single transaction.
        
        Args:
            positions: List of position domain objects to upsert
            
        Returns:
            Number of successfully upserted positions
        """
        try:
            upserted_count = 0
            for position in positions:
                # Check if position exists
                existing = self.session.query(self.model_class).filter(
                    self.model_class.id == position.id
                ).first()
                
                if existing:
                    # Update existing
                    for key, value in position.dict().items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                else:
                    # Create new
                    db_obj = self._to_db(position)
                    self.session.add(db_obj)
                
                upserted_count += 1
            
            self.session.commit()
            return upserted_count
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error bulk upserting positions: {str(e)}")
            return 0
    
    def _to_domain(self, db_obj: PositionModel) -> PositionDto:
        """
        Convert a database model to a domain model.
        
        Args:
            db_model: Database model instance
            
        Returns:
            Corresponding domain model
        """
        try:
            return PositionDto(
                id=db_obj.id,
                symbol=db_obj.symbol,
                side=db_obj.side,
                size=db_obj.size,
                entry_price=db_obj.entry_price,
                current_price=db_obj.current_price,
                pnl=db_obj.pnl,
                pnl_percent=db_obj.pnl_percent,
                timestamp=db_obj.created_at
            )
        except Exception as e:
            self.logger.error(f"Error converting DB model to domain model: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: PositionDto) -> PositionModel:
        """
        Convert a domain model to a database model.
        
        Args:
            domain_obj: Domain model instance
            
        Returns:
            Corresponding Database model
        """
        try:
            return PositionModel(
                id=domain_obj.id,
                exchange=domain_obj.exchange,
                symbol=domain_obj.symbol,
                side=domain_obj.side,
                size=domain_obj.size,
                entry_price=domain_obj.entry_price,
                current_price=domain_obj.current_price,
                liquidation_price=domain_obj.liquidation_price,
                stop_loss=domain_obj.stop_loss,
                take_profit=domain_obj.take_profit,
                pnl=domain_obj.pnl,
                pnl_percent=domain_obj.pnl_percent,
                status=domain_obj.status,
                leverage=domain_obj.leverage,
                entry_time=domain_obj.entry_time,
                exit_time=domain_obj.exit_time,
                created_at=domain_obj.created_at,
                updated_at=domain_obj.updated_at,
                metadata=domain_obj.metadata
            )
        except Exception as e:
            self.logger.error(f"Error converting domain model to DB model: {str(e)}")
            raise 