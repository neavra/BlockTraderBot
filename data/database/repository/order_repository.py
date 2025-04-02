from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from sqlalchemy.exc import SQLAlchemyError

from ..models.order_model import OrderModel
from .base_repository import BaseRepository
from shared.domain.dto.order_dto import OrderDto


class OrderRepository(BaseRepository[OrderModel]):
    """
    Repository for order data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the order repository.
        
        Args:
            session: The database session
        """
        super().__init__(OrderModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol(
        self,
        exchange: str,
        symbol: str,
        status: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderDto]:
        """
        Find orders for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            status: Optional order status filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            limit: Optional limit on number of results
            
        Returns:
            List of Order domain models
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
                query = query.filter(self.model_class.created_at >= start_time)
            
            if end_time is not None:
                query = query.filter(self.model_class.created_at <= end_time)
            
            query = query.order_by(desc(self.model_class.created_at))
            
            if limit is not None:
                query = query.limit(limit)
            
            db_orders = query.all()
            return [self._to_domain(o) for o in db_orders]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding orders for {exchange}/{symbol}: {str(e)}"
            )
            return []
    
    def find_by_signal_id(self, signal_id: int) -> List[OrderDto]:
        """
        Find all orders associated with a specific signal.
        
        Args:
            signal_id: ID of the signal
            
        Returns:
            List of Order domain models
        """
        try:
            db_orders = self.session.query(self.model_class).filter(
                self.model_class.signal_id == signal_id
            ).order_by(desc(self.model_class.created_at)).all()
            
            return [self._to_domain(o) for o in db_orders]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding orders for signal {signal_id}: {str(e)}")
            return []
    
    def get_latest_order(
        self,
        exchange: str,
        symbol: str,
        status: Optional[str] = None
    ) -> Optional[OrderDto]:
        """
        Get the latest order for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            status: Optional order status filter
            
        Returns:
            Latest Order domain model if found, None otherwise
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
            
            db_order = query.order_by(desc(self.model_class.created_at)).first()
            
            if db_order:
                return self._to_domain(db_order)
            return None
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error getting latest order for {exchange}/{symbol}: {str(e)}"
            )
            return None
    
    def bulk_upsert_orders(self, orders: List[OrderDto]) -> int:
        """
        Insert or update multiple orders in a single transaction.
        
        Args:
            orders: List of order domain objects to upsert
            
        Returns:
            Number of successfully upserted orders
        """
        try:
            upserted_count = 0
            for order in orders:
                # Check if order exists
                existing = self.session.query(self.model_class).filter(
                    self.model_class.id == order.id
                ).first()
                
                if existing:
                    # Update existing
                    for key, value in order.dict().items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                else:
                    # Create new
                    db_obj = self._to_db(order)
                    self.session.add(db_obj)
                
                upserted_count += 1
            
            self.session.commit()
            return upserted_count
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error bulk upserting orders: {str(e)}")
            return 0
    
    def _to_domain(self, db_obj: OrderModel) -> OrderDto:
        """
        Convert a database model to a domain model.
        
        Args:
            db_model: Database model instance
            
        Returns:
            Corresponding domain model
        """
        try:
            return OrderDto(
                id=db_obj.id,
                signal_id=db_obj.signal_id,
                exchange=db_obj.exchange,
                symbol=db_obj.symbol,
                order_type=db_obj.order_type,
                side=db_obj.side,
                price=db_obj.price,
                size=db_obj.size,
                value=db_obj.value,
                status=db_obj.status,
                filled_size=db_obj.filled_size,
                average_fill_price=db_obj.average_fill_price,
                fee=db_obj.fee,
                created_at=db_obj.created_at,
                updated_at=db_obj.updated_at,
                metadata=db_obj.metadata
            )
        except Exception as e:
            self.logger.error(f"Error converting DB model to domain model: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: OrderDto) -> OrderModel:
        """
        Convert a domain model to a database model.
        
        Args:
            domain_obj: Domain model instance
            
        Returns:
            Corresponding Database model
        """
        try:
            return OrderModel(
                id=domain_obj.id,
                signal_id=domain_obj.signal_id,
                exchange=domain_obj.exchange,
                symbol=domain_obj.symbol,
                order_type=domain_obj.order_type,
                side=domain_obj.side,
                price=domain_obj.price,
                size=domain_obj.size,
                value=domain_obj.value,
                status=domain_obj.status,
                filled_size=domain_obj.filled_size,
                average_fill_price=domain_obj.average_fill_price,
                fee=domain_obj.fee,
                created_at=domain_obj.created_at,
                updated_at=domain_obj.updated_at,
                metadata=domain_obj.metadata
            )
        except Exception as e:
            self.logger.error(f"Error converting domain model to DB model: {str(e)}")
            raise
