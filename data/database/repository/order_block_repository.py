from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, or_
from sqlalchemy.exc import SQLAlchemyError

from data.database.models.order_block_model import OrderBlockModel
from data.database.repository.base_repository import BaseRepository


class OrderBlockRepository(BaseRepository[OrderBlockModel]):
    """
    Repository for order block data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the order block repository.
        
        Args:
            session: The database session
        """
        super().__init__(OrderBlockModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol_timeframe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        status: Optional[str] = None,
        block_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Find order blocks for a specific exchange, symbol and timeframe.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timeframe: Candle timeframe
            status: Optional status filter ('active', 'mitigated', 'invalidated')
            block_type: Optional type filter ('demand', 'supply')
            start_time: Optional start time filter
            end_time: Optional end time filter
            limit: Optional limit on number of results
            
        Returns:
            List of order block dictionaries
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.timeframe == timeframe
                )
            )
            
            if status is not None:
                query = query.filter(self.model_class.status == status)
            
            if block_type is not None:
                query = query.filter(self.model_class.type == block_type)
            
            if start_time is not None:
                query = query.filter(self.model_class.timestamp >= start_time)
            
            if end_time is not None:
                query = query.filter(self.model_class.timestamp <= end_time)
            
            query = query.order_by(desc(self.model_class.timestamp))
            
            if limit is not None:
                query = query.limit(limit)
            
            db_blocks = query.all()
            return [block.to_dict() for block in db_blocks]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding order blocks for {exchange}/{symbol}/{timeframe}: {str(e)}"
            )
            return []
    
    def find_active_order_blocks(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        block_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Find all active order blocks, optionally filtered by exchange, symbol, timeframe and type.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            block_type: Optional type filter ('demand', 'supply')
            
        Returns:
            List of active order block dictionaries
        """
        try:
            query = self.session.query(self.model_class).filter(
                self.model_class.status == 'active'
            )
            
            if exchange is not None:
                query = query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                query = query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                query = query.filter(self.model_class.timeframe == timeframe)
            
            if block_type is not None:
                query = query.filter(self.model_class.type == block_type)
            
            query = query.order_by(desc(self.model_class.timestamp))
            
            db_blocks = query.all()
            return [block.to_dict() for block in db_blocks]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding active order blocks: {str(e)}")
            return []
    
    def find_active_indicators_in_price_range(
        self,
        exchange: str,
        symbol: str,
        min_price: float,
        max_price: float,
        timeframes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Find active order blocks that overlap with a specified price range.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            min_price: Lower bound of the price range
            max_price: Upper bound of the price range
            timeframes: Optional list of timeframes to filter on
            
        Returns:
            List of order block dictionaries that overlap with the price range
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.status.in_(['active']),
                    # Find blocks where either:
                    # 1. Block's low is within our range
                    # 2. Block's high is within our range
                    # 3. Block completely contains our range
                    or_(
                        and_(self.model_class.price_low >= min_price, self.model_class.price_low <= max_price),
                        and_(self.model_class.price_high >= min_price, self.model_class.price_high <= max_price),
                        and_(self.model_class.price_low <= min_price, self.model_class.price_high >= max_price)
                    )
                )
            )
            
            if timeframes:
                query = query.filter(self.model_class.timeframe.in_(timeframes))
            
            db_blocks = query.all()
            return [block.to_dict() for block in db_blocks]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding order blocks in price range for {exchange}/{symbol}: {str(e)}"
            )
            return []
    
    def update_indicator_status(
        self,
        block_id: int,
        status: str,
        mitigation_percentage: Optional[float] = None,
        touched: Optional[bool] = None
    ) -> bool:
        """
        Update the status and mitigation details of an order block.
        
        Args:
            block_id: ID of the order block
            status: New status ('active', 'mitigated', 'invalidated')
            mitigation_percentage: Optional new mitigation percentage
            touched: Optional new touched status
            
        Returns:
            True if the update was successful, False otherwise
        """
        try:
            block = self.session.query(self.model_class).filter(
                self.model_class.id == block_id
            ).first()
            
            if not block:
                self.logger.warning(f"Order block with ID {block_id} not found")
                return False
            
            block.status = status
            block.updated_at = datetime.now(timezone.utc)
            
            if mitigation_percentage is not None:
                block.mitigation_percentage = mitigation_percentage
            
            if touched is not None:
                block.touched = touched
            
            if status in ('mitigated', 'invalidated'):
                block.invalidated_at = datetime.now(timezone.utc)
            
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error updating status for order block ID {block_id}: {str(e)}")
            return False
    
    def calculate_order_block_stats(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        block_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Calculate statistics about order blocks.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            block_type: Optional type filter ('demand', 'supply')
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            Dictionary containing statistics:
                - active_count: Number of active order blocks
                - mitigated_count: Number of mitigated order blocks
                - invalidated_count: Number of invalidated order blocks
                - demand_count: Number of demand order blocks
                - supply_count: Number of supply order blocks
                - touched_count: Number of touched order blocks
                - avg_mitigation: Average mitigation percentage
                - avg_strength: Average strength score
        """
        try:
            base_query = self.session.query(self.model_class)
            
            if exchange is not None:
                base_query = base_query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                base_query = base_query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                base_query = base_query.filter(self.model_class.timeframe == timeframe)
            
            if block_type is not None:
                base_query = base_query.filter(self.model_class.type == block_type)
            
            if start_time is not None:
                base_query = base_query.filter(self.model_class.timestamp >= start_time)
            
            if end_time is not None:
                base_query = base_query.filter(self.model_class.timestamp <= end_time)
            
            # Count by status
            active_count = base_query.filter(self.model_class.status == 'active').count()
            mitigated_count = base_query.filter(self.model_class.status == 'mitigated').count()
            invalidated_count = base_query.filter(self.model_class.status == 'invalidated').count()
            
            # Count by type
            demand_count = base_query.filter(self.model_class.type == 'demand').count()
            supply_count = base_query.filter(self.model_class.type == 'supply').count()
            
            # Count touched blocks
            touched_count = base_query.filter(self.model_class.touched == True).count()
            
            # Calculate averages
            avg_mitigation = base_query.with_entities(
                func.avg(self.model_class.mitigation_percentage)
            ).scalar() or 0.0
            
            avg_strength = base_query.with_entities(
                func.avg(self.model_class.strength)
            ).scalar() or 0.0
            
            return {
                "active_count": active_count,
                "mitigated_count": mitigated_count,
                "invalidated_count": invalidated_count,
                "demand_count": demand_count,
                "supply_count": supply_count,
                "touched_count": touched_count,
                "avg_mitigation": float(avg_mitigation),
                "avg_strength": float(avg_strength),
                "total_count": active_count + mitigated_count + invalidated_count
            }
        except SQLAlchemyError as e:
            self.logger.error(f"Error calculating order block statistics: {str(e)}")
            return {
                "active_count": 0,
                "mitigated_count": 0,
                "invalidated_count": 0,
                "demand_count": 0,
                "supply_count": 0,
                "touched_count": 0,
                "avg_mitigation": 0.0,
                "avg_strength": 0.0,
                "total_count": 0
            }
        
    async def create_order_block(self, order_block_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a new order block record in the database.
        
        Args:
            order_block_data: Dictionary containing order block data
            
        Returns:
            Dictionary representation of the created order block, or None if creation failed
        """
        try:
            # Convert dictionary to model
            order_block_model = OrderBlockModel.from_dict(order_block_data)
            
            # Add to session
            self.session.add(order_block_model)
            self.session.commit()
            
            # Refresh to get updated values (like auto-generated ID)
            self.session.refresh(order_block_model)
            
            # Convert back to dictionary and return
            return order_block_model.to_dict()
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error creating order block: {str(e)}")
            return None

    async def bulk_create_order_blocks(self, order_blocks_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Create multiple order blocks in a single transaction.
        
        Args:
            order_blocks_data: List of dictionaries containing order block data
            
        Returns:
            List of created order blocks as dictionaries
        """
        created_blocks = []
        try:
            # Convert dictionaries to models
            order_block_models = [OrderBlockModel.from_dict(data) for data in order_blocks_data]
            
            # Add all to session
            self.session.add_all(order_block_models)
            self.session.commit()
            
            # Refresh to get updated values
            for model in order_block_models:
                self.session.refresh(model)
                created_blocks.append(model.to_dict())
                
            return created_blocks
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error bulk creating order blocks: {str(e)}")
            return []

    async def update_order_block(self, block_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Update an existing order block.
        
        Args:
            block_id: ID of the order block to update
            update_data: Dictionary containing fields to update
            
        Returns:
            Updated order block as dictionary, or None if update failed
        """
        try:
            # Find the order block
            order_block = self.session.query(self.model_class).filter(
                self.model_class.id == block_id
            ).first()
            
            if not order_block:
                self.logger.warning(f"Order block with ID {block_id} not found")
                return None
            
            # Update fields
            for key, value in update_data.items():
                if hasattr(order_block, key):
                    setattr(order_block, key, value)
            
            # Set updated_at timestamp
            order_block.updated_at = datetime.now(timezone.utc)
            
            # Commit changes
            self.session.commit()
            self.session.refresh(order_block)
            
            # Convert to dictionary and return
            return order_block.to_dict()
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error updating order block with ID {block_id}: {str(e)}")
            return None
    
    def _to_domain(self, db_obj: OrderBlockModel) -> Dict[str, Any]:
        """
        Convert a database model to a domain model.
        In this case, the to_dict method of the model is used directly.
        
        Args:
            db_obj: Database model instance
            
        Returns:
            Dictionary representation of the order block
        """
        try:
            return db_obj.to_dict()
        except Exception as e:
            self.logger.error(f"Error converting DB model to dictionary: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: Dict[str, Any]) -> OrderBlockModel:
        """
        Convert a domain model to a database model.
        In this case, the from_dict method of the model is used.
        
        Args:
            domain_obj: Dictionary data
            
        Returns:
            Database model instance
        """
        try:
            return OrderBlockModel.from_dict(domain_obj)
        except Exception as e:
            self.logger.error(f"Error converting dictionary to DB model: {str(e)}")
            raise