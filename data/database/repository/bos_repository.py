from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, or_
from sqlalchemy.exc import SQLAlchemyError

from data.database.models.bos_model import BosModel
from data.database.repository.base_repository import BaseRepository


class BosRepository(BaseRepository[BosModel]):
    """
    Repository for Breaking of Structure (BOS) data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the BOS repository.
        
        Args:
            session: The database session
        """
        super().__init__(BosModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol_timeframe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        break_type: Optional[str] = None,
        direction: Optional[str] = None,
        confirmed: Optional[bool] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        min_break_percentage: Optional[float] = None,
        min_strength: Optional[float] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Find BOS events for a specific exchange, symbol and timeframe.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timeframe: Candle timeframe
            break_type: Optional break type filter ('higher_high', 'lower_low', 'higher_low', 'lower_high')
            direction: Optional direction filter ('bullish', 'bearish')
            confirmed: Optional confirmation status filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            min_break_percentage: Optional minimum break percentage filter
            min_strength: Optional minimum strength filter
            limit: Optional limit on number of results
            
        Returns:
            List of BOS dictionaries
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.timeframe == timeframe
                )
            )
            
            if break_type is not None:
                query = query.filter(self.model_class.break_type == break_type)
            
            if direction is not None:
                query = query.filter(self.model_class.direction == direction)
            
            if confirmed is not None:
                query = query.filter(self.model_class.confirmed == confirmed)
            
            if start_time is not None:
                query = query.filter(self.model_class.timestamp >= start_time)
            
            if end_time is not None:
                query = query.filter(self.model_class.timestamp <= end_time)
            
            if min_break_percentage is not None:
                query = query.filter(self.model_class.break_percentage >= min_break_percentage)
            
            if min_strength is not None:
                query = query.filter(self.model_class.strength >= min_strength)
            
            query = query.order_by(desc(self.model_class.timestamp))
            
            if limit is not None:
                query = query.limit(limit)
            
            db_bos_events = query.all()
            return [bos.to_dict() for bos in db_bos_events]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding BOS events for {exchange}/{symbol}/{timeframe}: {str(e)}"
            )
            return []
    
    def find_recent_bos_events(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        direction: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find recent BOS events, optionally filtered by exchange, symbol, timeframe and direction.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            direction: Optional direction filter ('bullish', 'bearish')
            limit: Maximum number of results (default: 10)
            
        Returns:
            List of recent BOS dictionaries
        """
        try:
            query = self.session.query(self.model_class)
            
            if exchange is not None:
                query = query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                query = query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                query = query.filter(self.model_class.timeframe == timeframe)
            
            if direction is not None:
                query = query.filter(self.model_class.direction == direction)
            
            query = query.order_by(desc(self.model_class.timestamp)).limit(limit)
            
            db_bos_events = query.all()
            return [bos.to_dict() for bos in db_bos_events]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding recent BOS events: {str(e)}")
            return []
    
    def find_by_swing_reference(
        self,
        exchange: str,
        symbol: str,
        swing_reference: float,
        tolerance: float = 0.001
    ) -> List[Dict[str, Any]]:
        """
        Find BOS events that reference a specific swing level.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            swing_reference: The swing reference price
            tolerance: Tolerance for price comparison (default: 0.001)
            
        Returns:
            List of BOS dictionaries that reference the swing level
        """
        try:
            # Calculate tolerance bounds
            lower_bound = swing_reference * (1 - tolerance)
            upper_bound = swing_reference * (1 + tolerance)
            
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.swing_reference >= lower_bound,
                    self.model_class.swing_reference <= upper_bound
                )
            )
            
            db_bos_events = query.all()
            return [bos.to_dict() for bos in db_bos_events]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding BOS events by swing reference for {exchange}/{symbol}: {str(e)}"
            )
            return []
    
    def update_confirmation_status(
        self,
        bos_id: int,
        confirmed: bool,
        confirmation_candles: Optional[int] = None
    ) -> bool:
        """
        Update the confirmation status of a BOS event.
        
        Args:
            bos_id: ID of the BOS event
            confirmed: New confirmation status
            confirmation_candles: Optional number of candles confirming the break
            
        Returns:
            True if the update was successful, False otherwise
        """
        try:
            bos = self.session.query(self.model_class).filter(
                self.model_class.id == bos_id
            ).first()
            
            if not bos:
                self.logger.warning(f"BOS event with ID {bos_id} not found")
                return False
            
            bos.confirmed = confirmed
            bos.updated_at = datetime.now(timezone.utc)
            
            if confirmation_candles is not None:
                bos.confirmation_candles = confirmation_candles
            
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error updating confirmation status for BOS ID {bos_id}: {str(e)}")
            return False
    
    def calculate_bos_stats(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        break_type: Optional[str] = None,
        direction: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Calculate statistics about BOS events.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            break_type: Optional break type filter
            direction: Optional direction filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            Dictionary containing statistics:
                - confirmed_count: Number of confirmed BOS events
                - unconfirmed_count: Number of unconfirmed BOS events
                - bullish_count: Number of bullish BOS events
                - bearish_count: Number of bearish BOS events
                - avg_break_percentage: Average break percentage
                - avg_strength: Average strength score
                - type_distribution: Count by break type
        """
        try:
            base_query = self.session.query(self.model_class)
            
            if exchange is not None:
                base_query = base_query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                base_query = base_query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                base_query = base_query.filter(self.model_class.timeframe == timeframe)
            
            if break_type is not None:
                base_query = base_query.filter(self.model_class.break_type == break_type)
            
            if direction is not None:
                base_query = base_query.filter(self.model_class.direction == direction)
            
            if start_time is not None:
                base_query = base_query.filter(self.model_class.timestamp >= start_time)
            
            if end_time is not None:
                base_query = base_query.filter(self.model_class.timestamp <= end_time)
            
            # Count by confirmation status
            confirmed_count = base_query.filter(self.model_class.confirmed == True).count()
            unconfirmed_count = base_query.filter(self.model_class.confirmed == False).count()
            
            # Count by direction
            bullish_count = base_query.filter(self.model_class.direction == 'bullish').count()
            bearish_count = base_query.filter(self.model_class.direction == 'bearish').count()
            
            # Calculate averages
            avg_break_percentage = base_query.with_entities(
                func.avg(self.model_class.break_percentage)
            ).scalar() or 0.0
            
            avg_strength = base_query.with_entities(
                func.avg(self.model_class.strength)
            ).scalar() or 0.0
            
            # Count by break type
            higher_high_count = base_query.filter(self.model_class.break_type == 'higher_high').count()
            lower_low_count = base_query.filter(self.model_class.break_type == 'lower_low').count()
            higher_low_count = base_query.filter(self.model_class.break_type == 'higher_low').count()
            lower_high_count = base_query.filter(self.model_class.break_type == 'lower_high').count()
            
            type_distribution = {
                'higher_high': higher_high_count,
                'lower_low': lower_low_count,
                'higher_low': higher_low_count,
                'lower_high': lower_high_count
            }
            
            return {
                "confirmed_count": confirmed_count,
                "unconfirmed_count": unconfirmed_count,
                "bullish_count": bullish_count,
                "bearish_count": bearish_count,
                "avg_break_percentage": float(avg_break_percentage),
                "avg_strength": float(avg_strength),
                "type_distribution": type_distribution,
                "total_count": confirmed_count + unconfirmed_count
            }
        except SQLAlchemyError as e:
            self.logger.error(f"Error calculating BOS statistics: {str(e)}")
            return {
                "confirmed_count": 0,
                "unconfirmed_count": 0,
                "bullish_count": 0,
                "bearish_count": 0,
                "avg_break_percentage": 0.0,
                "avg_strength": 0.0,
                "type_distribution": {
                    'higher_high': 0,
                    'lower_low': 0,
                    'higher_low': 0,
                    'lower_high': 0
                },
                "total_count": 0
            }
    
    def _to_domain(self, db_obj: BosModel) -> Dict[str, Any]:
        """
        Convert a database model to a domain model.
        In this case, the to_dict method of the model is used directly.
        
        Args:
            db_obj: Database model instance
            
        Returns:
            Dictionary representation of the BOS event
        """
        try:
            return db_obj.to_dict()
        except Exception as e:
            self.logger.error(f"Error converting DB model to dictionary: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: Dict[str, Any]) -> BosModel:
        """
        Convert a domain model to a database model.
        In this case, the from_dict method of the model is used.
        
        Args:
            domain_obj: Dictionary data
            
        Returns:
            Database model instance
        """
        try:
            return BosModel.from_dict(domain_obj)
        except Exception as e:
            self.logger.error(f"Error converting dictionary to DB model: {str(e)}")
            raise