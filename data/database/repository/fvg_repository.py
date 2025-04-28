from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, or_
from sqlalchemy.exc import SQLAlchemyError

from data.database.models.fvg_model import FvgModel
from data.database.repository.base_repository import BaseRepository


class FvgRepository(BaseRepository[FvgModel]):
    """
    Repository for Fair Value Gap (FVG) data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the FVG repository.
        
        Args:
            session: The database session
        """
        super().__init__(FvgModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol_timeframe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        fvg_type: Optional[str] = None,
        filled: Optional[bool] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        min_size_percent: Optional[float] = None,
        min_strength: Optional[float] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Find FVGs for a specific exchange, symbol and timeframe.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timeframe: Candle timeframe
            fvg_type: Optional type filter ('bullish', 'bearish')
            filled: Optional filled status filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            min_size_percent: Optional minimum size percentage filter
            min_strength: Optional minimum strength filter
            limit: Optional limit on number of results
            
        Returns:
            List of FVG dictionaries
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.timeframe == timeframe
                )
            )
            
            if fvg_type is not None:
                query = query.filter(self.model_class.type == fvg_type)
            
            if filled is not None:
                query = query.filter(self.model_class.filled == filled)
            
            if start_time is not None:
                query = query.filter(self.model_class.timestamp >= start_time)
            
            if end_time is not None:
                query = query.filter(self.model_class.timestamp <= end_time)
            
            if min_size_percent is not None:
                query = query.filter(self.model_class.size_percent >= min_size_percent)
            
            if min_strength is not None:
                query = query.filter(self.model_class.strength >= min_strength)
            
            query = query.order_by(desc(self.model_class.timestamp))
            
            if limit is not None:
                query = query.limit(limit)
            
            db_fvgs = query.all()
            return [fvg.to_dict() for fvg in db_fvgs]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding FVGs for {exchange}/{symbol}/{timeframe}: {str(e)}"
            )
            return []
    
    def find_unfilled_fvgs(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        fvg_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Find all unfilled FVGs, optionally filtered by exchange, symbol, timeframe and type.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            fvg_type: Optional type filter ('bullish', 'bearish')
            
        Returns:
            List of unfilled FVG dictionaries
        """
        try:
            query = self.session.query(self.model_class).filter(
                self.model_class.filled == False
            )
            
            if exchange is not None:
                query = query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                query = query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                query = query.filter(self.model_class.timeframe == timeframe)
            
            if fvg_type is not None:
                query = query.filter(self.model_class.type == fvg_type)
            
            query = query.order_by(desc(self.model_class.timestamp))
            
            db_fvgs = query.all()
            return [fvg.to_dict() for fvg in db_fvgs]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding unfilled FVGs: {str(e)}")
            return []
    
    def find_fvgs_by_price_range(
        self,
        exchange: str,
        symbol: str,
        price_low: float,
        price_high: float,
        filled: Optional[bool] = False
    ) -> List[Dict[str, Any]]:
        """
        Find FVGs that overlap with a specified price range.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            price_low: Lower bound of the price range
            price_high: Upper bound of the price range
            filled: Optional filled status filter (default: False)
            
        Returns:
            List of FVG dictionaries that overlap with the price range
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    # Find FVGs where either:
                    # 1. FVG's bottom is within our range
                    # 2. FVG's top is within our range
                    # 3. FVG completely contains our range
                    or_(
                        and_(self.model_class.bottom >= price_low, self.model_class.bottom <= price_high),
                        and_(self.model_class.top >= price_low, self.model_class.top <= price_high),
                        and_(self.model_class.bottom <= price_low, self.model_class.top >= price_high)
                    )
                )
            )
            
            if filled is not None:
                query = query.filter(self.model_class.filled == filled)
            
            db_fvgs = query.all()
            return [fvg.to_dict() for fvg in db_fvgs]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding FVGs in price range for {exchange}/{symbol}: {str(e)}"
            )
            return []
    
    def update_filled_status(
        self,
        fvg_id: int,
        filled: bool
    ) -> bool:
        """
        Update the filled status of an FVG.
        
        Args:
            fvg_id: ID of the FVG
            filled: New filled status
            
        Returns:
            True if the update was successful, False otherwise
        """
        try:
            fvg = self.session.query(self.model_class).filter(
                self.model_class.id == fvg_id
            ).first()
            
            if not fvg:
                self.logger.warning(f"FVG with ID {fvg_id} not found")
                return False
            
            fvg.filled = filled
            fvg.updated_at = datetime.now(timezone.utc)
            
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error updating filled status for FVG ID {fvg_id}: {str(e)}")
            return False
    
    def calculate_fvg_stats(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        fvg_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Calculate statistics about FVGs.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            fvg_type: Optional type filter ('bullish', 'bearish')
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            Dictionary containing statistics:
                - filled_count: Number of filled FVGs
                - unfilled_count: Number of unfilled FVGs
                - bullish_count: Number of bullish FVGs
                - bearish_count: Number of bearish FVGs
                - avg_size_percent: Average size as percentage
                - avg_strength: Average strength score
                - largest_gap: Largest gap size
        """
        try:
            base_query = self.session.query(self.model_class)
            
            if exchange is not None:
                base_query = base_query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                base_query = base_query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                base_query = base_query.filter(self.model_class.timeframe == timeframe)
            
            if fvg_type is not None:
                base_query = base_query.filter(self.model_class.type == fvg_type)
            
            if start_time is not None:
                base_query = base_query.filter(self.model_class.timestamp >= start_time)
            
            if end_time is not None:
                base_query = base_query.filter(self.model_class.timestamp <= end_time)
            
            # Count by filled status
            filled_count = base_query.filter(self.model_class.filled == True).count()
            unfilled_count = base_query.filter(self.model_class.filled == False).count()
            
            # Count by type
            bullish_count = base_query.filter(self.model_class.type == 'bullish').count()
            bearish_count = base_query.filter(self.model_class.type == 'bearish').count()
            
            # Calculate averages
            avg_size_percent = base_query.with_entities(
                func.avg(self.model_class.size_percent)
            ).scalar() or 0.0
            
            avg_strength = base_query.with_entities(
                func.avg(self.model_class.strength)
            ).scalar() or 0.0
            
            # Get largest gap
            largest_gap = base_query.with_entities(
                func.max(self.model_class.size)
            ).scalar() or 0.0
            
            return {
                "filled_count": filled_count,
                "unfilled_count": unfilled_count,
                "bullish_count": bullish_count,
                "bearish_count": bearish_count,
                "avg_size_percent": float(avg_size_percent),
                "avg_strength": float(avg_strength),
                "largest_gap": float(largest_gap),
                "total_count": filled_count + unfilled_count
            }
        except SQLAlchemyError as e:
            self.logger.error(f"Error calculating FVG statistics: {str(e)}")
            return {
                "filled_count": 0,
                "unfilled_count": 0,
                "bullish_count": 0,
                "bearish_count": 0,
                "avg_size_percent": 0.0,
                "avg_strength": 0.0,
                "largest_gap": 0.0,
                "total_count": 0
            }
    
    def _to_domain(self, db_obj: FvgModel) -> Dict[str, Any]:
        """
        Convert a database model to a domain model.
        In this case, the to_dict method of the model is used directly.
        
        Args:
            db_obj: Database model instance
            
        Returns:
            Dictionary representation of the FVG
        """
        try:
            return db_obj.to_dict()
        except Exception as e:
            self.logger.error(f"Error converting DB model to dictionary: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: Dict[str, Any]) -> FvgModel:
        """
        Convert a domain model to a database model.
        In this case, the from_dict method of the model is used.
        
        Args:
            domain_obj: Dictionary data
            
        Returns:
            Database model instance
        """
        try:
            return FvgModel.from_dict(domain_obj)
        except Exception as e:
            self.logger.error(f"Error converting dictionary to DB model: {str(e)}")
            raise