from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, or_
from sqlalchemy.exc import SQLAlchemyError

from data.database.models.doji_model import DojiModel
from data.database.repository.base_repository import BaseRepository


class DojiRepository(BaseRepository[DojiModel]):
    """
    Repository for Doji candlestick pattern data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the Doji repository.
        
        Args:
            session: The database session
        """
        super().__init__(DojiModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol_timeframe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        min_strength: Optional[float] = None,
        max_body_to_range_ratio: Optional[float] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Find Doji patterns for a specific exchange, symbol and timeframe.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            timeframe: Candle timeframe
            min_strength: Optional minimum strength filter
            max_body_to_range_ratio: Optional maximum body-to-range ratio filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            limit: Optional limit on number of results
            
        Returns:
            List of Doji dictionaries
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol,
                    self.model_class.timeframe == timeframe
                )
            )
            
            if min_strength is not None:
                query = query.filter(self.model_class.strength >= min_strength)
            
            if max_body_to_range_ratio is not None:
                query = query.filter(self.model_class.body_to_range_ratio <= max_body_to_range_ratio)
            
            if start_time is not None:
                query = query.filter(self.model_class.timestamp >= start_time)
            
            if end_time is not None:
                query = query.filter(self.model_class.timestamp <= end_time)
            
            query = query.order_by(desc(self.model_class.timestamp))
            
            if limit is not None:
                query = query.limit(limit)
            
            db_dojis = query.all()
            return [doji.to_dict() for doji in db_dojis]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding Doji patterns for {exchange}/{symbol}/{timeframe}: {str(e)}"
            )
            return []
    
    def find_strongest_dojis(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        min_strength: float = 0.7,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find the strongest Doji patterns, optionally filtered by exchange, symbol, and timeframe.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            min_strength: Minimum strength threshold (default: 0.7)
            limit: Maximum number of results (default: 10)
            
        Returns:
            List of strongest Doji dictionaries
        """
        try:
            query = self.session.query(self.model_class).filter(
                self.model_class.strength >= min_strength
            )
            
            if exchange is not None:
                query = query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                query = query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                query = query.filter(self.model_class.timeframe == timeframe)
            
            query = query.order_by(desc(self.model_class.strength), desc(self.model_class.timestamp)).limit(limit)
            
            db_dojis = query.all()
            return [doji.to_dict() for doji in db_dojis]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding strongest Doji patterns: {str(e)}")
            return []
    
    def find_recent_dojis(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        hours_ago: int = 24,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find recent Doji patterns within a specified time window.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            hours_ago: Look back period in hours (default: 24)
            limit: Maximum number of results (default: 10)
            
        Returns:
            List of recent Doji dictionaries
        """
        try:
            # Calculate time window
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timezone.timedelta(hours=hours_ago)
            
            query = self.session.query(self.model_class).filter(
                self.model_class.timestamp >= start_time
            )
            
            if exchange is not None:
                query = query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                query = query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                query = query.filter(self.model_class.timeframe == timeframe)
            
            query = query.order_by(desc(self.model_class.timestamp)).limit(limit)
            
            db_dojis = query.all()
            return [doji.to_dict() for doji in db_dojis]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding recent Doji patterns: {str(e)}")
            return []
    
    def calculate_doji_stats(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Calculate statistics about Doji patterns.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            timeframe: Optional timeframe filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            Dictionary containing statistics:
                - total_count: Total number of Doji patterns
                - avg_strength: Average strength score
                - avg_body_to_range_ratio: Average body-to-range ratio
                - avg_wick_size: Average wick size
                - strength_distribution: Count by strength range
        """
        try:
            base_query = self.session.query(self.model_class)
            
            if exchange is not None:
                base_query = base_query.filter(self.model_class.exchange == exchange)
            
            if symbol is not None:
                base_query = base_query.filter(self.model_class.symbol == symbol)
            
            if timeframe is not None:
                base_query = base_query.filter(self.model_class.timeframe == timeframe)
            
            if start_time is not None:
                base_query = base_query.filter(self.model_class.timestamp >= start_time)
            
            if end_time is not None:
                base_query = base_query.filter(self.model_class.timestamp <= end_time)
            
            # Count total
            total_count = base_query.count()
            
            # Calculate averages
            avg_strength = base_query.with_entities(
                func.avg(self.model_class.strength)
            ).scalar() or 0.0
            
            avg_body_to_range_ratio = base_query.with_entities(
                func.avg(self.model_class.body_to_range_ratio)
            ).scalar() or 0.0
            
            avg_wick_size = base_query.with_entities(
                func.avg(self.model_class.total_wick_size)
            ).scalar() or 0.0
            
            # Count by strength range
            very_strong_count = base_query.filter(self.model_class.strength >= 0.8).count()
            strong_count = base_query.filter(and_(
                self.model_class.strength >= 0.6,
                self.model_class.strength < 0.8
            )).count()
            medium_count = base_query.filter(and_(
                self.model_class.strength >= 0.4,
                self.model_class.strength < 0.6
            )).count()
            weak_count = base_query.filter(self.model_class.strength < 0.4).count()
            
            strength_distribution = {
                'very_strong': very_strong_count,
                'strong': strong_count,
                'medium': medium_count,
                'weak': weak_count
            }
            
            return {
                "total_count": total_count,
                "avg_strength": float(avg_strength),
                "avg_body_to_range_ratio": float(avg_body_to_range_ratio),
                "avg_wick_size": float(avg_wick_size),
                "strength_distribution": strength_distribution
            }
        except SQLAlchemyError as e:
            self.logger.error(f"Error calculating Doji statistics: {str(e)}")
            return {
                "total_count": 0,
                "avg_strength": 0.0,
                "avg_body_to_range_ratio": 0.0,
                "avg_wick_size": 0.0,
                "strength_distribution": {
                    'very_strong': 0,
                    'strong': 0,
                    'medium': 0,
                    'weak': 0
                }
            }
        
    async def create_doji(self, doji_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a new Doji candle pattern record in the database.
        
        Args:
            doji_data: Dictionary containing Doji data
            
        Returns:
            Dictionary representation of the created Doji, or None if creation failed
        """
        try:
            # Convert dictionary to model
            doji_model = DojiModel.from_dict(doji_data)
            
            # Add to session
            self.session.add(doji_model)
            self.session.commit()
            
            # Refresh to get updated values (like auto-generated ID)
            self.session.refresh(doji_model)
            
            # Convert back to dictionary and return
            return doji_model.to_dict()
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error creating Doji: {str(e)}")
            return None

        async def bulk_create_dojis(self, doji_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            """
            Create multiple Doji records in a single transaction.
            
            Args:
                doji_list: List of dictionaries containing Doji data
                
            Returns:
                List of created Dojis as dictionaries
            """
            created_items = []
            try:
                # Convert dictionaries to models
                doji_models = [DojiModel.from_dict(data) for data in doji_list]
                
                # Add all to session
                self.session.add_all(doji_models)
                self.session.commit()
                
                # Refresh to get updated values
                for model in doji_models:
                    self.session.refresh(model)
                    created_items.append(model.to_dict())
                    
                return created_items
            except SQLAlchemyError as e:
                self.session.rollback()
                self.logger.error(f"Error bulk creating Doji records: {str(e)}")
                return []
    
    def _to_domain(self, db_obj: DojiModel) -> Dict[str, Any]:
        """
        Convert a database model to a domain model.
        In this case, the to_dict method of the model is used directly.
        
        Args:
            db_obj: Database model instance
            
        Returns:
            Dictionary representation of the Doji pattern
        """
        try:
            return db_obj.to_dict()
        except Exception as e:
            self.logger.error(f"Error converting DB model to dictionary: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: Dict[str, Any]) -> DojiModel:
        """
        Convert a domain model to a database model.
        In this case, the from_dict method of the model is used.
        
        Args:
            domain_obj: Dictionary data
            
        Returns:
            Database model instance
        """
        try:
            return DojiModel.from_dict(domain_obj)
        except Exception as e:
            self.logger.error(f"Error converting dictionary to DB model: {str(e)}")
            raise