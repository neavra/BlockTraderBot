from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from sqlalchemy.exc import SQLAlchemyError

from ..models.signal_model import SignalModel
from .base_repository import BaseRepository
from shared.domain.dto.signal_dto import SignalDto


class SignalRepository(BaseRepository[SignalModel]):
    """
    Repository for signal data operations.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the signal repository.
        
        Args:
            session: The database session
        """
        super().__init__(SignalModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_exchange_symbol(
        self,
        exchange: str,
        symbol: str,
        execution_status: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[SignalDto]:
        """
        Find signals for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            execution_status: Optional execution status filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            limit: Optional limit on number of results
            
        Returns:
            List of Signal domain models
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol
                )
            )
            
            if execution_status is not None:
                query = query.filter(self.model_class.execution_status == execution_status)
            
            if start_time is not None:
                query = query.filter(self.model_class.created_at >= start_time)
            
            if end_time is not None:
                query = query.filter(self.model_class.created_at <= end_time)
            
            query = query.order_by(desc(self.model_class.created_at))
            
            if limit is not None:
                query = query.limit(limit)
            
            db_signals = query.all()
            return [self._to_domain(s) for s in db_signals]
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error finding signals for {exchange}/{symbol}: {str(e)}"
            )
            return []
    
    def find_by_strategy(
        self,
        strategy_name: str,
        execution_status: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[SignalDto]:
        """
        Find signals for a specific strategy.
        
        Args:
            strategy_name: Name of the strategy
            execution_status: Optional execution status filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            limit: Optional limit on number of results
            
        Returns:
            List of Signal domain models
        """
        try:
            query = self.session.query(self.model_class).filter(
                self.model_class.strategy_name == strategy_name
            )
            
            if execution_status is not None:
                query = query.filter(self.model_class.execution_status == execution_status)
            
            if start_time is not None:
                query = query.filter(self.model_class.created_at >= start_time)
            
            if end_time is not None:
                query = query.filter(self.model_class.created_at <= end_time)
            
            query = query.order_by(desc(self.model_class.created_at))
            
            if limit is not None:
                query = query.limit(limit)
            
            db_signals = query.all()
            return [self._to_domain(s) for s in db_signals]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding signals for strategy {strategy_name}: {str(e)}")
            return []
    
    def get_latest_signal(
        self,
        exchange: str,
        symbol: str,
        execution_status: Optional[str] = None
    ) -> Optional[SignalDto]:
        """
        Get the latest signal for a specific exchange and symbol.
        
        Args:
            exchange: Exchange name
            symbol: Trading symbol
            execution_status: Optional execution status filter
            
        Returns:
            Latest Signal domain model if found, None otherwise
        """
        try:
            query = self.session.query(self.model_class).filter(
                and_(
                    self.model_class.exchange == exchange,
                    self.model_class.symbol == symbol
                )
            )
            
            if execution_status is not None:
                query = query.filter(self.model_class.execution_status == execution_status)
            
            db_signal = query.order_by(desc(self.model_class.created_at)).first()
            
            if db_signal:
                return self._to_domain(db_signal)
            return None
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error getting latest signal for {exchange}/{symbol}: {str(e)}"
            )
            return None
    
    def bulk_upsert_signals(self, signals: List[SignalDto]) -> int:
        """
        Insert or update multiple signals in a single transaction.
        
        Args:
            signals: List of signal domain objects to upsert
            
        Returns:
            Number of successfully upserted signals
        """
        try:
            upserted_count = 0
            for signal in signals:
                # Check if signal exists
                existing = self.session.query(self.model_class).filter(
                    self.model_class.id == signal.id
                ).first()
                
                if existing:
                    # Update existing
                    for key, value in signal.dict().items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                else:
                    # Create new
                    db_obj = self._to_db(signal)
                    self.session.add(db_obj)
                
                upserted_count += 1
            
            self.session.commit()
            return upserted_count
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error bulk upserting signals: {str(e)}")
            return 0
        
    async def create_signal(self, signal_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a new signal record in the database.
        
        Args:
            signal_data: Dictionary containing signal data
            
        Returns:
            Dictionary representation of the created signal, or None if creation failed
        """
        try:
            # Convert dictionary to model
            signal_model = SignalModel.from_dict(signal_data)
            
            # Add to session
            self.session.add(signal_model)
            self.session.commit()
            
            # Refresh to get updated values (like auto-generated ID)
            self.session.refresh(signal_model)
            
            # Convert back to dictionary and return
            return signal_model.to_dict()
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error creating signal: {str(e)}")
            return None

    async def bulk_create_signals(self, signals_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Create multiple signals in a single transaction.
        
        Args:
            signals_data: List of dictionaries containing signal data
            
        Returns:
            List of created signals as dictionaries
        """
        created_signals = []
        try:
            # Convert dictionaries to models
            signal_models = [SignalModel.from_dict(data) for data in signals_data]
            
            # Add all to session
            self.session.add_all(signal_models)
            self.session.commit()
            
            # Refresh to get updated values
            for model in signal_models:
                self.session.refresh(model)
                created_signals.append(model.to_dict())
                
            return created_signals
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error bulk creating signals: {str(e)}")
            return []
    
    def _to_domain(self, db_obj: SignalModel) -> SignalDto:
        """
        Convert a database model to a domain model.
        
        Args:
            db_model: Database model instance
            
        Returns:
            Corresponding domain model
        """
        try:
            return SignalDto(
                id=db_obj.id,
                strategy_name=db_obj.strategy_name,
                exchange=db_obj.exchange,
                symbol=db_obj.symbol,
                timeframe=db_obj.timeframe,
                direction=db_obj.direction,
                signal_type=db_obj.signal_type,
                price_target=db_obj.price_target,
                stop_loss=db_obj.stop_loss,
                take_profit=db_obj.take_profit,
                risk_reward_ratio=db_obj.risk_reward_ratio,
                confidence_score=db_obj.confidence_score,
                execution_status=db_obj.execution_status,
                created_at=db_obj.created_at,
                updated_at=db_obj.updated_at,
                metadata=db_obj.metadata,
                indicator_id=db_obj.indicator_id
            )
        except Exception as e:
            self.logger.error(f"Error converting DB model to domain model: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: SignalDto) -> SignalModel:
        """
        Convert a domain model to a database model.
        
        Args:
            domain_obj: Domain model instance
            
        Returns:
            Corresponding Database model
        """
        try:
            return SignalModel(
                id=domain_obj.id,
                strategy_name=domain_obj.strategy_name,
                exchange=domain_obj.exchange,
                symbol=domain_obj.symbol,
                timeframe=domain_obj.timeframe,
                direction=domain_obj.direction,
                signal_type=domain_obj.signal_type,
                price_target=domain_obj.price_target,
                stop_loss=domain_obj.stop_loss,
                take_profit=domain_obj.take_profit,
                risk_reward_ratio=domain_obj.risk_reward_ratio,
                confidence_score=domain_obj.confidence_score,
                execution_status=domain_obj.execution_status,
                created_at=domain_obj.created_at,
                updated_at=domain_obj.updated_at,
                metadata=domain_obj.metadata,
                indicator_id=domain_obj.indicator_id
            )
        except Exception as e:
            self.logger.error(f"Error converting domain model to DB model: {str(e)}")
            raise 