from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from sqlalchemy.exc import SQLAlchemyError

from data.database.models.indicator_model import IndicatorModel
from data.database.repository.base_repository import BaseRepository


class IndicatorRepository(BaseRepository[IndicatorModel]):
    """
    Repository for indicator registry operations.
    Handles operations for the indicators registry table.
    """
    
    def __init__(self, session: Session):
        """
        Initialize the indicator repository.
        
        Args:
            session: The database session
        """
        super().__init__(IndicatorModel, session)
        self.logger = logging.getLogger(__name__)
    
    def find_by_name(self, indicator_name: str) -> Optional[Dict[str, Any]]:
        """
        Find an indicator by its name.
        
        Args:
            indicator_name: Name of the indicator
            
        Returns:
            Indicator data as dictionary if found, None otherwise
        """
        try:
            db_indicator = self.session.query(self.model_class).filter(
                self.model_class.indicator_name == indicator_name
            ).first()
            
            if db_indicator:
                return db_indicator.to_dict()
            return None
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding indicator by name '{indicator_name}': {str(e)}")
            return None
    
    def get_active_indicators(self) -> List[Dict[str, Any]]:
        """
        Get all active indicators.
        
        Returns:
            List of active indicator data dictionaries
        """
        try:
            db_indicators = self.session.query(self.model_class).filter(
                self.model_class.is_active == True
            ).all()
            
            return [indicator.to_dict() for indicator in db_indicators]
        except SQLAlchemyError as e:
            self.logger.error(f"Error retrieving active indicators: {str(e)}")
            return []
    
    def find_by_category(self, category: str) -> List[Dict[str, Any]]:
        """
        Find indicators by category.
        
        Args:
            category: Indicator category
            
        Returns:
            List of indicator data dictionaries in the specified category
        """
        try:
            db_indicators = self.session.query(self.model_class).filter(
                self.model_class.indicator_category == category
            ).all()
            
            return [indicator.to_dict() for indicator in db_indicators]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding indicators in category '{category}': {str(e)}")
            return []
    
    def find_composite_indicators(self) -> List[Dict[str, Any]]:
        """
        Find all composite indicators.
        
        Returns:
            List of composite indicator data dictionaries
        """
        try:
            db_indicators = self.session.query(self.model_class).filter(
                self.model_class.is_composite == True
            ).all()
            
            return [indicator.to_dict() for indicator in db_indicators]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding composite indicators: {str(e)}")
            return []
    
    def get_dependencies(self, indicator_id: int) -> List[Dict[str, Any]]:
        """
        Get all indicators that the specified indicator depends on.
        
        Args:
            indicator_id: ID of the indicator
            
        Returns:
            List of dependency indicator data dictionaries
        """
        try:
            # First, get the indicator to find its dependencies
            indicator = self.session.query(self.model_class).filter(
                self.model_class.id == indicator_id
            ).first()
            
            if not indicator or not indicator.required_dependencies:
                return []
            
            # Get all the dependencies
            dependencies = self.session.query(self.model_class).filter(
                self.model_class.id.in_(indicator.required_dependencies)
            ).all()
            
            return [dep.to_dict() for dep in dependencies]
        except SQLAlchemyError as e:
            self.logger.error(f"Error getting dependencies for indicator ID {indicator_id}: {str(e)}")
            return []
    
    def update_indicator_status(self, indicator_id: int, is_active: bool) -> bool:
        """
        Update the active status of an indicator.
        
        Args:
            indicator_id: ID of the indicator
            is_active: New active status
            
        Returns:
            True if the update was successful, False otherwise
        """
        try:
            indicator = self.session.query(self.model_class).filter(
                self.model_class.id == indicator_id
            ).first()
            
            if not indicator:
                self.logger.warning(f"Indicator with ID {indicator_id} not found")
                return False
            
            indicator.is_active = is_active
            indicator.updated_at = datetime.now(timezone.utc)
            
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error updating status for indicator ID {indicator_id}: {str(e)}")
            return False
    
    def _to_domain(self, db_obj: IndicatorModel) -> Dict[str, Any]:
        """
        Convert a database model to a domain model.
        In this case, the to_dict method of the model is used directly.
        
        Args:
            db_obj: Database model instance
            
        Returns:
            Dictionary representation of the indicator
        """
        try:
            return db_obj.to_dict()
        except Exception as e:
            self.logger.error(f"Error converting DB model to dictionary: {str(e)}")
            raise
    
    def _to_db(self, domain_obj: Dict[str, Any]) -> IndicatorModel:
        """
        Convert a domain model to a database model.
        In this case, the from_dict method of the model is used.
        
        Args:
            domain_obj: Dictionary data
            
        Returns:
            Database model instance
        """
        try:
            return IndicatorModel.from_dict(domain_obj)
        except Exception as e:
            self.logger.error(f"Error converting dictionary to DB model: {str(e)}")
            raise