from typing import TypeVar, Generic, Type, List, Optional, Any, Dict, Union
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ..models.base import BaseModel

T = TypeVar('T', bound=BaseModel)
D = TypeVar('D')  # Domain model type


class BaseRepository(Generic[T]):
    """
    Base repository with common CRUD operations.
    
    This generic class provides standard database operations for any model
    that extends BaseModel.
    """
    
    def __init__(self, model_class: Type[T], session: Session):
        """
        Initialize the repository.
        
        Args:
            model_class: The SQLAlchemy model class
            session: The database session
        """
        self.model_class = model_class
        self.session = session
        self.logger = logging.getLogger(__name__)
    
    def _to_domain(self, db_obj: T) -> D:
        """Convert DB model to domain model."""
        raise NotImplementedError("Must implement in subclass")

    def _to_db(self, domain_obj: D) -> T:
        """Convert domain model to DB model."""
        raise NotImplementedError("Must implement in subclass")
    
    def get_by_id(self, id: int) -> Optional[D]:
        """
        Retrieve a record by its ID.
        
        Args:
            id: The record ID
            
        Returns:
            The domain model instance if found, None otherwise
        """
        try:
            db_obj = self.session.query(self.model_class).filter(self.model_class.id == id).first()
            return self._to_domain(db_obj) if db_obj else None
        except SQLAlchemyError as e:
            self.logger.error(f"Error retrieving {self.model_class.__name__} with id {id}: {str(e)}")
            return None
      
    def get_all(self) -> List[D]:
        """
        Retrieve all records.
        
        Returns:
            List of all model instances
        """
        try:
            return [self._to_domain(obj) for obj in self.session.query(self.model_class).all()]
        except SQLAlchemyError as e:
            self.logger.error(f"Error retrieving all {self.model_class.__name__} records: {str(e)}")
            return []
    
    def find(self, **kwargs) -> List[D]:
        """
        Find records matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to filter by
            
        Returns:
            List of matching model instances
        """
        try:
            query = self.session.query(self.model_class)
            for key, value in kwargs.items():
                if hasattr(self.model_class, key):
                    query = query.filter(getattr(self.model_class, key) == value)
            db_objects = query.all()
            return [self._to_domain(obj) for obj in db_objects]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding {self.model_class.__name__} records: {str(e)}")
            return []
    
    def find_one(self, **kwargs) -> Optional[D]:
        """
        Find a single record matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to filter by
            
        Returns:
            Matching model instance if found, None otherwise
        """
        try:
            query = self.session.query(self.model_class)
            for key, value in kwargs.items():
                if hasattr(self.model_class, key):
                    query = query.filter(getattr(self.model_class, key) == value)
            db_object = query.first()
            return self._to_domain(db_object) if db_object else None
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding {self.model_class.__name__} record: {str(e)}")
            return None
    
    def create(self, domain_obj: D) -> Optional[D]:
        """
        Create a new record.
        
        Args:
            domain_obj: Field-value pairs for the new record
            
        Returns:
            The created domain model instance or None if error
        """
        try:
            db_obj = self._to_db(domain_obj)
            self.session.add(db_obj)
            self.session.commit()
            self.session.refresh(db_obj)
            return self._to_domain(db_obj)
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error creating {self.model_class.__name__} record: {str(e)}")
            return None
    
    def update(self, id: int, domain_obj: D) -> Optional[D]:
        """
        Update a record.
        
        Args:
            id: The record ID
            domain_obj: Obj to update
            
        Returns:
            The updated model instance if found, None otherwise
        """
        try:
            db_obj = self.session.query(self.model_class).filter(self.model_class.id == id).first()
            if not db_obj:
                return None
                
            for key, value in vars(domain_obj).items():
                if hasattr(db_obj, key):
                    setattr(db_obj, key, value)
                    
            self.session.commit()
            self.session.refresh(db_obj)
            return self._to_domain(db_obj)
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error updating {self.model_class.__name__} with id {id}: {str(e)}")
            return None
    
    def delete(self, id: int) -> bool:
        """
        Delete a record.
        
        Args:
            id: The record ID
            
        Returns:
            True if deleted, False if not found or error
        """
        try:
            db_obj = self.session.query(self.model_class).filter(self.model_class.id == id).first()
            if not db_obj:
                return False
                
            self.session.delete(db_obj)
            self.session.commit()
            return True
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error deleting {self.model_class.__name__} with id {id}: {str(e)}")
            return False
    
    def bulk_create(self, items: List[D]) -> List[D]:
        """
        Create multiple records.
        
        Args:
            items: List of domain objects
            
        Returns:
            List of created model instances
        """
        try:
            db_instances = [self._to_db(item) for item in items]
            self.session.add_all(db_instances)
            self.session.commit()

            # Refresh instances and return as domain models
            for instance in db_instances:
                self.session.refresh(instance)
            
            return [self._to_domain(instance) for instance in db_instances]
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Error bulk creating {self.model_class.__name__} records: {str(e)}")
            return []
    
    def count(self, **kwargs) -> int:
        """
        Count records matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to filter by
            
        Returns:
            Count of matching records
        """
        try:
            query = self.session.query(self.model_class)
            for key, value in kwargs.items():
                if hasattr(self.model_class, key):
                    query = query.filter(getattr(self.model_class, key) == value)
            return query.count()
        except SQLAlchemyError as e:
            self.logger.error(f"Error counting {self.model_class.__name__} records: {str(e)}")
            return 0