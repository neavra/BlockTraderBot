from typing import TypeVar, Generic, Type, List, Optional, Any, Dict, Union
from sqlalchemy.orm import Session
from sqlalchemy import select, update, delete

from ..base import BaseModel

T = TypeVar('T', bound=BaseModel)


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
    
    def get_by_id(self, id: int) -> Optional[T]:
        """
        Retrieve a record by its ID.
        
        Args:
            id: The record ID
            
        Returns:
            The model instance if found, None otherwise
        """
        return self.session.query(self.model_class).filter(self.model_class.id == id).first()
    
    def get_all(self) -> List[T]:
        """
        Retrieve all records.
        
        Returns:
            List of all model instances
        """
        return self.session.query(self.model_class).all()
    
    def find(self, **kwargs) -> List[T]:
        """
        Find records matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to filter by
            
        Returns:
            List of matching model instances
        """
        query = self.session.query(self.model_class)
        for key, value in kwargs.items():
            if hasattr(self.model_class, key):
                query = query.filter(getattr(self.model_class, key) == value)
        return query.all()
    
    def find_one(self, **kwargs) -> Optional[T]:
        """
        Find a single record matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to filter by
            
        Returns:
            Matching model instance if found, None otherwise
        """
        query = self.session.query(self.model_class)
        for key, value in kwargs.items():
            if hasattr(self.model_class, key):
                query = query.filter(getattr(self.model_class, key) == value)
        return query.first()
    
    def create(self, **kwargs) -> T:
        """
        Create a new record.
        
        Args:
            **kwargs: Field-value pairs for the new record
            
        Returns:
            The created model instance
        """
        instance = self.model_class(**kwargs)
        self.session.add(instance)
        self.session.commit()
        self.session.refresh(instance)
        return instance
    
    def update(self, id: int, **kwargs) -> Optional[T]:
        """
        Update a record.
        
        Args:
            id: The record ID
            **kwargs: Field-value pairs to update
            
        Returns:
            The updated model instance if found, None otherwise
        """
        instance = self.get_by_id(id)
        if instance is None:
            return None
        
        for key, value in kwargs.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
        
        self.session.commit()
        self.session.refresh(instance)
        return instance
    
    def delete(self, id: int) -> bool:
        """
        Delete a record.
        
        Args:
            id: The record ID
            
        Returns:
            True if deleted, False if not found
        """
        instance = self.get_by_id(id)
        if instance is None:
            return False
        
        self.session.delete(instance)
        self.session.commit()
        return True
    
    def bulk_create(self, items: List[Dict[str, Any]]) -> List[T]:
        """
        Create multiple records.
        
        Args:
            items: List of field-value dictionaries
            
        Returns:
            List of created model instances
        """
        instances = [self.model_class(**item) for item in items]
        self.session.add_all(instances)
        self.session.commit()
        
        # Refresh all instances
        for instance in instances:
            self.session.refresh(instance)
        
        return instances
    
    def count(self, **kwargs) -> int:
        """
        Count records matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to filter by
            
        Returns:
            Count of matching records
        """
        query = self.session.query(self.model_class)
        for key, value in kwargs.items():
            if hasattr(self.model_class, key):
                query = query.filter(getattr(self.model_class, key) == value)
        return query.count()