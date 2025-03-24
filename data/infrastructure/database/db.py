from typing import Generator, Any
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from .base import Base


class Database:
    """Handles database connection and session management."""
    
    def __init__(self, db_url: str, echo: bool = False):
        """
        Initialize the database connection.
        
        Args:
            db_url: The database connection URL
            echo: Whether to echo SQL statements (for debugging)
        """
        self.db_url = db_url
        self.engine = create_engine(
            db_url,
            echo=echo,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600
        )
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
    
    def create_tables(self) -> None:
        """Create all tables defined in the models."""
        Base.metadata.create_all(bind=self.engine)
    
    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()
    
    @contextmanager
    def session_scope(self) -> Generator[Session, Any, None]:
        """
        Provide a transactional scope around a series of operations.
        
        Usage:
            with db.session_scope() as session:
                session.add(some_object)
                session.add(some_other_object)
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    