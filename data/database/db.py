from typing import Generator, Any
from contextlib import contextmanager
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from .models.base import Base


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
        self.logger = logging.getLogger("Database")
    
    async def disconnect(self) -> None:
        """
        Disconnect from the database and clean up all connections.
        This should be called when shutting down the service to ensure
        all database resources are properly released.
        """
        try:
            if self.engine:
                # Dispose of the engine to close all connections in the pool
                self.engine.dispose()
                self.logger.info("Database engine disposed, all connections closed")
            else:
                self.logger.warning("No database engine to dispose")
        except Exception as e:
            self.logger.error(f"Error while disconnecting from database: {e}", exc_info=True)
            # We don't re-raise the exception here to ensure shutdown continues
            # even if there's an issue with database cleanup
        finally:
            # Reset attributes to ensure we don't use them after disconnect
            self.engine = None
            self.SessionLocal = None
            self.logger.info("Database disconnect completed")
            
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
    
    