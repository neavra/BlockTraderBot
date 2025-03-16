from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os
from contextlib import asynccontextmanager, contextmanager
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.
from contextlib import contextmanager
from .base import Base

DATABASE_URL = os.environ["DATABASE_URL"] # TODO: need to change this url
engine = create_engine(DATABASE_URL, pool_size=50, max_overflow=0)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

ASYNC_DATABASE_URL = os.environ["DATABASE_URL"].replace("postgresql://", "postgresql+asyncpg://")
async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    pool_size=50,
    max_overflow=0,
    echo=False,
)
AsyncSessionLocal = sessionmaker(
    class_=AsyncSession,
    autocommit=False,
    autoflush=False,
    bind=async_engine,
    expire_on_commit=False,  # Prevent attributes from being expired after commit
)

# Create all tables
Base.metadata.create_all(bind=engine)

# Dependency function for getting the DB session

class DBAdapter: 
    @contextmanager
    def get_db(self):
        db = SessionLocal()
        try:
            yield db
        finally:
            if db:
                db.close()
    # Asynchronous session context manager
    @asynccontextmanager
    async def get_async_db(self):
        async with AsyncSessionLocal() as session:
            try:
                yield session
            finally:
                await session.close()
    
    # Standalone function to get a new async session
    # Use this when you need a fresh session for each operation
    async def create_async_session(self):
        return AsyncSessionLocal()