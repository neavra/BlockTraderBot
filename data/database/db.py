from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.
from contextlib import contextmanager
from .base import Base

DATABASE_URL = os.environ["DATABASE_URL"] # TODO: need to change this url

engine = create_engine(DATABASE_URL, pool_size=20, max_overflow=0)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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
