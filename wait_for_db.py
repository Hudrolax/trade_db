from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
import os
import logging
import time

logger = logging.getLogger('wait_for_db')

DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

TESTING = bool(os.environ.get('TESTING', False))

if TESTING:
    DATABASE_URL = f"postgresql://test:test@test_db/test"
else:
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def wait_for_db():
    """Wait connection for DB"""
    while True:
        try:
            logger.info(f'Try connectiong to DB {DATABASE_URL}')
            engine.connect()
            break
        except OperationalError:
            logger.info("Waiting for database connection...")
            time.sleep(1)

if __name__ == '__main__':
    wait_for_db()