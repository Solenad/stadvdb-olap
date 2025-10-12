from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

LOCAL_USER = os.getenv("LOCAL_USER")
LOCAL_PASSWORD = os.getenv("LOCAL_PASSWORD")
LOCAL_HOST = os.getenv("LOCAL_HOST")
LOCAL_DB = os.getenv("LOCAL_DB")

DATABASE_CONN_STRING = f"mysql+pymysql://{
    LOCAL_USER}:{LOCAL_PASSWORD}@{LOCAL_HOST}/{LOCAL_DB}"
engine = create_engine(
    DATABASE_CONN_STRING, pool_pre_ping=True, pool_size=10, max_overflow=20
)

Session = sessionmaker(bind=engine)
