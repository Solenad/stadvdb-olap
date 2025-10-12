from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

ONLINE_USER = os.getenv("ONLINE_USER")
ONLINE_PASSWORD = os.getenv("ONLINE_PASSWORD")
ONLINE_HOST = os.getenv("ONLINE_HOST")
ONLINE_PORT = os.getenv("ONLINE_PORT")
ONLINE_DBNAME = os.getenv("ONLINE_DBNAME")

DATABASE_CONN_STRING = (
    f"postgresql+psycopg2://{ONLINE_USER}:{ONLINE_PASSWORD}@"
    f"{ONLINE_HOST}:{ONLINE_PORT}/{ONLINE_DBNAME}?sslmode=require"
)

engine = create_engine(
    DATABASE_CONN_STRING,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

Session = sessionmaker(bind=engine)
