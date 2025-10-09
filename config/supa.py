from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def supabaseConnect():
    load_dotenv()

    ONLINE_USER = os.getenv("ONLINE_USER")
    ONLINE_PASSWORD = os.getenv("ONLINE_PASSWORD")
    ONLINE_HOST = os.getenv("ONLINE_HOST")
    ONLINE_PORT = os.getenv("ONLINE_PORT ")
    ONLINE_DBNAME = os.getenv("ONLINE_DBNAME")

    DATABASE_URL = f"postgresql+psycopg2://{ONLINE_USER}:{ONLINE_PASSWORD}@{ONLINE_HOST}:{ONLINE_PORT}/{ONLINE_DBNAME}?sslmode=require"

    supabase_engine = create_engine(DATABASE_URL)

    return supabase_engine