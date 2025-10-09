from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def localConnect():
    load_dotenv()

    LOCAL_USER = os.getenv("LOCAL_USER")
    LOCAL_PASSWORD = os.getenv("LOCAL_PASSWORD")
    LOCAL_HOST = os.getenv("LOCAL_HOST")
    LOCAL_DB = os.getenv("LOCAL_DB")

    local_connection_string = f"mysql+pymysql://{LOCAL_USER}:{LOCAL_PASSWORD}@{LOCAL_HOST}/{LOCAL_DB}"
    local_engine = create_engine(local_connection_string)
    
    return local_engine