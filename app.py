from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Float, ForeignKey
from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2

load_dotenv()

# Load environment variables from .env
load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

#creates the engine to connect to supabase
engine = create_engine(DATABASE_URL)

#if the tables don't exist in supabase yet
metadata_obj = MetaData()

fact_table = Table(
    "FactSales",
    metadata_obj,
    Column("quantity", Integer),
    Column("revenue", Float),
    Column("OrderNumber", String(255)),
    Column("UserId", ForeignKey("Users.id")),
    Column("ProductId", ForeignKey("Products.id")),
    Column("LocationId", ForeignKey("Location.id")),
    Column("DateId", ForeignKey("Date.id")),
)

user_table = Table(
    "Users",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("username", String(255)),
    Column("firstName", String(255)),
    Column("lastName", String(255)),
    Column("dateOfBirth", Date),
    Column("gender", String(255)),
)

loc_table = Table(
    "Location",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("address1", String(255)),
    Column("address2", String(255)),
    Column("city", String(255)),
    Column("country", String(255)),
    Column("zipCode", String(255)),
)

date_table = Table(
    "Date",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("createdAt", Date),
)

product_table = Table(
    "Products",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("category", String(255)),
    Column("description", String(255)),
    Column("name", String(255)),
    Column("price", Float),
)

metadata_obj.create_all(engine, checkfirst=True)


#not sure if the sql files will be inside the supabase or nakalagay locally sa github so i'll just do both

#supabase queries
products_df_supa = pd.read_sql("SELECT ", con=engine)
users_df_supa = pd.read_sql("", con=engine)
date_df_supa = pd.read_sql("", con=engine)
location_df_supa = pd.read_sql("", con=engine)
