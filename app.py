from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Float, ForeignKey
from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
import pymysql

# Load environment variables from .env
load_dotenv()

# Fetch variables
ONLINE_USER = os.getenv("user")
ONLINE_PASSWORD = os.getenv("password")
ONLINE_HOST = os.getenv("host")
ONLINE_PORT = os.getenv("port")
ONLINE_DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{ONLINE_USER}:{ONLINE_PASSWORD}@{ONLINE_HOST}:{ONLINE_PORT}/{ONLINE_DBNAME}?sslmode=require"

#creates the engine to connect to supabase
supabase_engine = create_engine(DATABASE_URL)

connect = supabase_engine.connect()

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

#adds the tables if not yet created
metadata_obj.create_all(connect, checkfirst=True)

connect.close()

#for local mysql datasets
LOCAL_USER = 'root'
LOCAL_PASSWORD = 'place_your_local_password'
LOCAL_HOST = 'localhost'
LOCAL_DB = 'faker'

local_connection_string = f"mysql+pymysql://{LOCAL_USER}:{LOCAL_PASSWORD}@{LOCAL_HOST}/{LOCAL_DB}"
local_engine = create_engine(local_connection_string)

#queries to get from dataset
products_df = pd.read_sql("SELECT id, category, description, name, price FROM Products", con=local_engine)
users_df = pd.read_sql("SELECT id, username, firstName, lastName, dateOfBirth, gender FROM Users", con=local_engine)
date_df = pd.read_sql("SELECT id, deliveryDate FROM Orders", con=local_engine)
location_df = pd.read_sql("SELECT id, address1, address2, city, country, zipCode FROM Users", con=local_engine)
fact_df = pd.read_sql("", con=local_engine)
