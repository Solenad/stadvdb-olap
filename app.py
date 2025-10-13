from config import local
from ETL import user_ETL, date_ETL, loc_ETL, prod_ETL, fact_ETL
from sqlalchemy import text

try:
    with local.engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("Connected to local DB.")

    user_df = user_ETL.extractUser()

    loc_df = loc_ETL.extractLocation()
    
    date_df = date_ETL.extractDate()

    prod_df = prod_ETL.extractProduct()

    fact_df = fact_ETL.extractFact(user_df, loc_df, date_df, prod_df)

    print(fact_df)

except Exception as e:
    print(f"Connection failed: {e}")
