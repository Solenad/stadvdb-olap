from config import local, supa
from contextlib import contextmanager
from sqlalchemy import select, func, MetaData, text
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

@contextmanager
def extract():
    session = local.Session()
    try:
        logging.info("DB connection established.")
        yield session
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise
    finally:
        session.close()
        logging.info("DB connection closed.")

def cleanDateData(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning date data...")

    df["deliveryDate"] = pd.to_datetime(df["deliveryDate"], errors="coerce")
    df = df.dropna(subset=["deliveryDate"])
    df =  df.copy()
    df["deliveryDate"] = df["deliveryDate"].dt.date

    df = df.drop_duplicates(subset=["deliveryDate"]).reset_index(drop=True)
    df["id"] = df.index + 1  # matches Supabase's Users.id structure

    logging.info("Date data cleaning completed successfully.")
    return df[["id","deliveryDate"]], df

def loadDateData(df: pd.DataFrame) -> int:
    logging.info("Loading date data into warehouse...")

    try:
        with supa.engine.begin() as conn:
            conn.execute(text('TRUNCATE TABLE olap."Date" RESTART IDENTITY CASCADE'))
            df.to_sql(
                "Date",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            result = conn.execute(text('SELECT COUNT(*) FROM "Date"'))
            total_inserted = result.scalar_one()

        logging.info(f"Loaded {total_inserted} records into Dates successfully.")
        return total_inserted

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def extractDate():
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["orders"])
    iddf = 0
    orders = metadata.tables["orders"]

    stmt = select(
        func.row_number().over(order_by=orders.c.createdAt).label("id"),
        orders.c.id.label("nat_key"),
        orders.c.deliveryDate,
    )

    logging.info("Extracting date data...")

    with extract() as session:
        df = pd.read_sql(stmt, session.bind)

    logging.info(f"Extracted {len(df)} raw date records.")
    df, iddf = cleanDateData(df)
    
    logging.info(
        f"Transformed date data: {
                 len(df)} records ready for loading."
    )
    total_inserted = loadDateData(df)

    logging.info(f"ETL process completed — totalInserted = {total_inserted}")
    #return {"totalInserted": total_inserted, "transformedRows": len(df)}
    return iddf

    
       