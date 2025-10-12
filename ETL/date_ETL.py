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


def loadDateData(df: pd.DataFrame) -> int:
    logging.info("Loading date data into warehouse...")

    try:
        with supa.engine.begin() as conn:
            conn.execute(text('TRUNCATE TABLE "Date" RESTART IDENTITY CASCADE'))
            df.to_sql(
                "Dates",
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
    orders = metadata.tables["orders"]

    stmt = select(
        func.row_number().over(order_by=orders.c.createdAt).label("id"),
        orders.c.id.label("nat_key"),
        orders.c.createdAt,
    )

    logging.info("Extracting date data...")

    with extract() as session:
        df = pd.read_sql(stmt, session.bind)

    logging.info(f"Extracted {len(df)} raw date records.")

    logging.info(
        f"Transformed date data: {
                 len(df)} records ready for loading."
    )
    total_inserted = loadDateData(df)

    logging.info(f"ETL process completed — totalInserted = {total_inserted}")
    return {"totalInserted": total_inserted, "transformedRows": len(df)}

    
       