from config import local, supa
from contextlib import contextmanager
from sqlalchemy import select, func, MetaData, text
import pandas as pd
import numpy as np
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


def cleanProductData(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning product data...")

    df = df.dropna(subset=["category", "description", "name", "price"]).copy()
    df["category"] = df["category"].str.strip().str.lower()
    df["description"] = df["description"].str.strip().str.title()
    df["name"] = df["name"].str.strip().str.title()
    
    df['price'] = np.ceil(df['price'] * 100)/100
    
    df['category'] = df['category'].replace(
       {'toy':'Toys', 
        'toys':'Toys', 
        'gadgets':'Gadgets', 
        'makeup': 'Make up', 
        'bag':'Bags'})

    df = df.drop_duplicates(subset=["name"]).reset_index(drop=True)
    df["id"] = df.index + 1  # matches Supabase's Users.id structure

    logging.info("Product data cleaning completed successfully.")
    return df[["category", "description", "name", "price"]]


def loadProductData(df: pd.DataFrame) -> int:
    logging.info("Loading product data into warehouse...")

    try:
        with supa.engine.begin() as conn:
            conn.execute(text('TRUNCATE TABLE "Products" RESTART IDENTITY CASCADE'))
            df.to_sql(
                "Products",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            result = conn.execute(text('SELECT COUNT(*) FROM "Products"'))
            total_inserted = result.scalar_one()

        logging.info(f"Loaded {total_inserted} records into Products successfully.")
        return total_inserted

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def extractProduct():
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["products"])
    products = metadata.tables["products"]

    stmt = select(
        func.row_number().over(order_by=products.c.category).label("id"),
        products.c.id.label("nat_key"),
        products.c.category,
        products.c.description,
        products.c.name,
        products.c.price,
    )

    logging.info("Extracting product data...")

    with extract() as session:
        df = pd.read_sql(stmt, session.bind)

    logging.info(f"Extracted {len(df)} raw product records.")
    df = cleanProductData(df)

    logging.info(
        f"Transformed product data: {
                 len(df)} records ready for loading."
    )
    total_inserted = loadProductData(df)

    logging.info(f"ETL process completed — totalInserted = {total_inserted}")
    return {"totalInserted": total_inserted, "transformedRows": len(df)}
