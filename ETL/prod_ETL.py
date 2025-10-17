from config import local, supa
from contextlib import contextmanager
from sqlalchemy import select, MetaData, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import numpy as np
import logging
import itertools
import gc
import os
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE") or 5000)


@contextmanager
def extract():
    session = local.Session()
    try:
        logging.info("Source DB connection established.")
        yield session
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise
    finally:
        session.close()
        logging.info("Source DB connection closed.")


@contextmanager
def warehouse_conn():
    conn = supa.engine.connect()
    try:
        yield conn
    except Exception as e:
        logging.error(f"Error during warehouse operation: {str(e)}")
        raise
    finally:
        conn.close()
        logging.info("Warehouse connection closed.")


def cleanProductData(df: pd.DataFrame) -> pd.DataFrame:
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
    return df[["nat_key", "category", "description", "name", "price"]]


def extractProduct():
    start = time.time()
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["products"])
    prods = metadata.tables["products"]
    
    target_metadata = MetaData()
    target_metadata.reflect(bind=supa.engine, only=["Products"])
    target_prods = target_metadata.tables["Products"]

    stmt = (
        select(
            prods.c.id.label("nat_key"),
            prods.c.category,
            prods.c.description,
            prods.c.name,
            prods.c.price,
        )
        .order_by(prods.c.name)
        .execution_options(stream_results=True, yield_per=BATCH_SIZE)
    )

    total_inserted = 0
    logging.info("Starting product data extraction.")

    mapping_data = []

    with extract() as session, warehouse_conn() as conn:
        result = session.execute(stmt)

        while True:
            chunk = list(itertools.islice(result, BATCH_SIZE))
            if not chunk:
                break

            df = pd.DataFrame(chunk, columns=result.keys())
            logging.info(f"Extracted {len(df)} raw product records.")
            
            df = cleanProductData(df)
            
            if df.empty:
                continue

            insert_data = df[["category", "description", "name", "price"]].to_dict(orient="records")
            insert_stmt = insert(target_prods).values(insert_data)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["name"],
                set_={
                    "category": insert_stmt.excluded.category,
                    "description": insert_stmt.excluded.description, 
                    "price": insert_stmt.excluded.price,
                },
            ).returning(target_prods.c.id, target_prods.c.name)

            result_set = conn.execute(upsert_stmt)
            conn.commit()
            db_rows = result_set.fetchall()
            
            for db_row in db_rows:
                name = db_row.name
                surrogate_key = db_row.id
                
                nat_keys = df[df['name'] == name]['nat_key'].tolist()
                for nat_key in nat_keys:
                    mapping_data.append({
                        'nat_key': nat_key,
                        'surrogate_key': surrogate_key,
                        'name': name
                    })
            
            total_inserted += len(df)
            del df, chunk
            gc.collect()

    mapped_df = pd.DataFrame(mapping_data)
    logging.info(f"ETL completed - {total_inserted} products, {len(mapped_df)} mappings")
    end = time.time()
    length = end - start

    print("Product extraction took", length, "secondss")
    return mapped_df, {"totalInserted": total_inserted, "mapping": mapped_df}

