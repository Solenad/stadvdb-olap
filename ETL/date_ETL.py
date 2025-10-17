from config import local, supa
from contextlib import contextmanager
from sqlalchemy import select, MetaData, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
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


def cleanDateData(df: pd.DataFrame) -> pd.DataFrame:
    df['deliveryDate'] = pd.to_datetime(df['deliveryDate'], format='mixed').dt.normalize()
    
    df = df.rename(columns={'deliveryDate': 'date'})
    return df[["nat_key", "date"]]


def extractDate():
    start = time.time()
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["orders"])
    orders = metadata.tables["orders"]
    
    target_metadata = MetaData()
    target_metadata.reflect(bind=supa.engine, only=["Date"])
    target_date = target_metadata.tables["Date"]

    stmt = (
        select(
            orders.c.id.label("nat_key"),
            orders.c.deliveryDate,
        )
        .execution_options(stream_results=True, yield_per=BATCH_SIZE)
    )

    total_inserted = 0
    logging.info("Starting date data extraction.")

    mapping_data = []
    
    with extract() as session, warehouse_conn() as conn:
        result = session.execute(stmt)

        while True:
            chunk = list(itertools.islice(result, BATCH_SIZE))
            if not chunk:
                break

            df = pd.DataFrame(chunk, columns=result.keys())
            logging.info(f"Extracted {len(df)} raw date records.")
            
            df = cleanDateData(df)
            
            if df.empty:
                continue

            insert_data = df[["date"]].drop_duplicates().to_dict(orient="records")
            insert_stmt = insert(target_date).values(insert_data)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["date"],
                set_={
                      "date": insert_stmt.excluded.date
                }
            ).returning(target_date.c.id, target_date.c.date)

            result_set = conn.execute(upsert_stmt)
            conn.commit()
            db_rows = result_set.fetchall()
            total_inserted += len(db_rows)
            
            if db_rows:
                surrogate_key_df = pd.DataFrame(db_rows, columns=['id', 'date'])
                surrogate_key_df = surrogate_key_df.rename(columns={'id': 'surrogate_key'})
                surrogate_key_df['date'] = pd.to_datetime(surrogate_key_df['date'])

                merged_df = pd.merge(df, surrogate_key_df, on='date', how='inner')

                if not merged_df.empty:
                    mapping_data.append(merged_df[['nat_key', 'surrogate_key']])
                    
                print(merged_df)
            
            del df, chunk, db_rows, surrogate_key_df, merged_df
            gc.collect()

    mapped_df = pd.concat(mapping_data, ignore_index=True) if mapping_data else pd.DataFrame(columns=['nat_key', 'surrogate_key'])
    logging.info(f"ETL completed - {total_inserted} dates, {len(mapped_df)} mappings")
    end = time.time()
    length = end - start
    
    print("Date extraction took", length, "seconds")
    return mapped_df, {"totalInserted": total_inserted, "mapping": mapped_df}


    
       