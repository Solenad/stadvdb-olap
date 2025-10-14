from config import local, supa
from contextlib import contextmanager
from sqlalchemy import select, MetaData, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import logging
import itertools
import gc
import os

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


def cleanLocationData(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["address1", "address2", "city", "country", "zipCode"]).copy()
    df["address1"] = df["address1"].str.strip().str.title()
    df["address2"] = df["address2"].str.strip().str.title()
    df["city"] = df["city"].str.strip().str.title()
    df["country"] = df["country"].str.strip().str.title()
    df["zipCode"] = df["zipCode"].str.strip().str.title()   

    df = df.drop_duplicates(subset=["address1"]).reset_index(drop=True)
    return df[["nat_key", "address1", "address2", "city", "country", "zipCode"]]


def extractLocation():
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["users"])
    users = metadata.tables["users"]
    
    target_metadata = MetaData()
    target_metadata.reflect(bind=supa.engine, only=["Location"])
    target_locs = target_metadata.tables["Location"]

    stmt = (
        select(
            users.c.id.label("nat_key"),
            users.c.address1,
            users.c.address2,
            users.c.city,
            users.c.country,
            users.c.zipCode,
        )
        .order_by(users.c.address1)
        .execution_options(stream_results=True, yield_per=BATCH_SIZE)
    )

    total_inserted = 0
    logging.info("Starting location data extraction.")

    mapping_data = []

    with extract() as session, warehouse_conn() as conn:
        result = session.execute(stmt)

        while True:
            chunk = list(itertools.islice(result, BATCH_SIZE))
            if not chunk:
                break

            df = pd.DataFrame(chunk, columns=result.keys())
            logging.info(f"Extracted {len(df)} raw location records.")
            
            df = cleanLocationData(df)
            
            if df.empty:
                continue

            insert_data = df[["address1", "address2", "city", "country", "zipCode"]].to_dict(orient="records")
            insert_stmt = insert(target_locs).values(insert_data)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["address1"],
                set_={
                    "address2": insert_stmt.excluded.address2,
                    "city": insert_stmt.excluded.city, 
                    "country": insert_stmt.excluded.country,
                    "zipCode": insert_stmt.excluded.zipCode,
                },
            ).returning(target_locs.c.id, target_locs.c.address1)

            result_set = conn.execute(upsert_stmt)
            conn.commit()
            db_rows = result_set.fetchall()
            
            for db_row in db_rows:
                address1 = db_row.address1
                surrogate_key = db_row.id
                
                nat_keys = df[df['address1'] == address1]['nat_key'].tolist()
                for nat_key in nat_keys:
                    mapping_data.append({
                        'nat_key': nat_key,
                        'surrogate_key': surrogate_key,
                        'address1': address1
                    })
            
            total_inserted += len(df)

    mapped_df = pd.DataFrame(mapping_data)
    print(mapped_df)
    logging.info(f"ETL completed - {total_inserted} locations, {len(mapped_df)} mappings")
    return mapped_df, {"totalInserted": total_inserted, "mapping": mapped_df}
