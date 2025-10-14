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


def cleanUserData(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["username", "firstName", "lastName"]).copy()
    df["username"] = df["username"].str.strip().str.lower()
    df["firstName"] = df["firstName"].str.strip().str.title()
    df["lastName"] = df["lastName"].str.strip().str.title()
    df["gender"] = (
        df["gender"]
        .str.strip()
        .str.lower()
        .replace({"male": "M", "female": "F", "m": "M", "f": "F"})
    )
    df["dateOfBirth"] = pd.to_datetime(df["dateOfBirth"], errors="coerce")
    df = df.dropna(subset=["dateOfBirth"])
    df["dateOfBirth"] = df["dateOfBirth"].dt.date
    df = df.drop_duplicates(subset=["username"]).reset_index(drop=True)
    return df[["nat_key", "username", "firstName", "lastName", "dateOfBirth", "gender"]]

def extractUser():
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["users"])
    users = metadata.tables["users"]
    
    target_metadata = MetaData()
    target_metadata.reflect(bind=supa.engine, only=["Users"])
    target_users = target_metadata.tables["Users"]

    stmt = (
        select(
            users.c.id.label("nat_key"),
            users.c.username,
            users.c.firstName,
            users.c.lastName,
            users.c.dateOfBirth,
            users.c.gender,
        )
        .order_by(users.c.username)
        .execution_options(stream_results=True, yield_per=BATCH_SIZE)
    )

    total_inserted = 0
    logging.info("Starting user data extraction.")

    mapping_data = []

    with extract() as session, warehouse_conn() as conn:
        result = session.execute(stmt)

        while True:
            chunk = list(itertools.islice(result, BATCH_SIZE))
            if not chunk:
                break

            df = pd.DataFrame(chunk, columns=result.keys())
            logging.info(f"Extracted {len(df)} raw user records.")
            
            df = cleanUserData(df)
            
            if df.empty:
                continue

            insert_data = df[["username", "firstName", "lastName", "dateOfBirth", "gender"]].to_dict(orient="records")
            insert_stmt = insert(target_users).values(insert_data)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["username"],
                set_={
                    "firstName": insert_stmt.excluded.firstName,
                    "lastName": insert_stmt.excluded.lastName, 
                    "dateOfBirth": insert_stmt.excluded.dateOfBirth,
                    "gender": insert_stmt.excluded.gender,
                },
            ).returning(target_users.c.id, target_users.c.username)

            result_set = conn.execute(upsert_stmt)
            conn.commit()
            db_rows = result_set.fetchall()
            
            for db_row in db_rows:
                username = db_row.username
                surrogate_key = db_row.id
                
                nat_keys = df[df['username'] == username]['nat_key'].tolist()
                for nat_key in nat_keys:
                    mapping_data.append({
                        'nat_key': nat_key,
                        'surrogate_key': surrogate_key,
                        'username': username
                    })
            
            total_inserted += len(df)

    mapped_df = pd.DataFrame(mapping_data)
    print(mapped_df)
    logging.info(f"ETL completed - {total_inserted} users, {len(mapped_df)} mappings")
    return mapped_df, {"totalInserted": total_inserted, "mapping": mapped_df}
