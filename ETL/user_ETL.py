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
    conn = supa.engine.begin()
    try:
        yield conn
    except Exception as e:
        logging.error(f"Error during warehouse operation: {e}")
        raise
    finally:
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

    with extract() as session, warehouse_conn() as conn:
        result = session.execute(stmt)

        while True:
            chunk = list(itertools.islice(result, BATCH_SIZE))
            if not chunk:
                break

            df = pd.DataFrame(chunk, columns=result.keys())
            df = cleanUserData(df)
            if df.empty:
                continue

            insert_stmt = insert(text('"Users"')).values(df.to_dict(orient="records"))
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["nat_key"],
                set_={
                    "username": insert_stmt.excluded.username,
                    "firstName": insert_stmt.excluded.firstName,
                    "lastName": insert_stmt.excluded.lastName,
                    "dateOfBirth": insert_stmt.excluded.dateOfBirth,
                    "gender": insert_stmt.excluded.gender,
                },
            )
            conn.execute(upsert_stmt)
            total_inserted += len(df)
            logging.info(f"Processed {total_inserted} records so far.")
            del df, chunk
            gc.collect()

    logging.info(
        f"ETL completed successfully — totalInserted = {
            total_inserted}"
    )
    return {"totalInserted": total_inserted}
