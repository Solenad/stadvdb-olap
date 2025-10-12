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


def cleanUserData(df: pd.DataFrame, iddf) -> pd.DataFrame:
    logging.info("Cleaning user data...")

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
    df["id"] = df.index + 1  # matches Supabase's Users.id structure
    
    logging.info("User data cleaning completed successfully.")
    return df[["id", "username", "firstName", "lastName", "dateOfBirth", "gender"]], df


def loadUserData(df: pd.DataFrame) -> int:
    logging.info("Loading user data into warehouse...")

    try:
        with supa.engine.begin() as conn:
            conn.execute(text('TRUNCATE TABLE olap."Users" RESTART IDENTITY CASCADE'))
            df.to_sql(
                "Users",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            result = conn.execute(text('SELECT COUNT(*) FROM "Users"'))
            total_inserted = result.scalar_one()

        logging.info(f"Loaded {total_inserted} records into Users successfully.")
        return total_inserted

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def extractUser():
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["users"])
    iddf = 0
    users = metadata.tables["users"]

    stmt = select(
        func.row_number().over(order_by=users.c.username).label("id"),
        users.c.id.label("nat_key"),
        users.c.username,
        users.c.firstName,
        users.c.lastName,
        users.c.dateOfBirth,
        users.c.gender,
    )

    logging.info("Extracting user data...")

    with extract() as session:
        df = pd.read_sql(stmt, session.bind)

    logging.info(f"Extracted {len(df)} raw user records.")
    df, iddf = cleanUserData(df, iddf)

    logging.info(
        f"Transformed user data: {
                 len(df)} records ready for loading."
    )
    total_inserted = loadUserData(df)

    logging.info(f"ETL process completed — totalInserted = {total_inserted}")
    #return {"totalInserted": total_inserted, "transformedRows": len(df)}
    return iddf
