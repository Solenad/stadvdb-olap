from config import local
from sqlalchemy import  select, func
from sqlalchemy.ext.automap import automap_base
import pandas as pd

def extractLocation():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

   User = Base.classes.users
   
   stmt = select(func.row_number().over(order_by=User.address1).label('id'), (User.id).label('nat_key'), User.address1, User.address2, User.city, User.country, User.zipCode)
   
   df = pd.read_sql(stmt, engine)
   
   return df

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


def cleanLocationData(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning location data...")

    df = df.dropna(subset=["address1", "address2", "city", "country", "zipCode"]).copy()
    df["address1"] = df["address1"].str.strip().str.title()
    df["address2"] = df["address2"].str.strip().str.title()
    df["city"] = df["city"].str.strip().str.title()
    df["country"] = df["country"].str.strip().str.title()
    df["zipCode"] = df["zipCode"].str.strip().str.title()   

    df = df.drop_duplicates(subset=["address1"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["address2"]).reset_index(drop=True)
    df["id"] = df.index + 1  # matches Supabase's Users.id structure

    logging.info("Location data cleaning completed successfully.")
    return df[["address1", "address2", "city", "country", "zipCode"]], df


def loadLocationData(df: pd.DataFrame) -> int:
    logging.info("Loading location data into warehouse...")

    try:
        with supa.engine.begin() as conn:
            conn.execute(text('TRUNCATE TABLE olap."Location" RESTART IDENTITY CASCADE'))
            df.to_sql(
                "Location",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            result = conn.execute(text('SELECT COUNT(*) FROM "Location"'))
            total_inserted = result.scalar_one()

        logging.info(f"Loaded {total_inserted} records into Location successfully.")
        return total_inserted

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def extractLocation():
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["users"])
    iddf = 0
    users = metadata.tables["users"]

    stmt = select(
        func.row_number().over(order_by=users.c.address1).label("id"),
        users.c.id.label("nat_key"),
        users.c.address1,
        users.c.address2,
        users.c.city,
        users.c.country,
        users.c.zipCode,
    )

    logging.info("Extracting location data...")

    with extract() as session:
        df = pd.read_sql(stmt, session.bind)

    logging.info(f"Extracted {len(df)} raw location records.")
    df, iddf = cleanLocationData(df)

    logging.info(
        f"Transformed location data: {
                 len(df)} records ready for loading."
    )
    total_inserted = loadLocationData(df)

    logging.info(f"ETL process completed — totalInserted = {total_inserted}")
    #return {"totalInserted": total_inserted, "transformedRows": len(df)}
    return iddf