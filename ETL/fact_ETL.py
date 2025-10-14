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


def cleanFactData(df: pd.DataFrame, user_df, loc_df, date_df, prod_df) -> pd.DataFrame:
    df = df.dropna(subset=["OrderNumber"]).copy()
    df["OrderNumber"] = df["OrderNumber"].str.strip().str.title()

    df = df.drop_duplicates(subset=["OrderNumber"]).reset_index(drop=True)

    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue'])
    df['revenue'] = np.ceil(df['revenue'] * 100)/100
    
    merge_info = [
      ('UserId', user_df),
      ('LocationId', loc_df),
      ('DateId', date_df),
      ('ProductId', prod_df)
    ]
    
    for col, dim_df in merge_info:
        n_to_s = dict(zip(dim_df['nat_key'], dim_df['surrogate_key']))
        mapped = df[col].map(n_to_s)

        mask = mapped.notna()
        df = df.loc[mask].copy()
        
        df[col] = mapped.loc[mask].astype('int64')
    
    return df[["quantity","revenue","OrderNumber","UserId","LocationId","DateId","ProductId"]]


def extractFact(user_df, loc_df, date_df, prod_df):
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["orderitems","users","products","orders"])
    oitems = metadata.tables["orderitems"]
    users = metadata.tables["users"]
    orders = metadata.tables["orders"]
    products = metadata.tables["products"]

    target_metadata = MetaData()
    target_metadata.reflect(bind=supa.engine, only=["FactSales"])
    target_facts = target_metadata.tables["FactSales"]
    stmt = (
        select(
                oitems.c.quantity,
                (oitems.c.quantity * products.c.price).label('revenue'),
                orders.c.orderNumber.label('OrderNumber'),
                users.c.id.label('UserId'),
                products.c.id.label('ProductId'),
                users.c.id.label('LocationId'),
                orders.c.id.label('DateId')    
            ).join(
                orders, oitems.c.OrderId == orders.c.id
            ).join(
                products, oitems.c.ProductId == products.c.id
            ).join(
                users, orders.c.userId == users.c.id
            )
        ).order_by(users.c.username).execution_options(stream_results=True, yield_per=BATCH_SIZE)

    total_inserted = 0
    logging.info("Starting fact data extraction.")

    with extract() as session, warehouse_conn() as conn:
        result = session.execute(stmt)

        while True:
            chunk = list(itertools.islice(result, BATCH_SIZE))
            if not chunk:
                break

            df = pd.DataFrame(chunk, columns=result.keys())
            df = cleanFactData(df,user_df, loc_df, date_df, prod_df)
            if df.empty:
                continue
            insert_data = df[["quantity", "revenue", "UserId", "ProductId", "LocationId", "DateId", "OrderNumber"]].to_dict(orient="records")
            insert_stmt = insert(target_facts).values(insert_data)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["OrderNumber"],
                set_={
                    "quantity": insert_stmt.excluded.quantity,
                    "revenue": insert_stmt.excluded.revenue,
                    "UserId": insert_stmt.excluded.UserId,
                    "ProductId": insert_stmt.excluded.ProductId,
                    "LocationId": insert_stmt.excluded.LocationId,
                    "DateId": insert_stmt.excluded.DateId,
                },
            )
            conn.execute(upsert_stmt)
            conn.commit()
            total_inserted += len(df)
            logging.info(f"Processed {total_inserted} records so far.")
            del df, chunk
            gc.collect()

    logging.info(
        f"ETL completed successfully — totalInserted = {
            total_inserted}"
    )
    return {"totalInserted": total_inserted}
