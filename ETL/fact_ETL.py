from config import local, supa
from contextlib import contextmanager
from sqlalchemy import select, literal, MetaData, text, func, cast, Integer
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


def cleanFactData(df: pd.DataFrame, user_df, loc_df, date_df, prod_df) -> pd.DataFrame:
    logging.info("Cleaning fact data...")

    df = df.dropna(subset=["orderNumber"]).copy()
    df["orderNumber"] = df["orderNumber"].str.strip().str.title()

    df = df.drop_duplicates(subset=["orderNumber"]).reset_index(drop=True)

    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df = df.dropna(subset=['revenue'])
    df['revenue'] = np.ceil(df['revenue'] * 100)/100
    
    merge_info = [
      ('UserId', user_df, 's_u_key', 'u_nat_key'),
      ('LocationId', loc_df, 's_l_key', 'l_nat_key'),
      ('DateId', date_df, 's_d_key', 'd_nat_key'),
      ('ProductId', prod_df, 's_p_key', 'p_nat_key')
    ]
    
    for col, dim_df, s_key, n_key in merge_info:
      df = df.merge(dim_df[['nat_key', 'id']], left_on=col, right_on='nat_key', how='left')
      df.rename(columns={'id': s_key, 'nat_key': n_key}, inplace=True)
      df[col] = df[s_key]
      df.drop(columns=[s_key, n_key], inplace=True)
      
    df["id"] = df.index + 1
    
    logging.info("Fact data cleaning completed successfully.")
    return df[["id","revenue","orderNumber","UserId","LocationId","DateId","ProductId"]]


def loadFactData(df: pd.DataFrame) -> int:
    logging.info("Loading fact data into warehouse...")

    try:
        with supa.engine.begin() as conn:
            conn.execute(text('TRUNCATE TABLE olap."FactSales" RESTART IDENTITY CASCADE'))
            df.to_sql(
                "FactSales",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            result = conn.execute(text('SELECT COUNT(*) FROM "FactSales"'))
            total_inserted = result.scalar_one()

        logging.info(f"Loaded {total_inserted} records into FactSales successfully.")
        return total_inserted

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


def extractFact(user_df, loc_df, date_df, prod_df):
    metadata = MetaData()
    metadata.reflect(bind=local.engine, only=["orderitems", "orders", "products", "users"])
    orderitems = metadata.tables["orderitems"]
    orders = metadata.tables["orders"]
    products = metadata.tables["products"]
    users = metadata.tables["users"]

    stmt = select(
        func.row_number().over(order_by=orders.c.orderNumber).label("id"),
        orderitems.c.quantity,
        (orderitems.c.quantity * products.c.price).label('revenue'),
        orders.c.orderNumber,
        users.c.id.label('UserId'),
        products.c.id.label('ProductId'),
        cast(literal(None), Integer).label('LocationId'),
        orders.c.id.label('DateId')    
    ).join(
        orders, orderitems.c.OrderId == orders.c.id
    ).join(
        products, orderitems.c.ProductId == products.c.id
    ).join(
        users, orders.c.userId == users.c.id
    )

    logging.info("Extracting fact data...")

    with extract() as session:
        df = pd.read_sql(stmt, session.bind)

    logging.info(f"Extracted {len(df)} raw fact records.")
    df = cleanFactData(df, user_df, loc_df, date_df, prod_df)

    logging.info(
        f"Transformed fact data: {
                 len(df)} records ready for loading."
    )
    total_inserted = loadFactData(df)

    logging.info(f"ETL process completed — totalInserted = {total_inserted}")
    return {"totalInserted": total_inserted, "transformedRows": len(df)}
