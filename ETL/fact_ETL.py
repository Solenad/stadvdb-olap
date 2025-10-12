from config import local
from sqlalchemy import  select
from sqlalchemy.ext.automap import automap_base
import pandas as pd
import numpy as np

def extractFact():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

   Product = Base.classes.products
   OrderItem = Base.classes.orderitems
   Order = Base.classes.orders
   
   #sa dataframe (transform) ilalagay ung surrogate keys
   stmt = select(OrderItem.quantity, (OrderItem.quantity * Product.price).label('revenue'), Order.orderNumber, Product.id).join(
    Order, OrderItem.OrderId == Order.id).join(Product, OrderItem.ProductId == Product.id)
   
   df = pd.read_sql(stmt, engine)
   
   return df

def transformFact(fact_df, user_df, loc_df, date_df, prod_df):
   
   fact_df['revenue'] = np.ceil(fact_df['revenue'] * 100)/100
   
   merge_info = [
      ('UserId', user_df, 's_u_key', 'u_nat_key'),
      ('LocationId', loc_df, 's_l_key', 'l_nat_key'),
      ('DateId', date_df, 's_d_key', 'd_nat_key'),
      ('ProductId', prod_df, 's_p_key', 'p_nat_key')
   ]

   for col, dim_df, s_key, n_key in merge_info:
      fact_df = fact_df.merge(dim_df[['nat_key', 'id']], 
                              left_on=col, right_on='nat_key', how='left')
      fact_df.rename(columns={'id': s_key, 'nat_key': n_key}, inplace=True)
      fact_df[col] = fact_df[s_key]
      fact_df.drop(columns=[s_key, n_key], inplace=True)

   
   return fact_df