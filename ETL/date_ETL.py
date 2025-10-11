from config import local
from sqlalchemy import  select, func
from sqlalchemy.ext.automap import automap_base
import pandas as pd

def extractDate():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

    #change the table if necessary
   Order = Base.classes.orders
   
   stmt = select(func.row_number().over(order_by=Order.createdAt).label('id'), (Order.id).label('nat_key'), Order.createdAt)
   
   df = pd.read_sql(stmt, engine)
   
   return df

    
       