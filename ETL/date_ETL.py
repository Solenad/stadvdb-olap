from config import local
from sqlalchemy import  select, automap_base
import pandas as pd

def extractDate():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

    #change the table if necessary
   Order = Base.classes.orders
   
   stmt = select(Order.createdAt)
   
   df = pd.read_sql(stmt, engine)
   
   return df
    
def loadDate(date, warehouse):
   date.to_sql(
      'Date',
      con = warehouse,
      if_exists='append',
      index = False
   )