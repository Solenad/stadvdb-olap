from config import local
from sqlalchemy import  select, automap_base
import pandas as pd

def extractProduct():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

   Product = Base.classes.products
   
   stmt = select(Product.category, Product.description, Product.name, Product.price)
   
   df = pd.read_sql(stmt, engine)
   
   return df