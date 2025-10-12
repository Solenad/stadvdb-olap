from config import local
from sqlalchemy import  select, func
from sqlalchemy.ext.automap import automap_base
import pandas as pd

def extractProduct():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

   Product = Base.classes.products
   
   stmt = select(func.row_number().over(order_by=Product.name).label('id'), (Product.id).label('nat_key'), Product.category, Product.description, Product.name, Product.price)
   
   df = pd.read_sql(stmt, engine)
   
   return df

def transformProduct(df):
   df['category'] = df['category'].replace({'Toy':'Toys', 'TOYS':'Toys', 'GADGETS':'Gadgets', 'make up': 'Make up', 'Makeup': 'Make up', 'BAG':'Bags'})
   
   return df