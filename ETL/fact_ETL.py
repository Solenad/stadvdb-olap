from config import local
from sqlalchemy import  select
from sqlalchemy.ext.automap import automap_base
import pandas as pd

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