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