from config import local
from sqlalchemy import  select, automap_base, func
import pandas as pd

def extractUser():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

   User = Base.classes.users
   
   stmt = select(func.row_number().over(order_by=User.username).label('id'),User.username, User.firstName, User.lastName, User.dateOfBirth, User.gender)
   
   df = pd.read_sql(stmt, engine)
   
   return df
