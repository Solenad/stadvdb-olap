from config import local
from sqlalchemy import  select, automap_base
import pandas as pd

def extractLocation():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

   User = Base.classes.user
   
   stmt = select(User.address1, User.address2, User.city, User.country, User.zipCode)
   
   df = pd.read_sql(stmt, engine)
   
   return df

def loadLocation(location, warehouse):
   location.to_sql(
      'Location',
      con = warehouse,
      if_exists='append',
      index = False
   )