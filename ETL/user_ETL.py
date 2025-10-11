from config import local
from sqlalchemy import  select, func
from sqlalchemy.ext.automap import automap_base
import pandas as pd

def extractUser():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

   User = Base.classes.users
   
   stmt = select(func.row_number().over(order_by=User.username).label('id'), (User.id).label('nat_key'), User.username, User.firstName, User.lastName, User.dateOfBirth, User.gender)
   
   df = pd.read_sql(stmt, engine)
   
   return df

def transformUser(df):
   df = df.dropna()
   
   df['gender'] = df['gender'].replace({'Male':'M','Female':'F'})
   
   df['dateOfBirth'] = pd.to_datetime(df['dateOfBirth'],format='mixed', errors='coerce')
   df['dateOfBirth'] = df['dateOfBirth'].dt.strftime('%m/%d/%Y')
   
   return df
