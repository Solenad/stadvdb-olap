from config import local
from sqlalchemy import  select, automap_base
import pandas as pd

def extractUser():
   engine = local.localConnect()
   
   Base = automap_base()
   
   Base.prepare(autoload_with=engine)

   User = Base.classes.users
   
   stmt = select(User.username, User.firstName, User.lastName, User.dateOfBirth, User.gender)
   
   df = pd.read_sql(stmt, engine)
   
   return df

# def transformUser():

# gets user from extract and warehouse for connection
def loadUser(user, warehouse):
   user.to_sql(
      'Users',
      con = warehouse,
      if_exists='append',
      index = False
   )
