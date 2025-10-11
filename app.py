from config import local
from ETL import user_ETL, date_ETL, loc_ETL, prod_ETL
import pandas as pd

try:
    local.localConnect()
    print('Successfully connected to Local DB!')
    
    user_df = user_ETL.extractUser()
    usert_df = user_ETL.transformUser(user_df)
    
    loc_df = loc_ETL.extractLocation()
    
    date_df = date_ETL.extractDate()
    
    prod_df = prod_ETL.extractProduct()
    prodt_df = prod_ETL.transformProduct(prod_df)
    print(prodt_df)
    
    
except Exception as e:
    print(f'Connection Failed... {e}')
    
