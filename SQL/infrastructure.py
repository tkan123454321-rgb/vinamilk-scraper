from IPython.display import display
from vnstock import *
import pandas as pd
import time
import os
import shutil
from sqlalchemy import *
from urllib.parse import quote_plus
# Database connection setup
db_user = 'tkan'
db_password = 'Maihainganha@1'
db_host = 'localhost'  
db_port = '5432'       
db_name = 'finance_db' 
password = quote_plus(db_password) # Encode password
connection_str = f'postgresql://{db_user}:{password}@{db_host}:{db_port}/{db_name}' # Connection string
engine = create_engine(connection_str) # Create engine
inspector = inspect(engine) # Create inspector

def init_db_infrastructure():
    with engine.connect() as connection:
        # tạo schema nếu chưa có 
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS analysis_data;"))
        
        #tạo bảng master (company_list) nếu chưa có)
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS analysis_data.companies_list (
         "Ticker" VARCHAR(10) PRIMARY KEY,
        "Company Name" VARCHAR(300),
         "Industry Name" VARCHAR(200)
        );
        """))
        connection.commit()
    