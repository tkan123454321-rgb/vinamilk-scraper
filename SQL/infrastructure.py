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

def init_db_infrastructure(engine): # hàm tạo cơ sở hạ tầng database
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
        # tạo bảng báo cáo kết quả kinh doanh theo quý nếu chưa có
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.ic_quarter (
            "Ticker" VARCHAR(10) NOT NULL,
            "Year" INTEGER NOT NULL,
            "Quarter" INTEGER NOT NULL,
            "data" JSONB,
            "insert at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT PK_ic_quarter PRIMARY KEY ("Ticker", "Year", "Quarter"),
        CONSTRAINT fk_ic_quarter_ticker FOREIGN KEY ("Ticker") 
            REFERENCES analysis_data.companies_list("Ticker") 
            ON DELETE 
                CASCADE,
        CONSTRAINT chk_ic_quarter_year CHECK ("Year" >= 2019 AND "Year" <= EXTRACT(YEAR FROM CURRENT_DATE)),
        CONSTRAINT chk_ic_quarter_quarter CHECK ("Quarter" >= 1 AND "Quarter" <= 4)
        );
        """))
        # tạo bảng dữ liệu giá theo ngày nếu chưa có
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.daily_price (
            "Ticker" VARCHAR(10) NOT NULL,
            "data" JSONB,
            "insert at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT PK_daily_price PRIMARY KEY ("Ticker"),
        CONSTRAINT fk_daily_price_ticker FOREIGN KEY ("Ticker") 
            REFERENCES analysis_data.companies_list("Ticker") 
            ON DELETE 
                CASCADE
        );
        """))
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.daily_price_history (
            "Ticker" VARCHAR(10) NOT NULL,
            "open" FLOAT,
            "high" FLOAT,
            "low" FLOAT,
            "close" FLOAT,
            "volume" BIGINT,
            "date" DATE NOT NULL,
        CONSTRAINT PK_daily_price_history PRIMARY KEY ("Ticker", "date"),
        CONSTRAINT fk_daily_price_history_ticker FOREIGN KEY ("Ticker") 
            REFERENCES analysis_data.companies_list("Ticker") 
            ON DELETE 
                CASCADE
        );
        """))
        connection.commit()

def data_clean_sql(engine, inspector): # hàm dọn dẹp dữ liệu trong các bảng
    with engine.connect() as connection:
        transaction = connection.begin()
        try: 
            if inspector.has_table('companies_list', schema='analysis_data'): # chuẩn hóa dữ liệu bảng companies_list
                connection.execute(text("""
                UPDATE analysis_data.companies_list
                SET "Ticker" = UPPER(TRIM("Ticker")),
                    "Company Name" = LOWER(TRIM("Company Name")),
                    "Industry Name" = LOWER(TRIM("Industry Name"))
                """))
                
            tables = ['balance_sheet', 'income_statement', 'cash_flow_direct', 'financial_ratio']
            for table in tables:
                if inspector.has_table(table, schema='raw'): # xóa các bản ghi trùng lặp trong các bảng dữ liệu tài chính nếu có
                    connection.execute(text(f"""
                    ALTER TABLE raw.{table} 
                    ALTER COLUMN year TYPE INTEGER USING year::INTEGER;
                    
                    DELETE FROM raw.{table} a
                    USING raw.{table} b
                    WHERE a."Ticker" = b."Ticker"
                        AND a.year = b.year
                        AND a.ctid < b.ctid;
                    
                    UPDATE raw.{table} -- chuẩn hóa dữ liệu trong các bảng tài chính
                    SET "Ticker" = UPPER(TRIM("Ticker")),
                        year = CAST(year AS INTEGER); 

                    DELETE FROM raw.{table} -- xóa các bản ghi có year null hoặc year < 2000 hoặc year > current year
                    WHERE year IS NULL OR year < 2019 OR year > EXTRACT(YEAR FROM CURRENT_DATE);
                    """))
            # chuẩn hoá bảng financial_ratio với các điều kiện cụ thể
            cols01 = ['eps','bvps','pe','pb','ps','ev_ebitda','ebit_margin','gross_margin','net_margin','current_ratio','quick_ratio','interest_coverage_ratio','operating_cash_flow_per_share','operating_cash_flow_to_equity_ratio','operating_cash_flow_to_total_assets_ratio','operating_cash_flow_to_revenue_ratio'] # chuẩn hoá với điều kiện 0,âm vô cực, dương vô cực là null
            for col in cols01:
                connection.execute(text(f"""
                UPDATE raw.financial_ratio
                SET "{col}" = NULL
                WHERE "{col}" = 0 
                       OR CAST("{col}" AS TEXT) IN ('inf', '-inf', 'NaN', 'nan', 'None', 'Infinity', '-Infinity');
                """))
            cols02 = ['roaa','roea','roce','revenue_growth','net_income_growth','days_payable_outstanding','payable_turnover','cash_conversion_cycle'] # chuẩn hoá với điều kiện âm vô cực, dương vô cực là null, dữ liệu 2019 nếu là 0 = null
            cols03 = ['days_sales_outstanding','days_inventory_outstanding','inventory_turnover','total_asset_turnover','debt_to_equity_ratio','debt_to_assets_ratio']# chuẩn hoá với điều kiện âm vô cực, dương vô cực là null
            for col in cols02:
                connection.execute(text(f"""
                UPDATE raw.financial_ratio
                SET "{col}" = NULL
                WHERE CAST("{col}" AS TEXT) IN ('inf', '-inf', 'NaN', 'nan', 'None', 'Infinity', '-Infinity')
                       OR (year = 2019 AND "{col}" = 0);
                """))
            for col in cols03:
                connection.execute(text(f"""
                UPDATE raw.financial_ratio
                SET "{col}" = NULL
                WHERE CAST("{col}" AS TEXT) IN ('inf', '-inf', 'NaN', 'nan', 'None', 'Infinity', '-Infinity');
                """))

            
            transaction.commit() 
        except Exception as e:
            transaction.rollback()
            print(f"Error during data cleaning: {e}")

# 1. Hàm phụ trợ: Kiểm tra xem bảng đã có Primary Key chưa
def check_pk_exists(conn, table_name, schema='raw'):
    query = text(f"""
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
          AND constraint_type = 'PRIMARY KEY';
    """)
    return conn.execute(query).fetchone() is not None

# 2. Hàm phụ trợ 2: Kiểm tra xem bảng đã có Foreign Key chưa
def check_fk_exists(connection, table_name, schema='raw'):
    query = text(f"""
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
          AND constraint_type = 'FOREIGN KEY';
    """)
    return connection.execute(query).fetchone() is not None
# hàm kiểm tra check constraint tồn tại chưa
def check_check_exists(connection, table_name, schema='raw'):
    query = text(f"""
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
          AND constraint_type = 'CHECK';
    """)
    return connection.execute(query).fetchone() is not None

# 3. Hàm chính: Áp dụng các ràng buộc dữ liệu cho các bảng
def apply_constraints(engine):
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            # Thêm các ràng buộc vào bảng 
            tables = ['balance_sheet', 'income_statement', 'cash_flow_direct', 'financial_ratio']
            for table in tables:
                if not check_pk_exists(connection, table): # nếu chưa có PK thì thêm PK, còn có rồi thì bỏ qua
                    connection.execute(text(f"""
                        ALTER TABLE raw.{table}
                        ADD CONSTRAINT PK_{table}
                        PRIMARY KEY ("Ticker", year);
                    """))
                if not check_fk_exists(connection, table): # nếu chưa có FK thì thêm FK, còn có rồi thì bỏ qua
                    connection.execute(text(f"""
                    ALTER TABLE raw.{table} 
                    ADD CONSTRAINT fk_{table}_ticker
                    FOREIGN KEY ("Ticker")
                    REFERENCES analysis_data.companies_list("Ticker")
                    ON DELETE CASCADE;
                    """))
                if not check_check_exists(connection, table): # nếu chưa có CHECK thì thêm CHECK, còn có rồi thì bỏ qua
                    connection.execute(text(f"""
                    ALTER TABLE raw.{table}
                    ADD CONSTRAINT chk_{table}_year
                    CHECK (year >= 2019 AND year <= EXTRACT(YEAR FROM CURRENT_DATE));
                    """))
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(f"Error applying constraints: {e}")

if __name__ == "__main__":
    init_db_infrastructure(engine)