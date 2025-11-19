from IPython.display import display
from vnstock import *
import pandas as pd
from datetime import *
from openpyxl import *
import os
from openpyxl.styles import *
from openpyxl.formatting.rule import *
from openpyxl.utils.dataframe import *
from openpyxl.utils import get_column_letter
import traceback
def price_history(co_phieu):
    try: 
        if not os.path.exists('data_raw/history_price'):  # tạo thư mục
            os.makedirs('data_raw/history_price')
        today = datetime.now().strftime("%Y-%m-%d")
        quote = Quote(symbol=co_phieu, source="VCI")
        data = quote.history(start='2019-01-01', end=today, interval='1D')  # lấy dữ liệu gía lịch sử
        df_history_price = data
        # căn chỉnh lại cột thời gian
        df_history_price['time'] = df_history_price['time'].dt.strftime('%d-%m-%Y')
    except Exception as e:
        print("Lỗi khi lấy dữ liệu giá lịch sử:", str(e))
        traceback.print_exc()
    return df_history_price
    

