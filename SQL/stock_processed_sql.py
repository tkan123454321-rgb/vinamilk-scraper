import requests
import pandas as pd
import ast
import os
import time
from datetime import *
import numpy as np
from openpyxl.styles import *
from openpyxl.formatting.rule import *
from openpyxl.utils.dataframe import *
from openpyxl.utils import get_column_letter
from openpyxl import *
import traceback
from balancesheet_sql import balance_sheet
from cf_sql import cash_flow
from ic_sql import income_statement
from price_history_sql import price_history
from openpyxl.utils import column_index_from_string




def stock_ratio_calculation(co_phieu):
    try:
        #lấy dữ liệu báo cáo tài chính
        df_income = income_statement(co_phieu)
        df_balancesheet = balance_sheet(co_phieu)
        df_cashflow = cash_flow(co_phieu)
        df_gia = price_history(co_phieu)
        #tạo file excel tổng hợp
        today = datetime.now().strftime("%d-%m-%Y")
        output_filename = f'data_processed/{co_phieu}_financial_data_{today}.xlsx'
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            #ghi sheet báo cáo kết quả kinh doanh
            df_income.to_excel(writer, sheet_name='Báo cáo kết quả kinh doanh', index=False)
            #ghi sheet bảng cân đối kế toán
            df_balancesheet.to_excel(writer, sheet_name='Bảng cân đối kế toán', index=False)
            #ghi sheet lưu chuyển tiền tệ
            df_cashflow.to_excel(writer, sheet_name='Báo cáo lưu chuyển tiền tệ', index=False)
            #ghi sheet giá lịch sử
            df_gia.to_excel(writer, sheet_name='Giá lịch sử', index=False)
    except Exception as e:
        print("Lỗi khi tạo file excel tổng hợp:", str(e))
        traceback.print_exc()
    return output_filename

