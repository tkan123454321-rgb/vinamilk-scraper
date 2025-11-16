from vnstock import *
import pandas as pd
from datetime import *
from openpyxl import *
import os
from openpyxl.styles import *
from openpyxl.formatting.rule import *
from openpyxl.utils.dataframe import *
from openpyxl.utils import get_column_letter


def company_overview(co_phieu):
    co_phieu = co_phieu
    company = Company(symbol=co_phieu, source='VCI')
    company_data = company.overview()

    if not os.path.exists('data_raw/company_overview'):  # tạo thư mục
        os.makedirs('data_raw/company_overview')
    today = datetime.now().strftime("%d-%m-%Y")
    filename_raw = f'data_raw/company_overview/{co_phieu}_{today}_company_overview.xlsx'

    company_data.to_excel(filename_raw, index=False)

    # đọc file để sửa bằng panda
    df_company_data = pd.read_excel(filename_raw)
    # loại bỏ các cột không cần thiết
    #xoá khoảng trăng tên các column trong file excel
    df_company_data.columns = df_company_data.columns.str.strip()
    column_drop = ['id','icb_name2','icb_name4','financial_ratio_issue_share','charter_capital']
    df_company_data = df_company_data.drop(columns=column_drop, errors='ignore')
    # đổi tên 1 số cột cho dễ nhìn
    rename_dict = {
        'icb_name3': 'Ngành cấp 3',
        'symbol': 'Mã cổ phiếu',
        'issue_share': 'Số lượng cổ phiếu',
        'history' : 'Lịch sử công ty',
        'company_profile': 'Hồ sơ công ty'
        }
    df_company_data = df_company_data.rename(columns=rename_dict)
    return df_company_data

    