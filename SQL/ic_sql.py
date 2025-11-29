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
def income_statement(co_phieu):
    finace_data = Finance(symbol=co_phieu, source="VCI")
    finance = finace_data.income_statement(period='year', lang='vi')  # lấy dữ liệu báo cáo tài chính
    # tạo framework chuẩn
    finance_khoiphuc = finance.set_index('Năm').T #transpose data, set header chuẩn
    df_finance = finance_khoiphuc.reset_index().rename(columns = {'index':co_phieu})

    # # xoá rows xấu, ko cần thiết
    df_final = df_finance[~df_finance[co_phieu].isin([
        'CP',
        'Tăng trưởng doanh thu (%)',
        'Doanh thu (đồng)',
        'Lợi nhuận sau thuế của Cổ đông công ty mẹ (đồng)',
        'Tăng trưởng lợi nhuận (%)',
        'Lãi lỗ trong công ty liên doanh, liên kết'
        ])]

    # đổi tên các row cho đúng formal
    df_đổi_tên = {
        'Doanh thu thuần': 'Doanh thu thuần về bán hàng và cung cấp dịch vụ',
        'Lãi gộp': 'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
        'Thu nhập tài chính': 'Doanh thu hoạt động tài chính',
        'Lãi/Lỗ từ hoạt động kinh doanh': 'Lợi nhuận thuần từ hoạt động kinh doanh',
        'Thu nhập/Chi phí khác': 'Chi phí khác',
        'LN trước thuế': 'Tổng lợi nhuận kế toán trước thuế',
        'Lợi nhuận thuần': 'Lợi nhuận sau thuế thu nhập doanh nghiệp',
        'Cổ đông thiểu số': 'Lợi ích của cổ đông thiểu số',
        'Cổ đông của Công ty mẹ': 'Lợi nhuận sau thuế của cổ đông của Công ty mẹ',
        'Chi phí tiền lãi vay': 'Trong đó: chi phí tiền lãi vay'
    }
    df_final[co_phieu] = df_final[co_phieu].replace(df_đổi_tên)

    # sắp xếp các row cho đúng vị trí
    df_thutudung = [
        'Doanh thu bán hàng và cung cấp dịch vụ',
        'Các khoản giảm trừ doanh thu',
        'Doanh thu thuần về bán hàng và cung cấp dịch vụ',
        'Giá vốn hàng bán',
        'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
        'Doanh thu hoạt động tài chính',
        'Chi phí tài chính',
        'Trong đó: chi phí tiền lãi vay',
        'Lãi/lỗ từ công ty liên doanh',
        'Chi phí bán hàng',
        'Chi phí quản lý DN',
        'Lợi nhuận thuần từ hoạt động kinh doanh',
        'Thu nhập khác',
        'Chi phí khác',
        'Lợi nhuận khác',
        'Tổng lợi nhuận kế toán trước thuế',
        'Chi phí thuế TNDN hiện hành',
        'Chi phí thuế TNDN hoãn lại',
        'Lợi nhuận sau thuế thu nhập doanh nghiệp',
        'Lợi ích của cổ đông thiểu số',
        'Lợi nhuận sau thuế của cổ đông của Công ty mẹ'
    ]
    df_final = df_final.set_index(co_phieu).reindex(df_thutudung).reset_index()
    
    #xoá các cột ko cần thiết
    columns_to_drop = [str(column) for column in range(2013, 2019)]
    df_final.columns = [str(c).strip() for c in df_final.columns]
    current_columns = df_final.columns.tolist()
    columns_exist_to_drop = [col for col in columns_to_drop if col in current_columns]
    df_final = df_final.drop(columns=columns_exist_to_drop, errors='ignore')
    # đổi lại thứ tự các cột từ 2019-2024
    right_order = [str(year) for year in range(2019, 2025)]
    available_year = [col for col in right_order if col in df_final.columns]
    df_final = df_final[[co_phieu] + available_year]
    return df_final


if __name__ == "__main__":
    # test hàm
    df_ic = income_statement('QNP')
    
    print(df_ic.columns.tolist())

