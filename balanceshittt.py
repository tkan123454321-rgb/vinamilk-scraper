import requests
import pandas as pd
import ast
import os
import time
import numpy as np
from openpyxl.styles import *
from openpyxl.formatting.rule import *
from openpyxl.utils.dataframe import *
from openpyxl.utils import get_column_letter
from openpyxl import *
def balance_sheet(co_phieu):
    stock = co_phieu
    auth_token ='Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
    url = f'https://restv2.fireant.vn/symbols/{stock}/full-financial-reports?type=1&year=2025&quarter=0&limit=6'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'


    params = {
        'type': '1',
        'year': '2025',
        'quarter': '0',
        'limit': '6'
    }

    headers = {
        'User-Agent': user_agent,
        'Authorization': auth_token
        }

    response = requests.get(url, headers=headers, params=params)
    response = response.json()

    data = pd.DataFrame(response)

    #xuất excel dữ liệu thô
    os.makedirs('data_raw/data_balance_sheet', exist_ok=True)
    filename_raw = f'{stock}_financial_reports_raw.xlsx'
    data.to_excel(os.path.join('data_raw','data_balance_sheet', filename_raw), index=False)
    df_raw = pd.read_excel(os.path.join('data_raw','data_balance_sheet',filename_raw))

    #đặt tên cho file đã qua xử lý
    filename_processed = f'{stock}_financial_reports_processed.xlsx'

    # đổi từ string sang python_language
    df_raw['values'] = df_raw['values'].apply(ast.literal_eval)
    #explode cột values thành nhiều hàng
    df_raw = df_raw.explode('values')
    df_normalized = pd.json_normalize(df_raw['values'])


    #nối df_raw với df_normalized
    df_raw = pd.concat([df_raw['name'].reset_index(drop=True), df_normalized], axis=1)
    df_raw = df_raw[['name','year','value']]

    # đánh số 1 số tên
    danh_sach =  ['- Giá trị hao mòn lũy kế','- Nguyên giá']
    mask = df_raw['name'].isin(danh_sach)
    cumcount_series = df_raw.groupby('name').cumcount().astype(str)
    df_raw['name'] =np.where(mask, df_raw['name'] + '_' + cumcount_series, df_raw['name'])

    df_raw = df_raw.pivot_table(index='name', columns='year', values='value').reset_index()

    #sắp xếp cột nguyên giá, hao mòn luỹ kế theo đúng thứ tự
    df_raw[['sorted name','sorted number']] = df_raw['name'].str.split('_', expand=True,n=1)
    df_raw['sorted number'] = pd.to_numeric(df_raw['sorted number'], errors='coerce').fillna(-1).astype(int)
    df_raw = df_raw.sort_values(by=['sorted name','sorted number'], ascending=[True, True])
    df_raw = df_raw.drop(columns=['sorted name', 'sorted number'])

    # gộp các giá trị trùng vào thành 1 hàng
    parts = df_raw['name'].str.rsplit('_', n=1, expand=True)
    phần_chữ = parts[0]
    phần_số = pd.to_numeric(parts[1], errors='coerce')

    #tạo điều kiện để so sanh danh sách 
    mask_target = (phần_chữ.isin(danh_sach)) & (phần_số.notna())

    # nhóm mỗi 6 hàng thành 1 nhóm
    group_start = (phần_số // 6 ).astype('Int64')
    df_raw['group'] = np.where(mask_target, phần_chữ + '_' + group_start.astype(str), df_raw['name'])

    #tạo danh sách cột năm
    years = [c for c in df_raw.columns if c not in ('name','group')]

    # bắt đầu ép


    def first_nonnull(s): # tạo hàm để chọn ra giá trị ko rỗng trong cột năm
        nz = s.dropna()
        return nz.iloc[0] if not nz.empty else pd.NA

    agg_dict = {}
    for year in years:
        agg_dict[year] = first_nonnull


    df_grouped = df_raw.groupby('group', sort=False).agg(agg_dict).reset_index()

    # giữ thứ tự xuất hiện ban đầu của nhóm
    thứ_tự = '''
    A. Tài sản lưu động và đầu tư ngắn hạn	
    I. Tiền và các khoản tương đương tiền	
    1. Tiền	
    2. Các khoản tương đương tiền	
    II. Các khoản đầu tư tài chính ngắn hạn	
    1. Chứng khoán kinh doanh	
    2. Dự phòng giảm giá chứng khoán kinh doanh	
    3. Đầu tư nắm giữ đến ngày đáo hạn	
    III. Các khoản phải thu ngắn hạn	
    1. Phải thu ngắn hạn của khách hàng	
    2. Trả trước cho người bán	
    3. Phải thu nội bộ ngắn hạn	
    4. Phải thu theo tiến độ hợp đồng xây dựng	
    5. Phải thu về cho vay ngắn hạn	
    6. Phải thu ngắn hạn khác	
    7. Dự phòng phải thu ngắn hạn khó đòi	
    IV. Tổng hàng tồn kho	
    1. Hàng tồn kho	
    2. Dự phòng giảm giá hàng tồn kho	
    V. Tài sản ngắn hạn khác	
    1. Chi phí trả trước ngắn hạn	
    2. Thuế giá trị gia tăng được khấu trừ	
    3. Thuế và các khoản phải thu Nhà nước	
    4. Giao dịch mua bán lại trái phiếu chính phủ	
    5. Tài sản ngắn hạn khác
    B. Tài sản cố định và đầu tư dài hạn	
    I. Các khoản phải thu dài hạn	
    1. Phải thu dài hạn của khách hàng	
    2. Vốn kinh doanh tại các đơn vị trực thuộc	
    3. Phải thu dài hạn nội bộ	
    4. Phải thu về cho vay dài hạn	                                   
    5. Phải thu dài hạn khác	
    6. Dự phòng phải thu dài hạn khó đòi	
    II. Tài sản cố định	
    1. Tài sản cố định hữu hình	
    - Nguyên giá_0	
    - Giá trị hao mòn lũy kế_0	
    2. Tài sản cố định thuê tài chính	
    - Nguyên giá_1	
    - Giá trị hao mòn lũy kế_1
    3. Tài sản cố định vô hình	
    - Nguyên giá_2
    - Giá trị hao mòn lũy kế_2
    III. Bất động sản đầu tư	
    - Nguyên giá_3	
    - Giá trị hao mòn lũy kế_3	
    IV. Tài sản dở dang dài hạn	
    1. Chi phí sản xuất, kinh doanh dở dang dài hạn
    2. chi phí xây dựng cơ bản dở dang	
    V. Các khoản đầu tư tài chính dài hạn	
    1. Đầu tư vào công ty con	
    2. Đầu tư vào công ty liên kết, liên doanh	
    3. Đầu tư khác vào công cụ vốn	
    4. Dự phòng giảm giá đầu tư tài chính dài hạn	
    5. Đầu tư nắm giữ đến ngày đáo hạn	
    VI. Tổng tài sản dài hạn khác	
    1. Chi phí trả trước dài hạn	
    2. Tài sản Thuế thu nhập hoãn lại	
    3. Tài sản dài hạn khác	
    VII. Lợi thế thương mại	
    TỔNG CỘNG TÀI SẢN	
    NGUỒN VỐN	
    A. Nợ phải trả	
    I. Nợ ngắn hạn	
    1. Vay và nợ thuê tài chính ngắn hạn	
    2. Vay và nợ dài hạn đến hạn phải trả	
    3. Phải trả người bán ngắn hạn	
    4. Người mua trả tiền trước	
    5. Thuế và các khoản phải nộp nhà nước	
    6. Phải trả người lao động	
    7. Chi phí phải trả ngắn hạn	
    8. Phải trả nội bộ ngắn hạn	
    9. Phải trả theo tiến độ kế hoạch hợp đồng xây dựng	
    10. Doanh thu chưa thực hiện ngắn hạn	
    11. Phải trả ngắn hạn khác	
    12. Dự phòng phải trả ngắn hạn	
    13. Quỹ khen thưởng phúc lợi	
    14. Quỹ bình ổn giá	
    15. Giao dịch mua bán lại trái phiếu chính phủ	
    II. Nợ dài hạn	
    1. Phải trả người bán dài hạn	
    2. Chi phí phải trả dài hạn	
    3. Phải trả nội bộ về vốn kinh doanh	
    4. Phải trả nội bộ dài hạn	
    5. Phải trả dài hạn khác	
    6. Vay và nợ thuê tài chính dài hạn	
    7. Trái phiếu chuyển đổi	
    8. Thuế thu nhập hoãn lại phải trả	
    9. Dự phòng trợ cấp mất việc làm	
    10. Dự phòng phải trả dài hạn	
    11. Doanh thu chưa thực hiện dài hạn	
    12. Quỹ phát triển khoa học và công nghệ	
    B. Nguồn vốn chủ sở hữu	
    I. Vốn chủ sở hữu	
    1. Vốn đầu tư của chủ sở hữu	
    2. Thặng dư vốn cổ phần	
    3. Quyền chọn chuyển đổi trái phiếu	
    4. Vốn khác của chủ sở hữu	
    5. Cổ phiếu quỹ	
    6. Chênh lệch đánh giá lại tài sản	
    7. Chênh lệch tỷ giá hối đoái	
    8. Quỹ đầu tư phát triển	
    9. Quỹ dự phòng tài chính	
    10. Quỹ khác thuộc vốn chủ sở hữu	
    11. Lợi nhuận sau thuế chưa phân phối	
    - LNST chưa phân phối lũy kế đến cuối kỳ trước	
    - LNST chưa phân phối kỳ này	
    12. Nguồn vốn đầu tư xây dựng cơ bản	
    13. Quỹ hỗ trợ sắp xếp doanh nghiệp	
    14. Lợi ích của cổ đông không kiểm soát	
    II. Nguồn kinh phí và quỹ khác	
    1. Nguồn kinh phí	
    2. Nguồn kinh phí đã hình thành tài sản cố định	
    3. Quỹ dự phòng trợ cấp mất việc làm	
    TỔNG CỘNG NGUỒN VỐN
    '''
    fireant_list_raw = thứ_tự.strip().splitlines()
    fireant_list_processed = []
    for item in fireant_list_raw:
        item_sach = item.strip()
        fireant_list_processed.append(item_sach)
    df_grouped =df_grouped.set_index('group').reindex(fireant_list_processed).reset_index()

    #đổi tên để nhận diện tên cổ phiếu đang xem
    df_grouped = df_grouped.rename(columns={'group': stock})
    #lưu file
    os.makedirs('data_processed/balance_sheet', exist_ok=True)
    link_dan =os.path.join('data_processed', 'balance_sheet', filename_processed) 
    df_grouped.to_excel(link_dan, index=False)




    
   


