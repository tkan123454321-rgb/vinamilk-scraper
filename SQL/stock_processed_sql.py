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
import traceback
from IPython.display import display
from openpyxl.utils import column_index_from_string
from vnstock import *
from datetime import datetime



# hàm tính toán các chỉ số tài chính và tạo file excel tổng hợp
def stock_ratio_calculation(co_phieu):
    try:
        #lấy dữ liệu báo cáo tài chính
        df_income = income_statement(co_phieu)
        df_balancesheet = balance_sheet(co_phieu)
        df_cashflow = cash_flow(co_phieu)
        df_gia = price_history(co_phieu)
        
        print("income:", type(df_income))
        print("balancesheet:", type(df_balancesheet))
        print("cashflow:", type(df_cashflow))
        print("gia:", type(df_gia))
        
        
        #tạo file excel tổng hợp
        today = datetime.now().strftime("%d-%m-%Y")
        if not os.path.exists('data_processed'):
            os.makedirs('data_processed')
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


def clean_data_sql(x):  #hàm biến đổi dữ liệu để phù hợp với sql (từ numpy sang kiểu dữ liệu bình thường của python)
    if isinstance(x, (np.floating, np.integer)):
        return x.item()
    if pd.isna(x):
        return None
    return x



# hàm lấy bảng cân đối kế toán
def balance_sheet(co_phieu):
    try: 
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

        df_raw = data
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
        df_grouped = df_grouped.map(clean_data_sql)
        #đổi tên để nhận diện tên cổ phiếu đang xem
        df_grouped = df_grouped.rename(columns={'group': f'{stock}, Đơn vị tính: đồng'})
        return df_grouped
    except Exception as e:
        print(f'Lỗi : {e}')




# hàm lấy bảng lưu chuyển tiền tệ
def cash_flow(co_phieu):
    try: 
            # lấy API ẩn từ trang web fireant.vn
        stock = co_phieu
        auth_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
        url = f'https://restv2.fireant.vn/symbols/{stock}/full-financial-reports?type=4&year=2025&quarter=0&limit=6'
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'


        params = {
                'type': '4',
                'year': '2025',
                'quarter': '0',
                'limit': '6'
        }

        headers = {
                'User-Agent': user_agent,
                'Authorization': auth_token
        }

        response = requests.get(url, headers=headers, params=params)
        response = response.json()  # đổi dữ liệu về dạng json để python đọc được (dạng dict hoặc list)

            # chuyển dữ liệu về dạng dataframe
        data = pd.DataFrame(response)
            # chuyển cột 'values' từ string về danh sách
        def convert_if_string(x):
            if isinstance(x,str):
                return ast.literal_eval(x)
            else:
                return x
        df_cf = data
            # Áp dụng hàm cho từng giá trị trong cột 'values'
        df_cf['values'] = df_cf['values'].apply(convert_if_string)
            # nổ cột 'values' thành nhiều hàng
        df_cf = df_cf.explode('values').reset_index(drop=True)
            # chuyển cột 'values' từ danh sách thành nhiều cột
        df_normalized = pd.json_normalize(df_cf['values'])
            #nối cột df_cf ban đầu với df_normalized
        df_cf = pd.concat([df_cf.drop(columns=[
            'values','id','parentID','expanded','level','field']), df_normalized], axis=1)
            # giữ lại các cột cần thiết
        df_cf = df_cf[['name','year','value']]
        df_cf = df_cf.pivot_table(index='name', columns='year', values='value',dropna = False).reset_index()

            # đổi tên 1 số hàng
        df_cf_rename = {' - Tăng, giảm hàng tồn kho': '- Tăng, giảm hàng tồn kho', ' - Tăng, giảm các khoản phải trả (Không kể lãi vay phải trả, thuế thu nhập doanh nghiệp phải nộp)': '- Tăng, giảm các khoản phải trả (Không kể lãi vay phải trả, thuế thu nhập doanh nghiệp phải nộp)'}
        df_cf['name'] = df_cf['name'].replace(df_cf_rename)
        cf_thutu = '''
            I. Lưu chuyển tiền từ hoạt động kinh doanh	
            1. Lợi nhuận trước thuế	
            2. Điều chỉnh cho các khoản	
            - Khấu hao TSCĐ	
            - Các khoản dự phòng	
            - Lợi nhuận thuần từ đầu tư vào công ty liên kết	
            - Xóa sổ tài sản cố định (thuần)	
            - Lãi, lỗ chênh lệch tỷ giá hối đoái chưa thực hiện	
            - Lãi, lỗ từ thanh lý TSCĐ	
            - Lãi, lỗ từ hoạt động đầu tư	
            - Lãi tiền gửi	
            - Thu nhập lãi	
            - Chi phí lãi vay	
            - Các khoản chi trực tiếp từ lợi nhuận	
            3. Lợi nhuận từ hoạt động kinh doanh trước thay đổi vốn lưu động	
            - Tăng, giảm các khoản phải thu
            - Tăng, giảm hàng tồn kho
            - Tăng, giảm các khoản phải trả (Không kể lãi vay phải trả, thuế thu nhập doanh nghiệp phải nộp)
            - Tăng giảm chi phí trả trước	
            - Tăng giảm tài sản ngắn hạn khác	
            - Tiền lãi vay phải trả	
            - Thuế thu nhập doanh nghiệp đã nộp	
            - Tiền thu khác từ hoạt động kinh doanh	
            - Tiền chi khác từ hoạt động kinh doanh	
            Lưu chuyển tiền thuần từ hoạt động kinh doanh	
            II. Lưu chuyển tiền từ hoạt động đầu tư	
            1. Tiền chi để mua sắm, xây dựng TSCĐ và các tài sản dài hạn khác	
            2. Tiền thu từ thanh lý, nhượng bán TSCĐ và các tài sản dài hạn khác	
            3. Tiền chi cho vay, mua các công cụ nợ của đơn vị khác	
            4. Tiền thu hồi cho vay, bán lại các công cụ nợ của các đơn vị khác	
            5. Đầu tư góp vốn vào công ty liên doanh liên kết	
            6. Chi đầu tư ngắn hạn	
            7. Tiền chi đầu tư góp vốn vào đơn vị khác	
            8. Tiền thu hồi đầu tư góp vốn vào đơn vị khác	
            9. Lãi tiền gửi đã thu	
            10. Tiền thu lãi cho vay, cổ tức và lợi nhuận được chia	
            11. Tiền chi mua lại phần vốn góp của các cổ đông thiểu số	
            Lưu chuyển tiền thuần từ hoạt động đầu tư	
            III. Lưu chuyển tiền từ hoạt động tài chính	
            1. Tiền thu từ phát hành cổ phiếu, nhận vốn góp của chủ sở hữu	
            2. Tiền chi trả vốn góp cho các chủ sở hữu, mua lại cổ phiếu của doanh nghiệp đã phát hành	
            3. Tiền vay ngắn hạn, dài hạn nhận được	
            4. Tiền chi trả nợ gốc vay	
            5. Tiền chi trả nợ thuê tài chính	
            6. Tiền chi khác từ hoạt động tài chính	
            7. Tiền chi trả từ cổ phần hóa	
            8. Cổ tức, lợi nhuận đã trả cho chủ sở hữu	
            9. Vốn góp của các cổ đông thiểu số vào các công ty con	
            10. Chi tiêu quỹ phúc lợi xã hội	
            Lưu chuyển tiền thuần từ hoạt động tài chính	
            Lưu chuyển tiền thuần trong kỳ	
            Tiền và tương đương tiền đầu kỳ	
            Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ	
            Tiền và tương đương tiền cuối kỳ
            '''
        cf_thutu = cf_thutu.strip().splitlines()
        cf_thutu_processed = [item.strip() for item in cf_thutu ]

        #sắp xếp lại index cho đúng
        df_cf = df_cf.set_index('name').reindex(cf_thutu_processed).reset_index()
        return df_cf
    except Exception as e:
        print(f"Lỗi khi lấy báo cáo lưu chuyển tiền tệ cho cổ phiếu {stock}: {e}, sẽ thử lại lần 2")
        traceback.print_exc()
        try:
            auth_token = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg'
            url = f'https://restv2.fireant.vn/symbols/{stock}/full-financial-reports?type=3&year=2025&quarter=0&limit=6'
            user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'


            params = {
                    'type': '4',
                    'year': '2025',
                    'quarter': '0',
                    'limit': '6'
            }

            headers = {
                    'User-Agent': user_agent,
                    'Authorization': auth_token
            }

            response = requests.get(url, headers=headers, params=params)
            response = response.json()  # đổi dữ liệu về dạng json để python đọc được (dạng dict hoặc list)

                # chuyển dữ liệu về dạng dataframe
            data = pd.DataFrame(response)
            def convert_if_string(x):
                if isinstance(x,str):
                    return ast.literal_eval(x)
                else:
                    return x
            df_cf = data
                # Áp dụng hàm cho từng giá trị trong cột 'values'
            df_cf['values'] = df_cf['values'].apply(convert_if_string)
                # nổ cột 'values' thành nhiều hàng
            df_cf = df_cf.explode('values').reset_index(drop=True)
                # chuyển cột 'values' từ danh sách thành nhiều cột
            df_normalized = pd.json_normalize(df_cf['values'])
                #nối cột df_cf ban đầu với df_normalized
            df_cf = pd.concat([df_cf.drop(columns=[
                'values','id','parentID','expanded','level','field']), df_normalized], axis=1)
            # giữ lại các cột cần thiết
            df_cf = df_cf[['name','year','value']]
            df_cf = df_cf.pivot_table(index='name', columns='year', values='value',dropna = False).reset_index()
            
            # strip hết khoảng trắng thừa trong cột name
            df_cf['name'] = df_cf['name'].str.strip()
            # sắp xếp thứ tự đúng
            df_cf_sort = '''
            I. Lưu chuyển tiền từ hoạt động kinh doanh	
            1. Tiền thu từ bán hàng, cung cấp dịch vụ và doanh thu khác	
            2. Tiền chi trả cho người cung cấp hàng hóa và dịch vụ	
            3. Tiền chi trả cho người lao động	
            4. Tiền chi trả lãi vay	
            5. Tiền chi nộp thuế thu nhập doanh nghiệp	
            6. Tiền chi nộp thuế giá trị gia tăng	
            7. Tiền thu khác từ hoạt động kinh doanh	
            8. Tiền chi khác cho hoạt động kinh doanh	
            Lưu chuyển tiền thuần từ hoạt động kinh doanh	
            II. Lưu chuyển tiền từ hoạt động đầu tư	
            1. Tiền chi để mua sắm, xây dựng TSCĐ và các tài sản dài hạn khác	
            2. Tiền thu từ thanh lý, nhượng bán TSCĐ và các tài sản dài hạn khác	
            3. Tiền chi cho vay, mua các công cụ nợ của đơn vị khác	
            4. Tiền thu hồi cho vay, bán lại các công cụ nợ của các đơn vị khác	
            5. Tiền chi đầu tư góp vốn vào đơn vị khác		
            6. Tiền thu hồi đầu tư góp vốn vào đơn vị khác	
            7. Tiền thu lãi cho vay, cổ tức và lợi nhuận được chia	
            Lưu chuyển tiền thuần từ hoạt động đầu tư	
            III. Lưu chuyển tiền từ hoạt động tài chính	
            1. Tiền thu từ phát hành cổ phiếu, nhận vốn góp của chủ sở hữu	
            2. Tiền chi trả vốn góp cho các chủ sở hữu, mua lại cổ phiếu của doanh nghiệp đã phát hành	
            3. Tiền vay ngắn hạn, dài hạn nhận được	
            4. Tiền chi trả nợ gốc vay	
            5. Tiền chi để mua sắm, xây dựng TSCĐ, BĐS đầu tư	
            6. Tiền chi trả nợ thuê tài chính	
            7. Cổ tức, lợi nhuận đã trả cho chủ sở hữu	
            8. Chi từ các quỹ của doanh nghiệp	
            Lưu chuyển tiền thuần từ hoạt động tài chính	
            Lưu chuyển tiền thuần trong kỳ	
            Tiền và tương đương tiền đầu kỳ	
            Ảnh hưởng của thay đổi tỷ giá hối đoái quy đổi ngoại tệ	
            Tiền và tương đương tiền cuối kỳ
            '''
            df_cf_sort = df_cf_sort.strip().splitlines()
            df_cf_sort_processed = [item.strip() for item in df_cf_sort ]
            df_cf = df_cf.set_index('name').reindex(df_cf_sort_processed).reset_index()
            print("Lấy báo cáo lưu chuyển tiền tệ lần 2 thành công")
            return df_cf
        except Exception as e2:
            print(f"Lỗi khi lấy báo cáo lưu chuyển tiền tệ lần 2 cho cổ phiếu {stock}: {e2}")
            traceback.print_exc()

# hàm lấy bảng báo cáo kết quả kinh doanh
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


def ratio_calculation(co_phieu):
    try:
        finace_data = Finance(symbol=co_phieu, source="VCI")
        ratio = finace_data.ratio(period = 'year', lang='vi')  # lấy dữ liệu các tỉ số tài chính

        today = datetime.now().strftime("%d-%m-%y")
        if not os.path.exists('data_raw/ratio_raw'):  # tạo thư mục
            os.makedirs('data_raw/ratio_raw', exist_ok=True)
        filename_raw = f'{co_phieu}_stock_ratio_{today}.xlsx'



        #lưu file raw
        ratio.to_excel(f'data_raw/ratio_raw/{filename_raw}', index= True)
        filename_raw_file = f'data_raw/ratio_raw/{filename_raw}'
    except Exception as e:
        print("Lỗi khi lấy dữ Bliệu tỉ số tài chính:", e)
        traceback.print_exc()
    
    try:
        
        df_ratio = pd.read_excel(filename_raw_file, header=1)  # Đọc file với MultiIndex cho cột
        #dọn khoảng trắng thừa
        df_ratio.columns = df_ratio.columns.str.strip()
        # lập danh sách cột cần giữ:
        column_keep = ['Năm']
        #lập danh sách cột bỏ
        columns_drop = ['Unnamed: 0','CP', 'Kỳ']
        #các cột chỉ số:
        ratio_col = [col for col in df_ratio.columns if col not in column_keep and col not in columns_drop]
        df_ratio = df_ratio.melt(id_vars=column_keep, value_vars=ratio_col, var_name='Chỉ số', value_name='Giá trị')

        #pivot lại
        df_ratio = df_ratio.pivot_table(index=column_keep, columns='Chỉ số', values='Giá trị').reset_index()

        #transpose lại
        df_ratio = df_ratio.set_index('Năm').transpose().reset_index()

        #bỏ các chỉ số rác
        drop_ratio = '''
        Chu kỳ tiền
        Chỉ số thanh toán tiền mặt
        EBIT (Tỷ đồng)
        EBITDA (Tỷ đồng)
        Nợ/VCSH
        P/Cash Flow
        Số CP lưu hành (Triệu CP)
        TSCĐ / Vốn CSH
        Tỷ suất cổ tức (%)
        Vòng quay TSCĐ
        Vốn CSH/Vốn điều lệ
        Vốn hóa (Tỷ đồng)
        Đòn bẩy tài chính
        P/B
        P/E
        P/S
        EPS (VND)
        BVPS (VND)
        Số ngày thanh toán bình quân
        Khả năng chi trả lãi vay
        Chỉ số thanh toán nhanh
        ROIC (%)
        Biên EBIT (%)
        ROA (%)
        ROE (%)
        '''
        drop_ratio_list = [item.strip() for item in drop_ratio.strip().splitlines()]
        df_ratio = df_ratio[~df_ratio['Chỉ số'].isin(drop_ratio_list)]
        
        #đổi tên các chỉ số cho dễ hiểu hơn
        rename_dict = {
            '(Vay NH+DH)/VCSH': 'Tỷ số Nợ vay trên Vốn chủ sở hữu (%)',
            'Vòng quay tài sản': 'Vòng quay tổng tài sản',
            'Số ngày tồn kho bình quân':'Thời gian tồn kho bình quân (ngày)',
            'Số ngày thu tiền bình quân': 'Thời gian thu tiền khách hàng bình quân (ngày)',
            'Chỉ số thanh toán hiện thời': 'Tỷ số thanh toán hiện hành (ngắn hạn)',
        }

        df_ratio['Chỉ số'] = df_ratio['Chỉ số'].replace(rename_dict)

        # đổi tên cột năm thành string các số nguyên
        columns = []
        for c in df_ratio.columns:
            if isinstance(c, float):
                columns.append(str(int(c)).strip())
            else:
                columns.append(str(c).strip())
        df_ratio.columns = columns

        #xoá các cột ko cần thiết
        columns_drop = [str(c) for c in range(2013, 2019)]
        to_drop = []
        for c in df_ratio.columns:
            if str(c).strip() in columns_drop:
                to_drop.append(c)
        df_ratio = df_ratio.drop(columns=to_drop, errors='ignore',axis = 1)


        #lưu vào filename processed để xử lý tiếp
        os.makedirs('data_processed/ratio_processed', exist_ok=True)
        filename_processed = f'{co_phieu}_stock_ratio_processed_{today}.xlsx'
        filename_processed_file = f'data_processed/ratio_processed/{filename_processed}'
        df_ratio.to_excel(filename_processed_file, index=False)

        #đọc dữ liệu file mới
        df_ratio = pd.read_excel(filename_processed_file, index_col=0)
        # nhập dữ liệu từ báo cáo tài chính
        filename_processed = stock_ratio_calculation(co_phieu)
        df_kqkd = pd.read_excel(filename_processed, sheet_name='Báo cáo kết quả kinh doanh', index_col=0)
        df_lctt = pd.read_excel(filename_processed, sheet_name='Báo cáo lưu chuyển tiền tệ', index_col=0)
        df_balance =pd.read_excel(filename_processed, sheet_name='Bảng cân đối kế toán', index_col=0)
        df_price = pd.read_excel(filename_processed, sheet_name='Giá lịch sử', index_col=0)

        #xoá khoảng trắng các tên cột, index
        df_price = df_price.reset_index()
        df_kqkd.columns = df_kqkd.columns.astype(str).str.strip()
        df_balance.columns = df_balance.columns.astype(str).str.strip()
        df_price.columns = df_price.columns.astype(str).str.strip()
        df_ratio.index = df_ratio.index.str.strip()
        df_balance.index = df_balance.index.str.strip()
        df_kqkd.index = df_kqkd.index.astype(str).str.strip()
        df_lctt.index = df_lctt.index.astype(str).str.strip()
        df_lctt.columns = df_lctt.columns.astype(str).str.strip()

        #________________________tính toán các chỉ số còn thiếu________________________
        thu_tu_dung = [
            "Nhóm chỉ số Định giá",
            "EPS",
            "BVPS",
            "P/E",
            "P/B",
            "P/S",
            "EV/EBITDA",
            "Nhóm chỉ số Sinh lợi",
            "Biên EBIT (%)",
            "Biên lợi nhuận gộp (%)",
            "Biên lợi nhuận ròng (%)",
            "Tỷ suất sinh lợi trên tổng tài sản bình quân (ROAA) (%)",
            "Tỷ suất sinh lợi trên vốn chủ sở hữu bình quân (ROEA) (%)",
            "Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE) (%)",
            "Nhóm chỉ số Tăng trưởng",
            "Tăng trưởng doanh thu thuần (%)",
            "Tăng trưởng lợi nhuận sau thuế (%)",
            "Nhóm chỉ số Thanh khoản",
            "Tỷ số thanh toán hiện hành (ngắn hạn)",
            "Chỉ số thanh toán nhanh",
            "Khả năng chi trả lãi vay",
            "Nhóm chỉ số Hiệu quả hoạt động",
            "Thời gian thu tiền khách hàng bình quân (ngày)",
            "Thời gian tồn kho bình quân (ngày)",
            "Vòng quay hàng tồn kho",
            "Vòng quay tổng tài sản",
            "Số ngày trả cho nhà cung cấp (ngày)",
            "Vòng quay trả cho nhà cung cấp",
            "Chu kỳ chuyển đổi tiền mặt (CCC) (ngày)",
            "Nhóm chỉ số Đòn bẩy tài chính",
            "Tỷ số Nợ vay trên Vốn chủ sở hữu (%)",
            "Tỷ số Nợ vay trên Tổng tài sản (%)",
            "Nhóm chỉ số Dòng tiền",
            "Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)",
            "Dòng tiền từ HĐKD trên Tổng tài sản (%)",
            "Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)",
            "Dòng tiền từ HĐKD trên mỗi cổ phần"
        ]
        

        #tính EPS
        von_dieu_le = df_balance.loc['1. Vốn đầu tư của chủ sở hữu']
        so_co_phieu = von_dieu_le / 10000 # giả sử mệnh giá 10k/cp
        loi_nhuan_rong_nam = df_kqkd.loc['Lợi nhuận sau thuế của cổ đông của Công ty mẹ']
        eps = (loi_nhuan_rong_nam / so_co_phieu).round(2)
        df_ratio.loc['EPS']= eps

        #tính P/E
        df_price['time'] = pd.to_datetime(df_price['time'], format = '%d-%m-%Y')
        df_price = df_price.set_index('time')
        gia_cuoi_nam_series = df_price['close'].resample('Y').last() #giá cuối năm
        gia_cuoi_nam_series= gia_cuoi_nam_series.iloc[:-1] #bỏ năm hiện tại chưa kết thúc
        #set index là năm, đổi index thành str
        gia_cuoi_nam_series.index = gia_cuoi_nam_series.index.year.astype(str)
        # nhân với 1,000 
        gia_cuoi_nam = gia_cuoi_nam_series * 1000
        von_hoa = gia_cuoi_nam * so_co_phieu
        pe_ratio = (von_hoa/loi_nhuan_rong_nam).round(2)
        df_ratio.loc['P/E'] = pe_ratio

        #tính BVPS
        loi_ich_co_dong_thieu_so = df_balance.loc['14. Lợi ích của cổ đông không kiểm soát']
        von_chu_so_huu = df_balance.loc['I. Vốn chủ sở hữu']
        bvps = (von_chu_so_huu  / so_co_phieu).round(2)
        df_ratio.loc['BVPS'] = bvps
        #tính P/B
        pb_ratio = (von_hoa / von_chu_so_huu).round(2)
        df_ratio.loc['P/B'] = pb_ratio

        #tính P/S
        doanh_thu_thuan_nam = df_kqkd.loc['Doanh thu thuần về bán hàng và cung cấp dịch vụ']
        ps_ratio = (von_hoa/ doanh_thu_thuan_nam).round(2)
        df_ratio.loc['P/S'] = ps_ratio


        #tăng trưởng doanh thu thuần và lợi nhuận sau thuế CĐ công ty mẹ
        doanh_thu_nam_series = df_kqkd.loc['Doanh thu thuần về bán hàng và cung cấp dịch vụ']
        growth_revenue_series = doanh_thu_nam_series.pct_change(periods=1).fillna(0)

        loi_nhuan_me_series = df_kqkd.loc['Lợi nhuận sau thuế của cổ đông của Công ty mẹ']
        growth_profit_series = loi_nhuan_me_series.pct_change(periods=1).fillna(0)


        df_ratio.loc['Tăng trưởng doanh thu thuần (%)'] = growth_revenue_series
        df_ratio.loc['Tăng trưởng lợi nhuận sau thuế (%)'] = growth_profit_series

        # vòng quay trả cho nhà cung cấp và thời gian trả cho nhà cung cấp
        cogs = df_kqkd.loc['Giá vốn hàng bán']  
        payable_begin = df_balance.loc['3. Phải trả người bán ngắn hạn'].shift(1)
        payable_end = df_balance.loc['3. Phải trả người bán ngắn hạn']
        average_payable = (payable_begin + payable_end) / 2
        vong_quay_tra_cho_nha_cung_cap = (cogs / average_payable) * -1
        so_ngay_tra_cho_nha_cung_cap = (365 / vong_quay_tra_cho_nha_cung_cap).round(2)
        df_ratio.loc['Số ngày trả cho nhà cung cấp (ngày)'] = so_ngay_tra_cho_nha_cung_cap.fillna(0)
        df_ratio.loc['Vòng quay trả cho nhà cung cấp'] = vong_quay_tra_cho_nha_cung_cap.round(2).fillna(0)

        #tính chu kỳ chuyển đổi tiền mặt
        if 'Thời gian tồn kho bình quân (ngày)' in df_ratio.index:
            so_ngay_ton_kho = df_ratio.loc['Thời gian tồn kho bình quân (ngày)']
            so_ngay_thu_tien_khach_hang = df_ratio.loc['Thời gian thu tiền khách hàng bình quân (ngày)']
            CCC = (so_ngay_ton_kho + so_ngay_thu_tien_khach_hang - so_ngay_tra_cho_nha_cung_cap).fillna(0).round(0)
            df_ratio.loc['Chu kỳ chuyển đổi tiền mặt (CCC) (ngày)'] = CCC

        #tính khả năng chi trả lãi vay
        loi_nhuan_truoc_thue = df_kqkd.loc['Tổng lợi nhuận kế toán trước thuế']
        chi_phi_lai = df_kqkd.loc['Trong đó: chi phí tiền lãi vay'] * -1
        ebit = loi_nhuan_truoc_thue + chi_phi_lai
        khả_năng_chi_tra_lai_vay = (ebit / chi_phi_lai).round(2)
        df_ratio.loc['Khả năng chi trả lãi vay'] = khả_năng_chi_tra_lai_vay

        # chỉ số thanh toán nhanh
        ts_ngan_han = df_balance.loc['A. Tài sản lưu động và đầu tư ngắn hạn']
        hang_ton_kho = df_balance.loc['IV. Tổng hàng tồn kho']
        ts_ngan_han_tru_hang_ton_kho = ts_ngan_han - hang_ton_kho
        no_ngan_han = df_balance.loc['I. Nợ ngắn hạn']
        chi_so_thanh_toan_nhanh = (ts_ngan_han_tru_hang_ton_kho / no_ngan_han).round(2)
        df_ratio.loc['Chỉ số thanh toán nhanh'] = chi_so_thanh_toan_nhanh

        # tỷ số nợ vay trên tổng tài sản
        tong_tai_san = df_balance.loc['TỔNG CỘNG TÀI SẢN']
        nợ_vay_ngắn_hạn = df_balance.loc['1. Vay và nợ thuê tài chính ngắn hạn'] + df_balance.loc['2. Vay và nợ dài hạn đến hạn phải trả'] 
        nợ_vay_dài_hạn = df_balance.loc['6. Vay và nợ thuê tài chính dài hạn'] + df_balance.loc['7. Trái phiếu chuyển đổi']
        tong_no_vay = nợ_vay_ngắn_hạn + nợ_vay_dài_hạn
        ty_so_no_vay_tren_tong_tai_san = ((tong_no_vay / tong_tai_san)*100).round(2)
        df_ratio.loc['Tỷ số Nợ vay trên Tổng tài sản (%)'] = ty_so_no_vay_tren_tong_tai_san

        # CF margin 
        cfo_series = df_lctt.loc['Lưu chuyển tiền thuần từ hoạt động kinh doanh']
        cf_margin = (cfo_series / doanh_thu_thuan_nam).round(4)
        df_ratio.loc['Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)'] = cf_margin

        # Dòng tiền từ HĐKD trên Tổng tài sản (CFROA)
        cfroa = (cfo_series / tong_tai_san).round(4)
        df_ratio.loc['Dòng tiền từ HĐKD trên Tổng tài sản (%)'] = cfroa

        # Dòng tiền từ HĐKD trên Vốn chủ sở hữu (CFROE)
        cfroe = (cfo_series / von_chu_so_huu).round(4)
        df_ratio.loc['Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)'] = cfroe

        # Dòng tiền từ HĐKD trên mỗi cổ phần (CPS)
        cps = (cfo_series / so_co_phieu).round(2)
        df_ratio.loc['Dòng tiền từ HĐKD trên mỗi cổ phần'] = cps

        #biên EBIT
        biên_ebit = ((ebit / doanh_thu_thuan_nam) * 100).round(2) 
        df_ratio.loc['Biên EBIT (%)'] = biên_ebit

        #Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE)
        asset_end = df_balance.loc['TỔNG CỘNG TÀI SẢN']
        asset_begin = asset_end.shift(1)
        asset_avg = (asset_begin + asset_end) / 2
        nợ_ngắn_hạn_end = df_balance.loc['I. Nợ ngắn hạn']
        nợ_ngắn_hạn_begin = nợ_ngắn_hạn_end.shift(1)
        nợ_ngắn_hạn_avg = (nợ_ngắn_hạn_begin + nợ_ngắn_hạn_end) / 2
        vốn_dài_hạn_avg = asset_avg - nợ_ngắn_hạn_avg
        roce = (ebit / vốn_dài_hạn_avg).round(4).fillna(0) * 100
        df_ratio.loc['Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE) (%)'] = roce

        #tỷ suất sinh lợi trên tổng tài sản bình quân (ROAA)
        lnst = df_kqkd.loc['Lợi nhuận sau thuế thu nhập doanh nghiệp']
        roaa = (lnst / asset_avg).round(4).fillna(0) * 100
        df_ratio.loc['Tỷ suất sinh lợi trên tổng tài sản bình quân (ROAA) (%)'] = roaa

        #tỷ suất sinh lợi trên vốn chủ sở hữu bình quân (ROEA)
        vốn_chủ_sở_hữu_end = df_balance.loc['B. Nguồn vốn chủ sở hữu']
        vốn_chủ_sở_hữu_begin = vốn_chủ_sở_hữu_end.shift(1)
        vốn_chủ_sở_hữu_avg = (vốn_chủ_sở_hữu_begin + vốn_chủ_sở_hữu_end) / 2
        roe = (loi_nhuan_me_series / vốn_chủ_sở_hữu_avg).round(4).fillna(0) * 100
        df_ratio.loc['Tỷ suất sinh lợi trên vốn chủ sở hữu bình quân (ROEA) (%)'] = roe

        # sửa lỗi thiếu index 1 số cổ phiếu (update)
        for index in thu_tu_dung:
            if not index in df_ratio.index:
                df_ratio.loc[index] = None
            
        #làm tròn các chỉ số đến hàng thập phân thứ 2
        row_round_2 = [
            'EV/EBITDA',
            'Biên lợi nhuận gộp (%)',
            'Biên lợi nhuận ròng (%)',
            'Tăng trưởng doanh thu thuần (%)',
            'Tăng trưởng lợi nhuận sau thuế (%)',
            'Tỷ số thanh toán hiện hành (ngắn hạn)',
            'Vòng quay hàng tồn kho',
            'Vòng quay tổng tài sản',
            'Thời gian thu tiền khách hàng bình quân (ngày)',
            'Thời gian tồn kho bình quân (ngày)',
            'Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)',
            'Dòng tiền từ HĐKD trên Tổng tài sản (%)',
            'Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)',
            'Tỷ số Nợ vay trên Vốn chủ sở hữu (%)'
            
        ]
        # các hàng cần phải nhân thêm cả 100
        round_100 = ['Biên lợi nhuận gộp (%)',
            'Biên lợi nhuận ròng (%)',
            'Tăng trưởng doanh thu thuần (%)',
            'Tăng trưởng lợi nhuận sau thuế (%)',
            'Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)',
            'Dòng tiền từ HĐKD trên Tổng tài sản (%)',
            'Dòng tiền từ HĐKD trên Vốn chủ sở hữu (%)',
            'Tỷ số Nợ vay trên Vốn chủ sở hữu (%)'
            ]
            
        for row in row_round_2:
            if df_ratio.loc[row].isnull().all():
                continue
            if row in round_100:
                df_ratio.loc[row] = (df_ratio.loc[row] * 100).round(2)
            else:
                df_ratio.loc[row] = df_ratio.loc[row].round(2)

        # thêm tên định danh các nhóm chỉ số
        nhom_chi_so = [
            "Nhóm chỉ số Định giá",
            "Nhóm chỉ số Sinh lợi",
            "Nhóm chỉ số Tăng trưởng",
            "Nhóm chỉ số Thanh khoản",
            "Nhóm chỉ số Hiệu quả hoạt động",
            "Nhóm chỉ số Đòn bẩy tài chính",
            "Nhóm chỉ số Dòng tiền"
        ]

        
        for group in nhom_chi_so:
            df_ratio.loc[group] = np.nan
            
        df_ratio = df_ratio.reindex(thu_tu_dung).reset_index()
        
        return df_ratio
    except Exception as e2:
        print(f"Lỗi trong quá trình tính toán tỷ số cho {co_phieu}: {e2}")
        traceback.print_exc()

# Hàm lấy lịch sử giá
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
    

