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
from datetime import *
import traceback
from IPython.display import display
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
    
if __name__ == "__main__":
   display(cash_flow('QNP'))