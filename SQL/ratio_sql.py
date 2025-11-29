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
from stock_processed_sql import stock_ratio_calculation
import numpy as np

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
        
        # sửa lỗi 
        if not 'Tỷ số Nợ vay trên Vốn chủ sở hữu (%)' in df_ratio.index:
            df_ratio.loc['Tỷ số Nợ vay trên Vốn chủ sở hữu (%)'] = None
        if not 'Thời gian tồn kho bình quân (ngày)' in df_ratio.index:
            df_ratio.loc['Thời gian tồn kho bình quân (ngày)'] = None
        if not 'Vòng quay hàng tồn kho' in df_ratio.index:
            df_ratio.loc['Vòng quay hàng tồn kho'] = None

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
        for group in nhom_chi_so:
            df_ratio.loc[group] = np.nan
            
        df_ratio = df_ratio.reindex(thu_tu_dung).reset_index()
        
        return df_ratio
    except Exception as e2:
        print(f"Lỗi trong quá trình tính toán tỷ số cho {ticker}: {e2}")
        traceback.print_exc()

if __name__ == "__main__":
    #ví dụ chạy hàm
    ticker = "HEV"
    df_ratio = ratio_calculation(ticker)

    
    