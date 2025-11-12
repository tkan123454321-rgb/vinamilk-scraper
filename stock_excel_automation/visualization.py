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
from openpyxl.utils import column_index_from_string
from stock_processed import stock_ratio_calculation
from ratio import ratio_calculation
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from openpyxl.drawing.image import Image

def plot_stock_prices(co_phieu):
    df_ratio, filename = ratio_calculation(co_phieu)
    df_visualization = pd.read_excel(filename, index_col=0)
    # vẽ biểu đồ P/E
    pe_to_plot = df_visualization.loc['P/E']
    plt.figure(figsize=(5, 3))
    sns.set_style("white")
    sns.lineplot(
        data=pe_to_plot, 
        marker='o',       
        label='P/E',
        color='black'
    )
    plt.title('Định giá [năm]', fontsize=13, fontweight='bold')
    plt.xlabel('Năm', fontsize=12, fontweight='bold')
    plt.ylabel('P/E', fontsize=12, fontweight='bold')
    if any(pe_to_plot < 0):
        plt.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)

    # lưu ảnh
    plt.savefig('pe_ratio_plot.png', dpi=300, bbox_inches='tight')
    plt.close()
    plt.show()
    # __________vẽ biều đồ Tăng trưởng Doanh thu (%) vs. Tăng trưởng Lợi nhuận (%)__________
    plt.figure(figsize=(5, 3))
    sns.set_style("white")
    growth_revenue = df_visualization.loc['Tăng trưởng doanh thu thuần (%)']
    growth_profit = df_visualization.loc['Tăng trưởng lợi nhuận sau thuế (%)']
    data_to_plot = pd.DataFrame({
        'Tăng trưởng Doanh thu': growth_revenue,
        'Tăng trưởng Lợi nhuận': growth_profit
    }) # gộp vào thành 1 dataframe duy nhát để tạo biểu đồ
    data_to_plot = data_to_plot.drop(index = '2019', errors = 'ignore') # bỏ index 2019 vì nó hiện số 0
    sns.lineplot(
        data = data_to_plot,
        marker = 'o',
        palette = ["#c60606", "#000000"],
        dashes = False
    )
    if any(growth_revenue < 0) or any(growth_profit < 0):
        plt.axhline(0, color='gray', linestyle='--', linewidth=0.5)
    plt.title('Tăng trưởng Doanh thu và Lợi nhuận [năm]', fontsize=13, fontweight='bold')
    plt.xlabel('Năm', fontsize=12, fontweight='bold')
    plt.ylabel('Tỷ lệ (%)', fontsize=12, fontweight='bold')
    plt.legend()
    plt.savefig('growth_revenue_profit_plot.png', dpi=300, bbox_inches='tight')
    plt.close()
    plt.show()

    # ________vẽ biểu đồ ROE vs. ROA, ROCE__________
    sns.set_style("white")

    # gọi dữ liệu
    roaa = df_visualization.loc['Tỷ suất sinh lợi trên tổng tài sản bình quân (ROAA) (%)'].drop(index = '2019', errors = 'ignore')
    roea = df_visualization.loc['Tỷ suất sinh lợi trên vốn chủ sở hữu bình quân (ROEA) (%)'].drop(index = '2019', errors = 'ignore')
    roce = df_visualization.loc['Tỷ suất sinh lợi trên vốn dài hạn bình quân (ROCE) (%)'].drop(index = '2019', errors = 'ignore')

    # tạo cái khung và tạo tấm canva để vẽ
    fig, ax = plt.subplots(figsize=(5, 3))

    # vẽ các đường biểu diễn
    sns.lineplot(x=roaa.index, y=roaa.values, marker='o', color="#cfbd14", label='ROAA (%)', ax=ax)
    sns.lineplot(x=roea.index, y=roea.values, marker='o', color="#000000", label='ROEA (%)', ax=ax)
    sns.lineplot(x=roce.index, y=roce.values, marker='o', color="#cf371f", label='ROCE (%)', ax=ax)

    # thêm tiêu đề và nhãn
    if any(roaa < 0) or any(roea < 0) or any(roce < 0):
        ax.axhline(0, color='gray', linestyle='--', linewidth=0.5)
    ax.set_title('Hiệu quả sử dụng vốn [năm]', fontsize=13, fontweight='bold')
    ax.set_xlabel('Năm', fontsize=12, fontweight='bold')
    ax.set_ylabel('Tỷ lệ (%)', fontsize=12, fontweight='bold')
    ax.legend()
    plt.savefig('roe_roa_roce_plot.png', dpi=300, bbox_inches='tight')
    plt.close()
    plt.show()


    #________vẽ biểu đồ biên lợi nhuận ròng, dòng tiền từ HĐKD__________
    fig, ax = plt.subplots(figsize=(5, 3))
    sns.set_style("white")
    net_profit_margin = df_visualization.loc['Biên lợi nhuận ròng (%)']
    cf_margin = df_visualization.loc['Tỷ số dòng tiền HĐKD trên doanh thu thuần (%)']

    sns.lineplot(x=net_profit_margin.index, y=net_profit_margin.values, marker='o', color="#EE9F18", label='Biên lợi nhuận ròng (%)', ax=ax)
    sns.lineplot(x=cf_margin.index, y=cf_margin.values, marker='o', color="#14acf2", label='Dòng tiền từ HĐKD/ doanh thu thuần (%)' , ax=ax)
    ax.axhline(0, color='gray', linestyle='--', linewidth=0.5)
    ax.set_title('Lợi nhuận ròng và Dòng tiền từ HĐKD [năm]', fontsize=13, fontweight='bold')
    ax.set_xlabel('Năm', fontsize=12, fontweight='bold')
    ax.set_ylabel('Tỷ lệ / doanh thu thuần (%)', fontsize=12, fontweight='bold')
    ax.legend(loc ='upper left', fontsize=7)
    plt.savefig('net_profit_cf_margin_plot.png', dpi=300, bbox_inches='tight')
    plt.close()
    plt.show()

    # tạo object ảnh
    img_pe = Image('pe_ratio_plot.png')
    img_growth = Image('growth_revenue_profit_plot.png')
    img_roe = Image('roe_roa_roce_plot.png')
    img_netprofit_cf = Image('net_profit_cf_margin_plot.png')
    
    
    return img_pe, img_growth, img_roe, img_netprofit_cf
