import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

def scrape_vnm_news_cafef():
    """
    Scrape tin tức VNM từ CafeF
    """
    print("🚀 Bắt đầu scraping tin tức VNM từ CafeF...")
    
    # URL CafeF tìm kiếm VNM
    url = "https://s.cafef.vn/tim-kiem.chn?keywords=VNM"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Gửi request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Kiểm tra status code
        
        print(f"✅ Kết nối thành công! Status: {response.status_code}")
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Tìm các bài viết (cần inspect HTML để tìm selector chính xác)
        articles = soup.find_all('div', class_='tlitem')  # Có thể cần điều chỉnh
        
        news_data = []
        
        for article in articles[:10]:  # Lấy 10 bài đầu
            try:
                # Trích xuất tiêu đề
                title_element = article.find('a', class_='tlitem-title')
                title = title_element.text.strip() if title_element else "N/A"
                
                # Trích xuất link
                link = title_element.get('href') if title_element else "N/A"
                if link and not link.startswith('http'):
                    link = "https://s.cafef.vn" + link
                
                # Trích xuất ngày đăng
                date_element = article.find('span', class_='tlitem-time')
                date = date_element.text.strip() if date_element else "N/A"
                
                # Trích xuất nội dung tóm tắt
                summary_element = article.find('div', class_='tlitem-sapo')
                summary = summary_element.text.strip() if summary_element else "N/A"
                
                news_data.append({
                    'Tiêu đề': title,
                    'Ngày đăng': date,
                    'Nội dung tóm tắt': summary,
                    'Link': link
                })
                
            except Exception as e:
                print(f"⚠️ Lỗi khi xử lý bài viết: {e}")
                continue
        
        print(f"📊 Đã scrape được {len(news_data)} bài viết")
        return news_data
        
    except Exception as e:
        print(f"❌ Lỗi khi scraping: {e}")
        return []

def save_to_dataframe_and_excel(news_data):
    """
    Lưu dữ liệu vào DataFrame và Excel
    """
    if not news_data:
        print("❌ Không có dữ liệu để lưu")
        return
    
    # Tạo DataFrame
    df = pd.DataFrame(news_data)
    
    print("\n📋 DỮ LIỆU SCRAPED:")
    print("=" * 80)
    for i, row in df.iterrows():
        print(f"\n{i+1}. {row['Tiêu đề']}")
        print(f"   📅 Ngày: {row['Ngày đăng']}")
        print(f"   📝 Tóm tắt: {row['Nội dung tóm tắt'][:100]}...")
        print(f"   �� Link: {row['Link']}")
    
    # Lưu vào Excel
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"vnm_news_cafef_{timestamp}.xlsx"
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Sheet 1: Dữ liệu gốc
        df.to_excel(writer, sheet_name='Tin_tuc_VNM', index=False)
        
        # Sheet 2: Thống kê
        stats_data = {
            'Metric': ['Tổng số bài viết', 'Ngày scrape', 'Nguồn'],
            'Value': [len(df), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'CafeF']
        }
        stats_df = pd.DataFrame(stats_data)
        stats_df.to_excel(writer, sheet_name='Thong_ke', index=False)
    
    print(f"\n�� Đã lưu dữ liệu vào: {filename}")
    
    # Thống kê cơ bản
    print(f"\n�� THỐNG KÊ:")
    print(f"   �� Tổng số bài viết: {len(df)}")
    print(f"   📅 Ngày scrape: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   �� Nguồn: CafeF")
    
    return df

def main():
    """
    Hàm chính
    """
    print("🎯 MINI PROJECT: SCRAPING TIN TỨC VNM TỪ CAFEF")
    print("=" * 60)
    
    # Scrape dữ liệu
    news_data = scrape_vnm_news_cafef()
    
    # Lưu dữ liệu
    if news_data:
        df = save_to_dataframe_and_excel(news_data)
        print("\n✅ Project hoàn thành thành công!")
    else:
        print("\n❌ Không thể scrape được dữ liệu")

if __name__ == "__main__":
    main()
