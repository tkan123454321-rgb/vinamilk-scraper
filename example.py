# SCRAPE DỮ LIỆU VINAMILK TỪ YAHOO FINANCE
# Mini Project: Scrape Vinamilk Data from Yahoo Finance
# import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import json

print("🚀 Starting Vinamilk Data Scraping Project")
print("=" * 50)


def scrape_vinamilk_basic_info():
    """
    Scrape thông tin cơ bản của Vinamilk từ Yahoo Finance
    """
    url = "https://finance.yahoo.com/quote/VNM.VN"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Lấy tên công ty
        company_name = soup.find('h1', {'data-testid': 'company-name'})
        if company_name:
            print(f"�� Company: {company_name.text}")

        # Lấy giá hiện tại
        price_element = soup.find(
            'fin-streamer', {'data-field': 'regularMarketPrice'})
        if price_element:
            current_price = price_element.get('value')
            print(f"💰 Current Price: {current_price} VND")

        # Lấy thay đổi giá
        change_element = soup.find(
            'fin-streamer', {'data-field': 'regularMarketChange'})
        if change_element:
            change = change_element.get('value')
            print(f"📈 Change: {change} VND")

        # Lấy % thay đổi
        change_percent_element = soup.find(
            'fin-streamer', {'data-field': 'regularMarketChangePercent'})
        if change_percent_element:
            change_percent = change_percent_element.get('value')
            print(f"📊 Change %: {change_percent}%")

        return {
            'company': company_name.text if company_name else 'N/A',
            'price': current_price,
            'change': change,
            'change_percent': change_percent
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        return None


# Chạy function
basic_info = scrape_vinamilk_basic_info()

# Bước 4: Scrape dữ liệu lịch sử giá


def scrape_vinamilk_historical_data(days=30):
    """
    Scrape dữ liệu lịch sử giá Vinamilk
    """
    # Tính toán timestamp
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/VNM.VN"

    params = {
        'period1': start_timestamp,
        'period2': end_timestamp,
        'interval': '1d',
        'includePrePost': 'true',
        'events': 'div%2Csplit'
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        data = response.json()

        # Trích xuất dữ liệu
        chart_data = data['chart']['result'][0]
        timestamps = chart_data['timestamp']
        quotes = chart_data['indicators']['quote'][0]

        # Tạo DataFrame
        df_data = []
        for i, timestamp in enumerate(timestamps):
            date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            df_data.append({
                'Date': date,
                'Open': quotes['open'][i],
                'High': quotes['high'][i],
                'Low': quotes['low'][i],
                'Close': quotes['close'][i],
                'Volume': quotes['volume'][i]
            })

        df = pd.DataFrame(df_data)
        df = df.dropna()  # Loại bỏ dữ liệu null

        print(f"�� Scraped {len(df)} days of historical data")
        print(f"📊 Date range: {df['Date'].min()} to {df['Date'].max()}")

        return df

    except Exception as e:
        print(f"❌ Error scraping historical data: {e}")
        return None


# Scrape dữ liệu 30 ngày qua
historical_data = scrape_vinamilk_historical_data(30)


# Bước 5: Hiển thị và phân tích dữ liệu
if historical_data is not None:
    print("\n�� VINAMILK HISTORICAL DATA (Last 30 days)")
    print("=" * 60)

    # Hiển thị 5 ngày gần nhất
    print("\n🔍 Recent 5 days:")
    print(historical_data.tail().to_string(index=False))

    # Thống kê cơ bản
    print(f"\n📈 Price Statistics:")
    print(f"   Highest Price: {historical_data['High'].max():,.0f} VND")
    print(f"   Lowest Price: {historical_data['Low'].min():,.0f} VND")
    print(f"   Average Price: {historical_data['Close'].mean():,.0f} VND")
    print(f"   Current Price: {historical_data['Close'].iloc[-1]:,.0f} VND")

    # Tính toán thay đổi
    price_change = historical_data['Close'].iloc[-1] - \
        historical_data['Close'].iloc[0]
    price_change_percent = (
        price_change / historical_data['Close'].iloc[0]) * 100

    print(f"\n📊 30-day Performance:")
    print(f"   Price Change: {price_change:+,.0f} VND")
    print(f"   Percentage Change: {price_change_percent:+.2f}%")

    # Volume analysis
    avg_volume = historical_data['Volume'].mean()
    print(f"\n�� Volume Analysis:")
    print(f"   Average Daily Volume: {avg_volume:,.0f}")
    print(f"   Highest Volume: {historical_data['Volume'].max():,.0f}")

    # Lưu dữ liệu vào Excel
if historical_data is not None:
    filename = f"vinamilk_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Sheet 1: Historical Data
        historical_data.to_excel(
            writer, sheet_name='Historical_Data', index=False)

        # Sheet 2: Summary Statistics
        summary_data = {
            'Metric': ['Current Price', '30-day High', '30-day Low', 'Average Price',
                       'Price Change', 'Percentage Change', 'Average Volume'],
            'Value': [
                f"{historical_data['Close'].iloc[-1]:,.0f} VND",
                f"{historical_data['High'].max():,.0f} VND",
                f"{historical_data['Low'].min():,.0f} VND",
                f"{historical_data['Close'].mean():,.0f} VND",
                f"{price_change:+,.0f} VND",
                f"{price_change_percent:+.2f}%",
                f"{avg_volume:,.0f}"
            ]
        }

        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

    print(f"\n💾 Data saved to: {filename}")
    print("✅ Mini project completed successfully!")

# import matplotlib.pyplot as plt
if historical_data is not None:
    # Tạo biểu đồ giá
    plt.figure(figsize=(12, 6))

    # Plot 1: Price over time
    plt.subplot(1, 2, 1)
    plt.plot(historical_data['Date'],
             historical_data['Close'], marker='o', linewidth=2)
    plt.title('Vinamilk Stock Price (30 days)', fontsize=14, fontweight='bold')
    plt.xlabel('Date')
    plt.ylabel('Price (VND)')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)

    # Plot 2: Volume
    plt.subplot(1, 2, 2)
    plt.bar(historical_data['Date'], historical_data['Volume'], alpha=0.7)
    plt.title('Trading Volume (30 days)', fontsize=14, fontweight='bold')
    plt.xlabel('Date')
    plt.ylabel('Volume')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()

    print("�� Charts generated successfully!")
