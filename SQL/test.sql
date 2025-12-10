-- Active: 1764400223395@@127.0.0.1@5432@finance_db
SELECT
    "Ticker",
    data ->> 'date' as ngày,
    data ->> 'priceLow' as giá_thấp_nhất,
    data ->> 'priceHigh' as giá_cao_nhất,
    data ->> 'priceOpen' as giá_mở_cửa
from raw.daily_price
where
    "Ticker" = 'VNM'
    and data ->> 'date' = '2024-12-05';

SELECT date, open
FROM raw.daily_price_history
WHERE
    "Ticker" = 'VCC'
ORDER BY date DESC
LIMIT 2000;

SELECT "Ticker", (item ->> 'priceOpen')::FLOAT, (item ->> 'priceHigh')::FLOAT, (item ->> 'priceLow')::FLOAT, (item ->> 'priceClose')::FLOAT, TO_CHAR(
        (item ->> 'totalVolume')::FLOAT, '999,999,999'
    ), TO_CHAR(
        (item ->> 'date')::DATE, 'DD/MM/YYYY'
    )
FROM raw.daily_price,
    -- Cú pháp "dấu phẩy" này tương đương với CROSS JOIN LATERAL
    jsonb_array_elements(data) AS item
WHERE
    "Ticker" = 'VNM'
    -- Thử lọc lấy ngày cụ thể
LIMIT 5;