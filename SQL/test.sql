-- Active: 1764400223395@@127.0.0.1@5432@finance_db
SELECT "Ticker", "Year", "Quarter", "data"
FROM raw.ic_quarter
WHERE "Ticker" = 'VNM' AND "Year" = 2024
LIMIT 1;


SELECT 
    "Ticker",
    "Year",
    "Quarter",
    item->>'id' AS ma_chi_tieu,
    item->>'name' AS ten_chi_tieu,
    -- Truy cập sâu vào: values -> phần tử 0 -> value
    (item->'values'->0->>'value')::NUMERIC AS gia_tri 
FROM 
    raw.ic_quarter,
    jsonb_array_elements(data) AS item -- Hàm này bung mảng ra thành các item
WHERE 
    "Ticker" = 'VNM' 
    AND "Year" = 2024
LIMIT 20;


DROP TABLE raw.daily_price;