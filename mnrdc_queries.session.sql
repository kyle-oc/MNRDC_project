-- 1. Identify number of stores in individual countries

SELECT
    country_code,
    COUNT(store_code) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_no_stores DESC;

-- 2. Identify which locations have highest number of stores

SELECT
    locality,
    COUNT(store_code) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC
LIMIT
    7;

-- 3. Identify months with highest sales totals

SELECT
    SUM(dpt.product_price * dot.product_quantity) AS total_sales,
    dtt.month
FROM
    dim_orders_table AS dot
JOIN
    dim_date_times AS dtt ON dot.date_uuid = dtt.date_uuid
JOIN
    dim_products AS dpt ON dot.product_code = dpt.product_code
GROUP BY
    dtt.month
ORDER BY
    total_sales DESC
LIMIT
    6;

-- 4. Calculate number of transactions and product quantities online vs offline

SELECT
    COUNT(dot.date_uuid) AS number_of_sales,
    SUM(dot.product_quantity) AS product_quantity_count,
    CASE
        WHEN dst.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    dim_orders_table AS dot
JOIN
    dim_store_details AS dst ON dot.store_code = dst.store_code
GROUP BY
    CASE
        WHEN dst.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END
ORDER BY location DESC;

-- 5. Calculate percentage of sales for store types

WITH store_sales AS (
    SELECT
        dst.store_type,
        SUM(dpt.product_price * dot.product_quantity) AS total_sales,
        COUNT(*) AS number_of_sales
    FROM
        dim_orders_table AS dot
    JOIN
        dim_store_details AS dst ON dot.store_code = dst.store_code
    JOIN
        dim_products AS dpt ON dot.product_code = dpt.product_code
    GROUP BY
        dst.store_type
),
total_sales_count AS (
    SELECT SUM(number_of_sales) AS total_sales FROM store_sales
)

SELECT
    ss.store_type,
    ROUND(ss.total_sales, 2) AS total_sales,
    ROUND((ss.number_of_sales::DECIMAL / tsc.total_sales) * 100, 2) AS "sales_made(%)"
FROM
    store_sales ss, total_sales_count tsc
ORDER BY
    total_sales DESC;

-- 6. Identify best months for each year

WITH monthly_sales AS (
    SELECT
        SUM(dpt.product_price * dot.product_quantity) AS total_sales,
        ddt.year,
        ddt.month
    FROM dim_orders_table AS dot
    JOIN dim_products AS dpt ON dot.product_code = dpt.product_code
    JOIN dim_date_times AS ddt ON dot.date_uuid = ddt.date_uuid
    GROUP BY ddt.year, ddt.month
),
ranked_sales AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_sales DESC) AS rn
    FROM monthly_sales
)
SELECT total_sales, year, month
FROM ranked_sales
WHERE rn = 1
ORDER BY total_sales DESC
LIMIT 9;

-- 7. Identify staff numbers per country

SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;

-- 8. Identify best performing store type in Germany

SELECT
    SUM(dpt.product_price * dot.product_quantity) AS total_sales,
    dst.store_type,
    dst.country_code
FROM
    dim_orders_table AS dot
JOIN
    dim_products AS dpt ON dot.product_code = dpt.product_code
JOIN
    dim_store_details AS dst ON dot.store_code = dst.store_code
WHERE
    dst.country_code = 'DE'
GROUP BY
    dst.store_type,
    dst.country_code
ORDER BY
    total_sales;

-- 9. Calculate average time between transactions

WITH sales_with_timestamps AS (
    SELECT
        TO_TIMESTAMP(
            CONCAT(year, '-', month, '-', day, ' ', "timestamp"),
            'YYYY-MM-DD HH24:MI:SS.MS'
        ) AS full_timestamp,
        year
    FROM dim_date_times
),

sales_with_lead AS (
    SELECT
        year,
        full_timestamp,
        LEAD(full_timestamp) OVER (ORDER BY full_timestamp) AS next_sale_timestamp
    FROM sales_with_timestamps
),

sales_differences AS (
    SELECT
        year,
        next_sale_timestamp - full_timestamp AS time_diff
    FROM sales_with_lead
    WHERE next_sale_timestamp IS NOT NULL
),

avg_diffs_by_year AS (
    SELECT
        year,
        AVG(time_diff) AS avg_time_diff
    FROM sales_differences
    GROUP BY year
)

SELECT
    year,
    CONCAT(
        '"hours": ', EXTRACT(HOUR FROM avg_time_diff), ', ',
        '"minutes": ', EXTRACT(MINUTE FROM avg_time_diff), ', ',
        '"seconds": ', ROUND(EXTRACT(SECOND FROM avg_time_diff)), ', ',
        '"milliseconds": ', ROUND(EXTRACT(MILLISECONDS FROM avg_time_diff))
    ) AS actual_time_taken
FROM avg_diffs_by_year
ORDER BY avg_time_diff DESC
LIMIT 5;