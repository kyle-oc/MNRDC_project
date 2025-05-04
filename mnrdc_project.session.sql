-- 1. CAST DIM_ORDERS_TABLE COLUMNS:
-- 1.1 Get max lengths for card_number, store_code, product_code in dim_orders_table:
SELECT
    MAX(LENGTH(card_number::TEXT)) AS max_card_number_length,
    MAX(LENGTH(store_code::TEXT)) AS max_store_code_length,
    MAX(LENGTH(product_code::TEXT)) AS max_product_code_length
FROM dim_orders_table;

-- 1.2 Cast columns in dim_orders_table:
ALTER TABLE dim_orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- 2. CAST DIM_USERS COLUMNS:
-- 2.1:
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- 3. UPDATE DIM_STORE_DETAILS: 
-- 3.1 Merge latitude columns
UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat);

-- 3.2 Drop redundant 'lat' column
ALTER TABLE dim_store_details
DROP COLUMN lat;

-- 3.3 Replace 'N/A' in business's website with NULL
UPDATE dim_store_details
SET address = NULL
WHERE address = 'N/A';

-- 3.4 Ascertain max lengths for VARCHAR conversions:
SELECT
    MAX(LENGTH(store_code:: TEXT)) AS max_store_code_length, --(12)
    MAX(LENGTH(country_code:: TEXT)) AS max_country_code_length --(2)
FROM
    dim_store_details;

-- 3.5 Change column data types
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE NUMERIC USING latitude::NUMERIC,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

-- 4. MAKE CHANGES TO DIM_PRODUCTS TABLE:
-- 4.1 Remove '£' from product_price column
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- 4.2 Add new column weight_class
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

-- 4.3 Populate weight_class column
UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

-- 5. UPDATE DIM_PRODUCTS TABLE DATA TYPES
-- 5.1 Rename removed column to still_available
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Rename 'EAN' column to 'ean' to prevent casing errors
ALTER TABLE dim_products
RENAME COLUMN "EAN" TO ean;

-- Identify max lengths of VARCHAR columns
SELECT
    MAX(LENGTH(ean:: TEXT)) AS max_ean_length, -- 17
    MAX(LENGTH(product_code:: TEXT)) AS max_product_code_length, -- 11
    MAX(LENGTH(weight_class:: TEXT)) AS max_weight_class_length -- 14
FROM
    dim_products;

-- Check bool conversion with SELECT statement before changing
SELECT
    still_available,
    still_available = 'Still_available' AS converted_bool
FROM
    dim_products;

-- 5.2 Change column data types
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE NUMERIC USING product_price::NUMERIC,
    ALTER COLUMN weight TYPE NUMERIC USING weight::NUMERIC,
    ALTER COLUMN ean TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN still_available TYPE BOOL USING (still_available = 'Still_available'),
    ALTER COLUMN weight_class TYPE VARCHAR(14);

-- 6. UPDATE DIM_DATE_TIMES TABLE
-- 6.1 Identify max lengths of columns
SELECT
    MAX(LENGTH(month:: TEXT)) AS max_month_length, -- 2
    MAX(LENGTH(year:: TEXT)) AS max_year_length, -- 4
    MAX(LENGTH(day:: TEXT)) AS max_day_length, -- 2
    MAX(LENGTH(time_period:: TEXT)) AS max_time_period_length -- 10
FROM
    dim_date_times;

-- 6.2 Change column data types
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- 7. UPDATE DIM_CARD_DETAILS TABLE
-- 7.1 Identify max column lengths

SELECT
    MAX(LENGTH(card_number:: TEXT)) AS max_card_number_length, -- 19
    MAX(LENGTH(expiry_date:: TEXT)) AS max_expiry_date_length -- 5
FROM
    dim_card_details;

-- Change column data types
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- 8. CREATE PRIMARY KEYS FOR TABLES
-- dim_card_details
SELECT COUNT(*), COUNT(DISTINCT card_number) FROM dim_card_details;

ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number); 

-- dim_date_times
SELECT COUNT(*), COUNT(DISTINCT date_uuid) FROM dim_date_times;

ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);

-- dim_products
SELECT COUNT(*), COUNT(DISTINCT product_code) FROM dim_products;

ALTER TABLE dim_products ADD PRIMARY KEY (product_code);

-- dim_store_details
SELECT COUNT(*), COUNT(DISTINCT store_code) FROM dim_store_details;

ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);

-- dim_users
SELECT COUNT(*), COUNT(DISTINCT user_uuid) FROM dim_users;

ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid)

-- 9. ADD FOREIGN KEYS TO ORDERS TABLE

-- card_details fk
ALTER TABLE dim_orders_table
ADD CONSTRAINT fk_card
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

-- date_times fk
ALTER TABLE dim_orders_table
ADD CONSTRAINT fk_date
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);

-- products fk
ALTER TABLE dim_orders_table
ADD CONSTRAINT fk_product
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);

-- store fk
ALTER TABLE dim_orders_table
ADD CONSTRAINT fk_store
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);

-- user fk
ALTER TABLE dim_orders_table
ADD CONSTRAINT fk_user
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);