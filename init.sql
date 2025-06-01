-- Создание основной таблицы для импорта данных
CREATE TABLE mock_data (
    id BIGINT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INTEGER,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC,
    product_quantity INTEGER,
    sale_date TEXT,
    sale_customer_id BIGINT,
    sale_seller_id BIGINT,
    sale_product_id BIGINT,
    sale_quantity INTEGER,
    sale_total_price NUMERIC,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight NUMERIC,
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC,
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

-- Импорт данных из CSV-файлов
COPY mock_data FROM '/data/MOCK_DATA.csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (1).csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (2).csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (3).csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (4).csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (5).csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (6).csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (7).csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (8).csv' WITH CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (9).csv' WITH CSV HEADER;

-- Создание звездной схемы
CREATE SCHEMA IF NOT EXISTS star_schema;

-- Dimension Tables
CREATE TABLE IF NOT EXISTS star_schema.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id BIGINT,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS star_schema.dim_seller (
    seller_key    SERIAL PRIMARY KEY,
    seller_id     BIGINT,
    first_name    TEXT,
    last_name     TEXT,
    email         TEXT,
    country       TEXT,
    postal_code   TEXT
);

CREATE TABLE IF NOT EXISTS star_schema.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id BIGINT,
    name TEXT,
    category TEXT,
    price NUMERIC(10,2),
    weight NUMERIC(10,2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    rating NUMERIC(3,1),
    reviews INTEGER,
    supplier_name TEXT
);

CREATE TABLE IF NOT EXISTS star_schema.dim_store (
    store_key SERIAL PRIMARY KEY,
    store_name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS star_schema.dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS star_schema.dim_date (
    date_key SERIAL PRIMARY KEY,
    sale_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

-- Fact Table
CREATE TABLE IF NOT EXISTS star_schema.fact_sales (
    sale_id BIGINT PRIMARY KEY,
    customer_key INTEGER REFERENCES star_schema.dim_customer(customer_key),
    seller_key INTEGER REFERENCES star_schema.dim_seller(seller_key),
    product_key INTEGER REFERENCES star_schema.dim_product(product_key),
    store_key INTEGER REFERENCES star_schema.dim_store(store_key),
    supplier_key INTEGER REFERENCES star_schema.dim_supplier(supplier_key),
    date_key INTEGER REFERENCES star_schema.dim_date(date_key),
    quantity INTEGER,
    total_price NUMERIC(10,2)
);