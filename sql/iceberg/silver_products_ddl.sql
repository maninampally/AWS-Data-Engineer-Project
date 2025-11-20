CREATE TABLE IF NOT EXISTS store_ops.silver_products (
    sku BIGINT,
    name STRING,
    brand STRING,
    regular_price DOUBLE,
    sale_price DOUBLE,
    on_sale BOOLEAN,
    category STRING,
    review_average DOUBLE,
    online_availability BOOLEAN,
    discount_pct DOUBLE,
    ingested_at TIMESTAMP,
    dt DATE
)
USING iceberg
PARTITIONED BY (dt);
