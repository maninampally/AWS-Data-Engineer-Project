CREATE TABLE IF NOT EXISTS glue_catalog.store_ops.gold_kpis_ic (
    dt DATE,
    sku BIGINT,
    product_name STRING,
    category STRING,
    store_id INT,
    units_sold INT,
    revenue DOUBLE,
    avg_unit_price DOUBLE,
    regular_price DOUBLE,
    sale_price DOUBLE,
    discount_pct DOUBLE,
    margin_pct DOUBLE,
    stock_on_hand INT,
    stock_reserved INT,
    stock_out_rate DOUBLE,
    ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (dt);
