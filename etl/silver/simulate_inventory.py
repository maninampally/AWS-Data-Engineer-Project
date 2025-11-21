# etl/silver/simulate_inventory.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    DateType, LongType, StringType, IntegerType, TimestampType
)
from datetime import date, timedelta

SILVER_BUCKET = "s3://store-ops-dev-silver"   # change if needed
SILVER_PREFIX = "inventory"                   # s3://bucket/inventory/...


def get_spark():
    return (
        SparkSession.builder
        .appName("SimulateInventoryData")
        .getOrCreate()
    )


def generate_dates(start: date, days: int):
    return [start + timedelta(days=i) for i in range(days)]


def main():
    spark = get_spark()

    # ----- config -----
    start_date = date(2025, 1, 1)    # pick any start date
    days = 30                        # 30 days
    store_ids = ["STORE_001", "STORE_002", "STORE_003"]
    skus = [100001, 100002, 100003, 100004, 100005]

    dates = generate_dates(start_date, days)

    # build rows: one row per (date, store, sku)
    rows = []
    for d in dates:
        for store in store_ids:
            for sku in skus:
                stock_on_hand = max(0, 50 + (hash((d, store, sku)) % 50) - 25)
                stock_reserved = max(0, stock_on_hand // 4)
                rows.append(
                    (d, sku, store, stock_on_hand, stock_reserved)
                )

    schema = StructType([
        StructField("inventory_date", DateType(), False),
        StructField("sku", LongType(), False),
        StructField("store_id", StringType(), False),
        StructField("stock_on_hand", IntegerType(), False),
        StructField("stock_reserved", IntegerType(), False),
    ])

    df = spark.createDataFrame(rows, schema)

    # add updated_at timestamp
    df = df.withColumn("updated_at", F.current_timestamp())

    # write as partitioned Parquet into Silver S3
    output_path = f"{SILVER_BUCKET}/{SILVER_PREFIX}"
    (
        df.write
        .mode("overwrite")
        .partitionBy("inventory_date")
        .parquet(output_path)
    )

    print(f"✅ Wrote inventory data to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
