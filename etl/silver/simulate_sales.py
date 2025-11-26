from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType, TimestampType
)
import datetime as dt
import random
import uuid
import sys

# ---- CONFIG ----
ICEBERG_WAREHOUSE = "s3://store-ops-dev-silver/iceberg/"
PRODUCTS_ICEBERG_TABLE = "glue_catalog.store_ops.silver_products_ic"
SALES_ICEBERG_TABLE = "glue_catalog.store_ops.silver_sales_ic"
NUM_DAYS = 30
TRANSACTIONS_PER_DAY = 500
NUM_STORES = 5
APP_NAME = "SimulateSalesIceberg"
# -----------------


def simulate_sales(spark):
    # 1) Load products from Iceberg to get SKUs + sale_price
    df_products = spark.read.table(PRODUCTS_ICEBERG_TABLE)

    df_prod = (
        df_products
        .select("sku", "sale_price")
        .where(F.col("sku").isNotNull())
        .distinct()
    )

    prod_rows = df_prod.collect()
    products = [(r["sku"], r["sale_price"]) for r in prod_rows]

    print(f"Loaded {len(products)} SKUs for sales simulation from {PRODUCTS_ICEBERG_TABLE}")

    all_rows = []
    today = dt.date.today()

    # 2) Generate sales for each day
    for day_offset in range(NUM_DAYS):
        sale_date = today - dt.timedelta(days=day_offset)

        for _ in range(TRANSACTIONS_PER_DAY):
            sku, price = random.choice(products)
            quantity = random.randint(1, 5)
            store_id = random.randint(1, NUM_STORES)

            sale_ts = dt.datetime.combine(
                sale_date,
                dt.time(
                    hour=random.randint(9, 21),
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
            )

            all_rows.append(
                (
                    str(uuid.uuid4()),
                    sku,
                    store_id,
                    quantity,
                    float(price),
                    float(price * quantity),
                    sale_ts,
                    sale_date.isoformat()
                )
            )

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("sku", LongType(), False),
        StructField("store_id", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), True),
        StructField("total_price", DoubleType(), True),
        StructField("ts", TimestampType(), True),
        StructField("dt_str", StringType(), True)
    ])

    df_sales = spark.createDataFrame(all_rows, schema)

    df_sales = (
        df_sales
        .withColumn("dt", F.to_date("dt_str"))
        .drop("dt_str")
        .withColumn("ingested_at", F.current_timestamp())
    )

    return df_sales


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.sparkContext.setLogLevel("WARN")

    # --- ICEBERG CONFIG ---
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    # ----------------------

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Create Iceberg table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SALES_ICEBERG_TABLE} (
            transaction_id STRING,
            sku BIGINT,
            store_id INT,
            quantity INT,
            unit_price DOUBLE,
            total_price DOUBLE,
            ts TIMESTAMP,
            ingested_at TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
    """)

    df_sales = simulate_sales(spark)
    print(f"Generated rows: {df_sales.count()}")

    (
        df_sales
        .writeTo(SALES_ICEBERG_TABLE)
        .overwritePartitions()
    )

    print(f"✅ Sales written to Iceberg table {SALES_ICEBERG_TABLE}")

    job.commit()


if __name__ == "__main__":
    main()
