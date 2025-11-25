from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import sys

# ---------------- CONFIG ----------------
ICEBERG_WAREHOUSE = "s3://store-ops-dev-silver/iceberg/"

PRODUCTS_TBL = "glue_catalog.store_ops.silver_products_ic"
INV_TBL      = "glue_catalog.store_ops.silver_inventory_ic"
SALES_TBL    = "glue_catalog.store_ops.silver_sales_ic"

GOLD_TBL     = "glue_catalog.store_ops.gold_kpis_ic"
# ----------------------------------------


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.sparkContext.setLogLevel("WARN")

    # -------- ICEBERG CONFIG ----------
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    # ----------------------------------

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # ========== READ SILVER TABLES ==========
    df_products = spark.read.table(PRODUCTS_TBL)
    df_inventory = spark.read.table(INV_TBL)
    df_sales = spark.read.table(SALES_TBL)

    # ========== SALES AGGREGATION ==========
    df_sales_agg = (
        df_sales
        .groupBy("dt", "sku", "store_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("total_price").alias("revenue"),
            F.avg("unit_price").alias("avg_unit_price")
        )
    )

    # ========== JOIN 1: Sales + Products ==========
    df_join1 = (
        df_sales_agg.alias("s")
        .join(df_products.alias("p"), "sku", "left")
        .select(
            "s.dt",
            "sku",
            F.col("p.name").alias("product_name"),
            "p.category",
            "s.store_id",
            "units_sold",
            "revenue",
            "avg_unit_price",
            "p.regular_price",
            "p.sale_price",
            "p.discount_pct"
        )
    )

    # ========== JOIN 2: Add Inventory ==========
    df_join2 = (
        df_join1.alias("g")
        .join(
            df_inventory.alias("i"),
            ["dt", "sku", "store_id"],
            "left"
        )
        .select(
            "g.*",
            "i.stock_on_hand",
            "i.stock_reserved"
        )
    )

    # ========== KPI CALCULATIONS ==========
    df_gold = (
        df_join2
        .withColumn(
            "margin_pct",
            F.when(F.col("revenue") > 0,
                   (F.col("revenue") - (F.col("regular_price") * F.col("units_sold")))
                   / F.col("revenue") * 100
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "stock_out_rate",
            F.when(F.col("units_sold") > F.col("stock_on_hand"), 1.0).otherwise(0.0)
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    # ========== WRITE TO GOLD ICEBERG ==========
    (
        df_gold
        .writeTo(GOLD_TBL)
        .overwritePartitions()
    )

    print(f"🔥 GOLD KPIs written to {GOLD_TBL}")

    job.commit()


if __name__ == "__main__":
    main()
