from pyspark.sql import SparkSession, functions as F

# ---- CONFIG ----
BRONZE_PATH = "s3://store-ops-dev-bronze/bestbuy/catalog/"   
SILVER_PATH = "s3://store-ops-dev-silver/silver_products/"   
APP_NAME = "CurateBestBuyCatalog"
# -----------------

def build_spark():
    spark = (SparkSession.builder.appName(APP_NAME).getOrCreate())
    return spark

def curate_catalog(spark):
    # 1) Read raw Bronze JSONL
    df_raw = (
        spark.read
        .json(f"{BRONZE_PATH}*/")   # bestbuy/catalog/dt=YYYY-MM-DD/*.jsonl
    )

    # 2) Extract department, category, subcategory from categoryPath
    df = (
        df_raw
        .withColumn("department", F.col("categoryPath").getItem(0)["name"])
        .withColumn("category",   F.col("categoryPath").getItem(1)["name"])
        .withColumn("subcategory", F.col("categoryPath").getItem(2)["name"])
    )

    # 3) Clean core fields + add discount_pct + ingestion_date
    df_curated = (
        df
        .select(
            F.col("sku").cast("bigint").alias("sku"),
            F.col("name").alias("product_name"),
            F.col("regularPrice").cast("double").alias("regular_price"),
            F.col("salePrice").cast("double").alias("sale_price"),
            "department",
            "category",
            "subcategory",
        )
        .withColumn(
            "discount_pct",
            F.when(F.col("regular_price") > 0,
                   (F.col("regular_price") - F.col("sale_price")) / F.col("regular_price") * 100.0
            ).otherwise(F.lit(0.0))
        )
        .withColumn("ingestion_date", F.current_date())
    )

    return df_curated


def write_silver(df_curated):
    # For now: write curated data as Parquet, partitioned by ingestion_date
    (
        df_curated
        .repartition("ingestion_date")
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("ingestion_date")
        .save(SILVER_PATH)
    )


def main():
    spark = build_spark()
    df_curated = curate_catalog(spark)
    print(f"Curated count: {df_curated.count()}")
    write_silver(df_curated)
    print(f"✅ Silver written to {SILVER_PATH}")


if __name__ == "__main__":
    main()
