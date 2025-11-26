from datetime import datetime, timedelta
from etl.bronze.bestbuy_pull import main as bronze_main
import sys
sys.path.append("/opt/airflow")

from datetime import datetime, timedelta
from etl.bronze.bestbuy_pull import main as bronze_main
# etc...

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---- Import your project ETL entrypoints ----
from etl.bronze.bestbuy_pull import main as bronze_main
from etl.silver.trigger_curate_catalog_glue import main as silver_catalog_main

# If you named these differently, adjust the imports:
try:
    from etl.silver.simulate_inventory import main as simulate_inventory_main
except ImportError:
    simulate_inventory_main = None

try:
    from etl.silver.simulate_sales import main as simulate_sales_main
except ImportError:
    simulate_sales_main = None

try:
    from etl.gold.marts_store_ops import main as gold_kpis_main
except ImportError:
    gold_kpis_main = None

try:
    from etl.snowflake_load.load_gold_to_snowflake import load_gold as snowflake_load_main
except ImportError:
    snowflake_load_main = None

# ---- Default args ----
default_args = {
    "owner": "mani",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bestbuy_ingest_to_gold_dag",
    description="Daily BestBuy → Bronze → Silver → Gold → Snowflake pipeline",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["store-ops", "bestbuy", "analytics"],
) as dag:

    # ---- Bronze: BestBuy API → S3 ----
    bronze_ingest = PythonOperator(
        task_id="bronze_bestbuy_ingest",
        python_callable=bronze_main,
    )

    # ---- Silver: curate products ----
    silver_products = PythonOperator(
        task_id="silver_curate_products",
        python_callable=silver_catalog_main,
    )

    # ---- Silver: inventory simulation (if script exists) ----
    if simulate_inventory_main:
        silver_inventory = PythonOperator(
            task_id="silver_simulate_inventory",
            python_callable=simulate_inventory_main,
        )
    else:
        silver_inventory = None

    # ---- Silver: sales simulation (if script exists) ----
    if simulate_sales_main:
        silver_sales = PythonOperator(
            task_id="silver_simulate_sales",
            python_callable=simulate_sales_main,
        )
    else:
        silver_sales = None

    # ---- Gold: build KPIs (Iceberg gold_kpis_ic) ----
    if gold_kpis_main:
        gold_kpis = PythonOperator(
            task_id="gold_build_kpis",
            python_callable=gold_kpis_main,
        )
    else:
        gold_kpis = None

    # ---- Snowflake: load GOLD_KPIS_IC ----
    if snowflake_load_main:
        snowflake_load = PythonOperator(
            task_id="snowflake_load_gold_kpis_ic",
            python_callable=snowflake_load_main,
        )
    else:
        snowflake_load = None

    # ---- Dependencies ----
    # bronze → silver_products
    bronze_ingest >> silver_products

    # silver_products → inventory / sales (if they exist)
    downstream_after_silver = []

    if silver_inventory:
        silver_products >> silver_inventory
        downstream_after_silver.append(silver_inventory)

    if silver_sales:
        silver_products >> silver_sales
        downstream_after_silver.append(silver_sales)

    # If both inventory + sales exist, gold depends on both
    if gold_kpis:
        if downstream_after_silver:
            for t in downstream_after_silver:
                t >> gold_kpis
        else:
            # fallback: no inv/sales tasks defined
            silver_products >> gold_kpis

    # gold → snowflake
    if gold_kpis and snowflake_load:
        gold_kpis >> snowflake_load
