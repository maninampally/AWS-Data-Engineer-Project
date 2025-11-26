import snowflake.connector
import os

ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
USER      = os.getenv("SNOWFLAKE_USER")
PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
WAREHOUSE = "COMPUTE_WH"
DATABASE  = "STORE_OPS"
SCHEMA    = "GOLD"
STAGE     = "GOLD_KPIS_STAGE"
TABLE     = "GOLD_KPIS"


def load_gold():
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    cur = conn.cursor()

    print("🔄 Loading Gold KPI data...")

    cur.execute(f"""
        COPY INTO {TABLE}
        FROM @{STAGE}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """)

    print("✅ Load Completed")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_gold()
