import os
import snowflake.connector
from dotenv import load_dotenv

# load variables from .env in project root
load_dotenv()

ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
USER      = os.getenv("SNOWFLAKE_USER")
PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "STORE_OPS")
SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "GOLD")
STAGE = "GOLD_KPIS_STAGE"
TABLE = "GOLD_KPIS_IC"



# --- safety check so we see the real problem early ---
print("ACCOUNT =", ACCOUNT)
print("USER    =", USER)

if not ACCOUNT or not USER or not PASSWORD:
    raise RuntimeError(
        "Missing one of SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD "
        "environment variables. Check your .env file."
    )


def load_gold():
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role="ACCOUNTADMIN"
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
