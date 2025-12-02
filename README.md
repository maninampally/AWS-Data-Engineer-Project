# 🚀 Store Ops Analytics — AWS Data Engineering Project

A complete **end-to-end data engineering pipeline** built using:

- **AWS S3 Lakehouse (Bronze → Silver → Gold)**
- **PySpark transformations**
- **Great Expectations for data quality**
- **Snowflake as the warehouse**
- **Power BI dashboards**
- **Apache Airflow for orchestration (Docker)**

This project replicates how modern data engineering teams build production data pipelines.

---

## 🏗 Architecture Overview
```
          BestBuy API  
              ⬇  
  Bronze Layer — Raw JSON in S3  
               ⬇  
Silver Layer — Cleaned PySpark Parquet  
              ⬇  
  Gold Layer — Business KPI Tables  
               ⬇  
   Snowflake — Fact & KPI Models  
              ⬇  
    Power BI Dashboards  
             ⬇  
  Airflow — Orchestration
```

---

## 🛠 Tech Stack

| Layer | Tools |
|------|-------|
| Cloud | AWS S3, SNS, IAM, Secrets Manager |
| Compute | PySpark, Python |
| Data Quality | Great Expectations |
| Warehouse | Snowflake |
| Orchestration | Apache Airflow (Docker) |
| BI | Power BI |
| Storage | Parquet, Iceberg |
| CI/CD | GitHub Actions |

---

---

## 🔥 Key Features

### ✔ 1. BestBuy API Ingestion (Bronze)
- Secure API key via **AWS Secrets Manager**
- Handles pagination & request limits
- Stores raw JSON in S3 (`dt=YYYY-MM-DD`)
- SNS alerts for success/failure

### ✔ 2. PySpark Transformations (Silver)
- Data cleaning and schema enforcement  
- Category hierarchy extraction  
- Price validation  
- Synthetic **inventory** & **sales** simulation  
- Saves optimized Parquet to S3  

### ✔ 3. Business KPIs (Gold)
- Product pricing KPIs  
- Sales KPIs (revenue, units sold)  
- Inventory KPIs (stock, low-stock detection)  
- Clean output for analytics + Snowflake  

### ✔ 4. Data Quality (Great Expectations)
- Null checks, type checks, value rules  
- Price consistency validation  
- KPI rule validation  
- Pipeline stops if validation fails  

### ✔ 5. Snowflake Warehouse
- Loads Gold Parquet → Stage  
- `COPY INTO` → Staging  
- `MERGE INTO` → Fact tables  
- Fully incremental & idempotent  

### ✔ 6. Power BI Dashboards
- Product performance  
- Sales trends  
- Inventory health  
- Category insights  

### ✔ 7. Airflow Orchestration (Docker)
- DAG: `bronze → silver → gold → snowflake`
- Retries, logging, scheduling  
- Runs via Docker Compose  

---

## 📁 Repository Structure
```
├── etl/
│ ├── bronze/ # Ingestion scripts
│ ├── silver/ # Transformations
│ ├── gold/ # KPI calculations
│ ├── snowflake_load/ # Warehouse loader
│ └── utils/ # Helpers & validation
│
├── expectations/ # Great Expectations config & suites
│ ├── ge_config.yml
│ └── suites/
│ ├── silver_products_suite.yml
│ ├── silver_inventory_suite.yml
│ ├── silver_sales_suite.yml
│ └── gold_kpis_suite.yml
│
├── dags/
│ ├── bestbuy_ingest_to_gold_dag.py
│ └── utils/
│ ├── callbacks.py
│ └── variables.py
│
├── sql/
│ ├── iceberg/ # Iceberg DDL
│ └── snowflake/ # Snowflake DDL + MERGE logic
│
├── bi/
│ └── powerbi/ # Dashboard notes
│
└── env/
├── dev.yaml
└── prod.yaml
```

---

## 🚀 Running the Pipeline
# 🏃‍♂️ How to Run the Pipeline (From Cloning the Repository)

Follow these steps to run the entire data engineering pipeline from scratch.

---

## 1️⃣ Clone the Repository
```
git clone https://github.com/maninampally/store-ops-analytics.git
cd store-ops-analytics
```
---

## 2️⃣ Create & Activate Virtual Environment
```
### Windows
python -m venv .venv
.venv\Scripts\activate
```

### Mac / Linux
```
python3 -m venv .venv
source .venv/bin/activate
```

---

## 3️⃣ Install All Dependencies
```
pip install -r requirements.txt
```

---

## 4️⃣ Configure AWS Credentials

You must have:
- AWS Access Key ID  
- AWS Secret Access Key  
- Region (ex: us-east-2)
```
aws configure
```

This allows:
- Bronze job to write to S3  
- Secrets Manager access  
- SNS notifications  

---

## 5️⃣ Add Your BestBuy API Key to AWS Secrets Manager

Create a secret:

aws secretsmanager create-secret
--name bestbuy_api_key
--secret-string "YOUR_API_KEY"


Your code automatically retrieves it using:
```
get_bestbuy_api_key()
```

---

## 6️⃣ Run the Full ETL Pipeline (Step-by-Step)

### 👉 Bronze Layer (Raw Data Ingestion)
Fetch BestBuy product data and store JSON in S3:
```
python -m etl.bronze.bestbuy_pull
```

### 👉 Silver Layer (Clean Transformations)
Run PySpark cleaning + inventory + sales simulation:
```
python -m etl.silver.curate_catalog
```


### 👉 Gold Layer (Business KPIs)
Generate aggregated KPI tables:
```
python -m etl.gold.marts_store_ops
```


### 👉 Load Gold Tables into Snowflake
```
python -m etl.snowflake_load.load_gold_to_snowflake
```


---

## 7️⃣ Start Apache Airflow (Optional – Full Automation)

Make sure Docker Desktop is running.

Start Airflow:
```
docker compose up --build
```


Airflow UI:

http://localhost:8080


Trigger the DAG:
```
bestbuy_ingest_to_gold_dag
```

This runs:

- Bronze ingestion  
- Silver transformation  
- Gold KPI generation  
- Snowflake load  
- Notifications  

ALL automatically.

---

## 8️⃣ View Your Data in Snowflake & Power BI

### Snowflake
Use:
```
USE SCHEMA store_ops.gold;
SELECT * FROM gold_sales_kpis;
```

### Power BI
1. Open Power BI Desktop  
2. Connect → “Snowflake”  
3. Load gold tables  
4. Build dashboards  

---

## 🎉 Pipeline Complete!

You now have:

- Automated ingestion  
- Clean curated datasets  
- Validated KPIs  
- Snowflake models  
- BI dashboards  
- Airflow orchestration  

End-to-end production-style data engineering pipeline!<img width="1404" height="12305" alt="image" src="https://github.com/user-attachments/assets/b3562653-efd7-4914-9674-75ed31712f57" />
