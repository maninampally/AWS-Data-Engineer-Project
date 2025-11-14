# 🏬 Store Operations Digitization & Workforce Productivity Analytics Platform

End-to-end **AWS Data Engineering Project** that digitizes store operations and workforce analytics for a retail chain (like Walmart/Target/Costco).  
The system ingests **real-time in-store events** and **e-commerce catalog data**, processes them into a **Lakehouse architecture (S3 + Iceberg)**, and visualizes KPIs in **QuickSight**.

---

## 🚀 Architecture Overview

**Flow:**  
`Kinesis → Firehose → S3 (Bronze) → Glue/EMR Spark ETL → Iceberg on S3 (Silver/Gold) → Glue Catalog → Athena + Redshift Serverless → Airflow (MWAA) / StepFunctions → Great Expectations → QuickSight → GitHub Actions`

### Key Components
| Layer | AWS Service | Description |
|--------|--------------|-------------|
| Ingestion | **Kinesis / Firehose** | Real-time in-store events streaming to S3 |
| Storage | **Amazon S3** | Data lake (Bronze, Silver, Gold layers) |
| Processing | **AWS Glue / EMR Spark** | ETL transformations and Iceberg writes |
| Table Format | **Apache Iceberg** | ACID tables and time-travel on S3 |
| Metadata | **AWS Glue Catalog** | Central schema registry |
| Query | **Athena / Redshift Serverless** | Ad-hoc + analytical queries |
| Orchestration | **MWAA (Airflow)** / **Step Functions** | Pipeline scheduling |
| Data Quality | **Great Expectations** | Validation and DQ alerts |
| Monitoring | **CloudWatch + SNS** | Logs, metrics, and alerts |
| Visualization | **QuickSight** | Retail & workforce KPI dashboards |
| CI/CD | **GitHub Actions** | Automatic code deploy to S3 |
| Config | **AWS Console + YAML** | Hybrid setup: GUI + IaC control |

---

## 🧱 Folder Structure

