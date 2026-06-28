# \U0001F6D2 Retail Lakehouse Pipeline

> An end-to-end, production-style **batch data lakehouse** that ingests **1M+ retail records**, processes them through a **Medallion architecture (Bronze \u2192 Silver \u2192 Gold)** using **PySpark + Delta Lake**, models analytics marts with **dbt**, and orchestrates everything with **Apache Airflow** \u2014 guarded by **15+ automated data-quality checks** across every layer.

`Python` \u00b7 `Apache Spark` \u00b7 `Delta Lake` \u00b7 `dbt` \u00b7 `Airflow` \u00b7 `Docker` \u00b7 `MIT License`

---

## \U0001F4CC Why this project

This repository demonstrates how a real production data platform is built and operated \u2014 the same patterns I use day-to-day building and migrating pipelines at enterprise scale (Expedia, Atlassian, Adidas).

It is **runnable end-to-end on a laptop** via Docker, yet mirrors how teams build on AWS EMR + S3 + Databricks in production. The sample generator produces **1M+ records** by default so the pipeline exercises genuine Spark/Delta scale.

---

## \u2699\uFE0F Scale & Performance

| Aspect | Value |
| --- | --- |
| Records processed | **1M+** (1,000,000 orders + 50,000 customers + 2,000 products) |
| Layers | Bronze \u2192 Silver \u2192 Gold (Delta Lake) |
| Data-quality gates | **15+ automated checks** across all layer transitions |
| Idempotency | Re-runnable without duplicates (Delta MERGE) |
| Run anywhere | Local laptop (Docker) or AWS EMR + S3 |

> Quick smoke run (fewer rows): `python data/generate_sample_data.py --orders 20000 --customers 1000`

---

## \U0001F3D7\uFE0F Architecture

```
                 \u250C\u2500 APACHE AIRFLOW (orchestration) \u2500\u2510
                 \u2502 ingest \u2192 quality \u2192 silver \u2192 quality \u2192 gold \u2192 dbt \u2192 publish
                 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518

  raw CSV/JSON \u2192 BRONZE (raw Delta) \u2192 SILVER (clean/conform) \u2192 GOLD (aggregates) \u2192 dbt marts
                       \u2514\u2500\u2500 Data-quality gates at every hop (counts, nulls, dupes, schema, ranges, RI) \u2500\u2500\u2518
```

**Medallion layers**

| Layer | Purpose | Tech |
| --- | --- | --- |
| \U0001F949 Bronze | Raw ingestion, schema-on-read, append-only history | PySpark + Delta |
| \U0001F948 Silver | Cleaned, deduplicated, type-cast, conformed | PySpark + Delta |
| \U0001F947 Gold | Business-level aggregates and KPIs | PySpark + Delta |
| \U0001F4CA Marts | Analytics-ready dimensional models | dbt |

---

## \U0001F9F0 Tech Stack

* **Processing:** Apache Spark 3.5 (PySpark)
* **Storage Format:** Delta Lake (ACID, time travel, schema enforcement)
* **Transformation/Modeling:** dbt (staging \u2192 marts)
* **Orchestration:** Apache Airflow 2.8
* **Data Quality:** Custom framework \u2014 15+ checks across layers
* **Local Infra:** Docker Compose
* **Language:** Python 3.10, SQL

---

## \U0001F4C2 Project Structure

```
retail-lakehouse-pipeline/
\u251C\u2500\u2500 dags/
\u2502   \u2514\u2500\u2500 retail_pipeline_dag.py        # Airflow DAG orchestrating the full pipeline
\u251C\u2500\u2500 src/
\u2502   \u251C\u2500\u2500 config.py                     # Central config (paths, layer names)
\u2502   \u251C\u2500\u2500 ingestion/bronze_ingest.py    # Raw \u2192 Bronze (Delta)
\u2502   \u251C\u2500\u2500 transform/silver_transform.py # Bronze \u2192 Silver (clean/dedupe/conform)
\u2502   \u251C\u2500\u2500 transform/gold_aggregate.py   # Silver \u2192 Gold (KPIs/aggregates)
\u2502   \u2514\u2500\u2500 quality/data_quality.py       # Reusable data quality framework (15+ checks)
\u251C\u2500\u2500 dbt/
\u2502   \u251C\u2500\u2500 dbt_project.yml
\u2502   \u251C\u2500\u2500 profiles.yml
\u2502   \u2514\u2500\u2500 models/
\u2502       \u251C\u2500\u2500 staging/stg_orders.sql
\u2502       \u251C\u2500\u2500 marts/fct_daily_sales.sql
\u2502       \u2514\u2500\u2500 marts/dim_customers.sql
\u251C\u2500\u2500 data/
\u2502   \u2514\u2500\u2500 generate_sample_data.py       # Generates 1M+ rows of realistic retail data
\u251C\u2500\u2500 tests/
\u2502   \u2514\u2500\u2500 test_transforms.py            # Unit tests for transformations
\u251C\u2500\u2500 docker-compose.yml
\u251C\u2500\u2500 requirements.txt
\u251C\u2500\u2500 Makefile
\u2514\u2500\u2500 README.md
```

---

## \U0001F680 Quick Start

```bash
# 1. Clone
git clone https://github.com/adityayadav97/retail-lakehouse-pipeline.git
cd retail-lakehouse-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate sample data (1M+ rows by default; use flags for a quick run)
python data/generate_sample_data.py                 # full 1M+ dataset
python data/generate_sample_data.py --orders 20000  # fast smoke run

# 4. Run the full pipeline locally (no Airflow needed)
make run-local

# 5. OR run the whole stack with Airflow + Docker
docker compose up -d
# Open Airflow at http://localhost:8080  (user: admin / pass: admin)
# Trigger the "retail_lakehouse_pipeline" DAG
```

---

## \U0001F50D Data Quality Framework (15+ checks)

Every layer transition passes through automated gates before data is promoted. The reusable framework in `src/quality/data_quality.py` provides:

* \u2705 **Row count / not-empty** \u2014 each dataset must contain rows
* \u2705 **Null checks** \u2014 critical columns must not be null (configurable threshold)
* \u2705 **Duplicate checks** \u2014 primary keys must be unique
* \u2705 **Schema validation** \u2014 expected columns and types present
* \u2705 **Range checks** \u2014 numeric values within valid bounds (e.g. price, quantity)
* \u2705 **Positive-value checks** \u2014 quantity / price must be > 0
* \u2705 **Accepted-values checks** \u2014 categoricals (region, category) within allowed set
* \u2705 **Referential integrity** \u2014 orders reference valid customers & products (no orphans)
* \u2705 **Freshness checks** \u2014 latest timestamp within an allowed age
* \u2705 **Delta-threshold reconciliation** \u2014 source vs target row variance within %

Applied across **Bronze \u2192 Silver \u2192 Gold**, the pipeline runs **15+ individual check invocations** per end-to-end run. If a gate fails, the pipeline stops and surfaces the exact failing rule \u2014 preventing bad data from reaching analytics consumers.

---

## \U0001F4CA Sample Output (Gold / Marts)

| metric | value |
| --- | --- |
| fct_daily_sales | daily revenue, orders, AOV per region |
| dim_customers | enriched customer dimension with lifetime value |

---

## \U0001F9EA Testing

```bash
pytest tests/ -v
```

---

## \U0001F4A1 Key Engineering Decisions

* **Delta Lake over plain Parquet** \u2014 enables ACID `MERGE`/`UPDATE`/`DELETE`, schema enforcement, and time travel for safe reprocessing.
* **Medallion architecture** \u2014 clean separation of raw, conformed, and business layers makes debugging and reprocessing trivial.
* **Idempotent writes** \u2014 each layer can be safely re-run without producing duplicates.
* **Quality gates as first-class steps** \u2014 data quality is enforced in the pipeline, not checked after the fact.

---

## \U0001F4DC License

MIT \u00a9 Aditya Yadav
