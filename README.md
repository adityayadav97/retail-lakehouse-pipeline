# 🛒 Retail Lakehouse Pipeline

> An end-to-end, production-style **batch data lakehouse** that ingests raw retail data, processes it through a **Medallion architecture (Bronze → Silver → Gold)** using **PySpark + Delta Lake**, models analytics marts with **dbt**, and orchestrates everything with **Apache Airflow** — with built-in **data quality checks** at every layer.

<p align="left">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.10-3776AB?logo=python&logoColor=white">
  <img alt="Apache Spark" src="https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Delta Lake" src="https://img.shields.io/badge/Delta%20Lake-3.1-00ADD4">
  <img alt="dbt" src="https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white">
  <img alt="Airflow" src="https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white">
  <img alt="License" src="https://img.shields.io/badge/License-MIT-green">
</p>

---

## 📌 Why this project

This repository demonstrates how a real production data platform is built and operated — the same patterns I use day-to-day migrating and building pipelines at enterprise scale (Expedia, Atlassian, adidas).

It is **runnable end-to-end on a laptop** via Docker, yet mirrors how teams build on AWS EMR + S3 + Databricks in production.

---

## 🏗️ Architecture

```
                 ┌─────────────────────────────────────────────────────────────┐
                 │                     APACHE AIRFLOW (orchestration)            │
                 │   ingest → quality → silver → quality → gold → dbt → publish  │
                 └─────────────────────────────────────────────────────────────┘
                                │              │              │
        raw CSV / JSON          ▼              ▼              ▼
   ┌──────────────┐      ┌────────────┐  ┌────────────┐  ┌────────────┐      ┌──────────────┐
   │  Source data │ ───▶ │   BRONZE   │ ─▶│   SILVER   │ ─▶│    GOLD    │ ───▶ │  dbt marts   │
   │ orders, users│      │  (raw, as- │  │ (cleaned,  │  │ (business  │      │ fct_/dim_    │
   │  products    │      │   is Delta)│  │ conformed) │  │ aggregates)│      │  analytics   │
   └──────────────┘      └────────────┘  └────────────┘  └────────────┘      └──────────────┘
                                │              │              │
                                └──────── Data Quality gates (row counts, nulls, dupes, schema) ───────┘
```

**Medallion layers**

| Layer | Purpose | Tech |
|-------|---------|------|
| 🥉 Bronze | Raw ingestion, schema-on-read, append-only history | PySpark + Delta |
| 🥈 Silver | Cleaned, deduplicated, type-cast, conformed | PySpark + Delta |
| 🥇 Gold | Business-level aggregates and KPIs | PySpark + Delta |
| 📊 Marts | Analytics-ready dimensional models | dbt |

---

## 🧰 Tech Stack

- **Processing:** Apache Spark 3.5 (PySpark)
- **Storage Format:** Delta Lake (ACID, time travel, schema enforcement)
- **Transformation/Modeling:** dbt (staging → marts)
- **Orchestration:** Apache Airflow 2.8
- **Data Quality:** Custom validation framework (counts, null checks, duplicate checks, schema validation, delta-threshold checks)
- **Local Infra:** Docker Compose
- **Language:** Python 3.10, SQL

---

## 📂 Project Structure

```
retail-lakehouse-pipeline/
├── dags/
│   └── retail_pipeline_dag.py        # Airflow DAG orchestrating the full pipeline
├── src/
│   ├── config.py                     # Central config (paths, layer names)
│   ├── ingestion/bronze_ingest.py    # Raw → Bronze (Delta)
│   ├── transform/silver_transform.py # Bronze → Silver (clean/dedupe/conform)
│   ├── transform/gold_aggregate.py   # Silver → Gold (KPIs/aggregates)
│   └── quality/data_quality.py       # Reusable data quality framework
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/stg_orders.sql
│       ├── marts/fct_daily_sales.sql
│       └── marts/dim_customers.sql
├── data/
│   └── generate_sample_data.py       # Generates realistic sample retail data
├── tests/
│   └── test_transforms.py            # Unit tests for transformations
├── docker-compose.yml
├── requirements.txt
├── Makefile
└── README.md
```

---

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/adityayadav97/retail-lakehouse-pipeline.git
cd retail-lakehouse-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate sample data
python data/generate_sample_data.py

# 4. Run the full pipeline locally (no Airflow needed)
make run-local

# 5. OR run the whole stack with Airflow + Docker
docker compose up -d
# Open Airflow at http://localhost:8080  (user: admin / pass: admin)
# Trigger the "retail_lakehouse_pipeline" DAG
```

---

## 🔍 Data Quality Framework

Every layer transition passes through automated gates before data is promoted:

- ✅ **Row count reconciliation** — source vs target counts within threshold
- ✅ **Null checks** — critical columns must not be null
- ✅ **Duplicate checks** — primary keys must be unique
- ✅ **Schema validation** — expected columns and types present
- ✅ **Delta-threshold check** — row variance between runs stays within acceptable %

If a gate fails, the pipeline stops and surfaces the exact failing rule — preventing bad data from reaching analytics consumers.

---

## 📊 Sample Output (Gold / Marts)

| metric | value |
|--------|-------|
| `fct_daily_sales` | daily revenue, orders, AOV per region |
| `dim_customers` | enriched customer dimension with lifetime value |

---

## 🧪 Testing

```bash
pytest tests/ -v
```

---

## 💡 Key Engineering Decisions

- **Delta Lake over plain Parquet** — enables ACID `MERGE`/`UPDATE`/`DELETE`, schema enforcement, and time travel for safe reprocessing.
- **Medallion architecture** — clean separation of raw, conformed, and business layers makes debugging and reprocessing trivial.
- **Idempotent writes** — each layer can be safely re-run without producing duplicates.
- **Quality gates as first-class steps** — data quality is enforced in the pipeline, not checked after the fact.

---

## 📜 License

MIT © Aditya Yadav
