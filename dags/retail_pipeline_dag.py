"""Apache Airflow DAG orchestrating the Retail Lakehouse Pipeline.

Flow:  ingest (bronze) → silver → gold → dbt build → done
Each stage runs as an isolated task with retries and clear dependencies.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "aditya",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

with DAG(
    dag_id="retail_lakehouse_pipeline",
    description="End-to-end Medallion lakehouse: Bronze → Silver → Gold → dbt",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # daily at 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "pyspark", "delta", "dbt"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command="cd /opt/project && python -m src.ingestion.bronze_ingest",
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command="cd /opt/project && python -m src.transform.silver_transform",
    )

    gold = BashOperator(
        task_id="gold_aggregate",
        bash_command="cd /opt/project && python -m src.transform.gold_aggregate",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/project/dbt && dbt build --profiles-dir .",
    )

    bronze >> silver >> gold >> dbt_build
