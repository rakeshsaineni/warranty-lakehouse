"""Airflow DAG: CDC -> bronze -> silver -> dbt build -> reconciliation, every 15 minutes.

Each task is idempotent (bronze batches are keyed by run id; silver is rebuilt from bronze;
dbt models are deterministic), so retries and backfills are safe."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DB = "/opt/airflow/data/warehouse.duckdb"


@dag(schedule="*/15 * * * *", start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
     tags=["warranty", "lakehouse"])
def warranty_lakehouse():
    @task
    def ingest_cdc_to_bronze(run_id: str | None = None) -> dict:
        from pathlib import Path

        from lakehouse.kafka import read_batch  # consumer for the Debezium topics
        from lakehouse.pipeline import load_contracts, to_bronze

        return to_bronze(read_batch(), Path("/opt/airflow/data"), load_contracts(), batch_id=run_id or "manual")

    @task
    def build_silver(_: dict) -> dict:
        from pathlib import Path

        from lakehouse.pipeline import build_silver as build

        return build(Path("/opt/airflow/data"), Path(DB))

    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"cd /opt/airflow/dbt && LAKEHOUSE_DB={DB} "
                             "dbt build --profiles-dir .")

    @task
    def quarantine_report() -> None:
        import duckdb

        n = duckdb.sql("SELECT count(*) FROM read_parquet('/opt/airflow/data/quarantine/*.parquet')").fetchone()[0]
        if n:
            print(f"{n} events quarantined by data contracts; see quarantine/")

    silver = build_silver(ingest_cdc_to_bronze())
    silver >> dbt_build >> quarantine_report()


warranty_lakehouse()
