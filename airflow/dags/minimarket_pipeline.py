"""
minimarket_pipeline — single end-to-end ELT DAG (Beginner level).

Flow:  extract_load  ->  dbt_run  ->  dbt_test

- extract_load : Python full-load from PostgreSQL into ClickHouse `raw`.
- dbt_run      : builds staging (views) + marts (star schema tables).
- dbt_test     : runs schema tests (not_null, unique, relationships, ...).

dbt runs from an ISOLATED virtualenv (/opt/dbt-venv) so its dependencies
never clash with Airflow's. Connection settings come from the container env.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt/minimarket"
DBT_BIN = "/opt/dbt-venv/bin/dbt"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="minimarket_pipeline",
    description="ELT: PostgreSQL -> ClickHouse -> dbt star schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",   # full-load; runs daily but is idempotent
    catchup=False,
    tags=["minimarket", "elt", "beginner"],
) as dag:

    extract_load = BashOperator(
        task_id="extract_load",
        bash_command="python /opt/airflow/pipeline/extract_load.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} run --select tag:beginner --profiles-dir {DBT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} test --select tag:beginner --profiles-dir {DBT_DIR}"
        ),
    )

    extract_load >> dbt_run >> dbt_test
