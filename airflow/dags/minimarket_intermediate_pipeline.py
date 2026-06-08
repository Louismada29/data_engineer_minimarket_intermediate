"""
minimarket_intermediate_pipeline — Intermediate ELT DAG.

Flow:  extract_load_mt  ->  dbt_run_mt  ->  dbt_test_mt

- extract_load_mt : concurrent (per-tenant threads) incremental Python EL,
                    PostgreSQL (3 tenant schemas) -> ClickHouse `raw_mt`.
- dbt_run_mt      : builds the intermediate staging + marts (tag:intermediate),
                    i.e. multi-tenant star schema + fct_sales + fct_promotion_usage.
- dbt_test_mt     : runs the intermediate dbt tests (data quality).

dbt runs from the isolated venv at /opt/dbt-venv.
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
    dag_id="minimarket_intermediate_pipeline",
    description="Multi-tenant incremental ELT: PostgreSQL (3 tenants) -> ClickHouse -> dbt",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["minimarket", "elt", "intermediate", "multi-tenant"],
) as dag:

    extract_load_mt = BashOperator(
        task_id="extract_load_mt",
        bash_command="python /opt/airflow/pipeline/extract_load_multitenant.py",
    )

    dbt_run_mt = BashOperator(
        task_id="dbt_run_mt",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} run --select tag:intermediate --profiles-dir {DBT_DIR}"
        ),
    )

    dbt_test_mt = BashOperator(
        task_id="dbt_test_mt",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} test --select tag:intermediate --profiles-dir {DBT_DIR}"
        ),
    )

    extract_load_mt >> dbt_run_mt >> dbt_test_mt
