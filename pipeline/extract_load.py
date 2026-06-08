"""
Extract & Load (EL) — full load, single tenant.

Reads the 4 OLTP tables from PostgreSQL and loads them into a `raw`
database in ClickHouse. dbt then transforms `raw.*` -> staging -> marts.

Strategy: FULL LOAD. Each run drops & recreates the raw table, then
bulk-inserts the current snapshot. Simple and correct for the beginner
scope (no incremental/watermark required).

Run standalone:  python pipeline/extract_load.py
Run in Airflow:  invoked by the `extract_load` task (PythonOperator).
"""

import logging
import os
import sys

import clickhouse_connect
import pandas as pd
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | EL | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extract_load")

RAW_DB = os.getenv("CLICKHOUSE_RAW_DB", "raw")

# Source tables and the ClickHouse DDL for their raw landing tables.
# Nullable() mirrors the OLTP nullability; money kept as Decimal for accuracy.
TABLES = {
    "customers": """
        CREATE TABLE {db}.customers (
            customer_id  Int64,
            name         String,
            phone        Nullable(String),
            email        Nullable(String),
            gender       Nullable(String),
            city         Nullable(String),
            created_at   Nullable(DateTime64(0))
        ) ENGINE = MergeTree ORDER BY customer_id
    """,
    "products": """
        CREATE TABLE {db}.products (
            product_id   Int64,
            product_name String,
            category     Nullable(String),
            brand        Nullable(String),
            unit_price   Decimal(12, 2),
            is_active    Nullable(UInt8),
            created_at   Nullable(DateTime64(0))
        ) ENGINE = MergeTree ORDER BY product_id
    """,
    "transactions": """
        CREATE TABLE {db}.transactions (
            transaction_id   Int64,
            customer_id      Nullable(Int64),
            store_id         Nullable(Int64),
            transaction_date DateTime64(0),
            total_amount     Decimal(14, 2),
            payment_method   Nullable(String),
            status           Nullable(String)
        ) ENGINE = MergeTree ORDER BY transaction_id
    """,
    "transaction_items": """
        CREATE TABLE {db}.transaction_items (
            item_id        Int64,
            transaction_id Nullable(Int64),
            product_id     Nullable(Int64),
            quantity       Int32,
            unit_price     Decimal(12, 2),
            discount       Decimal(5, 2),
            subtotal       Decimal(14, 2)
        ) ENGINE = MergeTree ORDER BY item_id
    """,
}


def pg_connection():
    # Raw psycopg2 connection. We avoid SQLAlchemy on purpose: the Airflow
    # image pins SQLAlchemy 1.4, which clashes with pandas' read_sql in this
    # environment. pandas works fine reading from a DBAPI2 (psycopg2) conn.
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "minimarket"),
        user=os.getenv("PG_USER", "minimarket"),
        password=os.getenv("PG_PASSWORD", "minimarket"),
    )


def ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def main():
    log.info("Starting full-load EL: PostgreSQL -> ClickHouse[%s]", RAW_DB)
    src = pg_connection()
    ch = ch_client()

    ch.command(f"CREATE DATABASE IF NOT EXISTS {RAW_DB}")
    log.info("Ensured ClickHouse database `%s` exists", RAW_DB)

    total_rows = 0
    for table, ddl in TABLES.items():
        log.info("[%s] extracting from Postgres...", table)
        df = pd.read_sql(f"SELECT * FROM {table}", src)
        log.info("[%s] extracted %d rows", table, len(df))

        # bool -> UInt8 so ClickHouse is happy
        if "is_active" in df.columns:
            df["is_active"] = df["is_active"].astype("Int64")

        log.info("[%s] (re)creating raw table & loading...", table)
        ch.command(f"DROP TABLE IF EXISTS {RAW_DB}.{table}")
        ch.command(ddl.format(db=RAW_DB))
        if len(df):
            ch.insert_df(f"{RAW_DB}.{table}", df)

        loaded = ch.query(f"SELECT count() FROM {RAW_DB}.{table}").result_rows[0][0]
        log.info("[%s] loaded %d rows into ClickHouse", table, loaded)
        total_rows += loaded

    src.close()
    log.info("EL finished successfully. Total rows loaded: %d", total_rows)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        log.exception("EL failed: %s", exc)
        sys.exit(1)
