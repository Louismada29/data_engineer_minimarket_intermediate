"""
Extract & Load (EL) — Intermediate: multi-tenant, concurrent, incremental.

Mirrors the required Golang pattern in Python:
  - Golang goroutine            -> ThreadPoolExecutor worker (one per tenant)
  - sync.WaitGroup / wg.Wait()  -> `with ThreadPoolExecutor(...)` join on exit
Work is I/O-bound (DB round-trips), so threads run effectively in parallel.

Multi-tenant : 3 PostgreSQL schemas, processed in parallel.
Incremental  : per-(tenant, table) high-watermark on `updated_at`, persisted to
               a JSON file. Each run only pulls rows newer than the watermark.
Idempotent   : raw tables are ReplacingMergeTree(updated_at) keyed on
               (tenant_id, pk), so re-loaded/updated rows collapse to the latest.

Run standalone:  python pipeline/extract_load_multitenant.py
Run in Airflow:  the `extract_load_mt` task.
"""

import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import clickhouse_connect
import pandas as pd
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | EL-MT | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extract_load_mt")

RAW_DB = os.getenv("CLICKHOUSE_RAW_MT_DB", "raw_mt")
TENANT_CFG = os.getenv("TENANT_CONFIG", "/opt/airflow/pipeline/config/tenants.json")
WATERMARK_FILE = os.getenv("WATERMARK_FILE", "/opt/airflow/state/watermarks.json")
EPOCH = "1970-01-01 00:00:00"

# table -> (primary key, [(col, clickhouse_type), ...])  -- source column order
TABLES = {
    "customers": ("customer_id", [
        ("customer_id", "Int64"), ("name", "String"), ("phone", "Nullable(String)"),
        ("email", "Nullable(String)"), ("gender", "Nullable(String)"), ("city", "Nullable(String)"),
        ("created_at", "Nullable(DateTime64(0))"), ("updated_at", "DateTime64(0)"),
    ]),
    "stores": ("store_id", [
        ("store_id", "Int64"), ("store_name", "String"), ("city", "Nullable(String)"),
        ("province", "Nullable(String)"), ("store_type", "Nullable(String)"),
        ("opened_at", "Nullable(Date)"), ("is_active", "Nullable(UInt8)"), ("updated_at", "DateTime64(0)"),
    ]),
    "suppliers": ("supplier_id", [
        ("supplier_id", "Int64"), ("supplier_name", "Nullable(String)"), ("contact_name", "Nullable(String)"),
        ("city", "Nullable(String)"), ("country", "Nullable(String)"),
        ("created_at", "Nullable(DateTime64(0))"), ("updated_at", "DateTime64(0)"),
    ]),
    "products": ("product_id", [
        ("product_id", "Int64"), ("product_name", "String"), ("category", "Nullable(String)"),
        ("brand", "Nullable(String)"), ("unit_price", "Decimal(12,2)"), ("is_active", "Nullable(UInt8)"),
        ("created_at", "Nullable(DateTime64(0))"), ("updated_at", "DateTime64(0)"),
    ]),
    "promotions": ("promo_id", [
        ("promo_id", "Int64"), ("promo_name", "Nullable(String)"), ("promo_type", "Nullable(String)"),
        ("discount_pct", "Nullable(Decimal(5,2))"), ("start_date", "Nullable(Date)"),
        ("end_date", "Nullable(Date)"), ("min_purchase", "Nullable(Decimal(12,2))"),
        ("updated_at", "DateTime64(0)"),
    ]),
    "transactions": ("transaction_id", [
        ("transaction_id", "Int64"), ("customer_id", "Nullable(Int64)"), ("store_id", "Nullable(Int64)"),
        ("transaction_date", "DateTime64(0)"), ("total_amount", "Decimal(14,2)"),
        ("payment_method", "Nullable(String)"), ("status", "Nullable(String)"), ("updated_at", "DateTime64(0)"),
    ]),
    "transaction_items": ("item_id", [
        ("item_id", "Int64"), ("transaction_id", "Nullable(Int64)"), ("product_id", "Nullable(Int64)"),
        ("quantity", "Int32"), ("unit_price", "Decimal(12,2)"), ("discount", "Decimal(5,2)"),
        ("subtotal", "Decimal(14,2)"), ("updated_at", "DateTime64(0)"),
    ]),
    "transaction_promotions": ("id", [
        ("id", "Int64"), ("transaction_id", "Nullable(Int64)"), ("promo_id", "Nullable(Int64)"),
        ("discount_applied", "Nullable(Decimal(12,2))"), ("updated_at", "DateTime64(0)"),
    ]),
}


def pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"), port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "minimarket"), user=os.getenv("PG_USER", "minimarket"),
        password=os.getenv("PG_PASSWORD", "minimarket"),
    )


def ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def ensure_tables(ch):
    """Create the raw_mt database + tables once (idempotent)."""
    ch.command(f"CREATE DATABASE IF NOT EXISTS {RAW_DB}")
    for table, (pk, cols) in TABLES.items():
        col_defs = ",\n  ".join([f"tenant_id String"] + [f"{c} {t}" for c, t in cols])
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {RAW_DB}.{table} (\n  {col_defs}\n) "
            f"ENGINE = ReplacingMergeTree(updated_at) ORDER BY (tenant_id, {pk})"
        )
        ch.command(ddl)


def load_watermarks() -> dict:
    if os.path.exists(WATERMARK_FILE):
        try:
            with open(WATERMARK_FILE) as fh:
                return json.load(fh)
        except json.JSONDecodeError:
            log.warning("Watermark file unreadable, starting fresh")
    return {}


def save_watermarks(wm: dict):
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, "w") as fh:
        json.dump(wm, fh, indent=2)


def process_tenant(tenant: dict, tenant_wm: dict) -> tuple:
    """Runs in its own thread. Returns (tenant_id, new_watermarks, rows_loaded)."""
    tid, schema = tenant["tenant_id"], tenant["schema"]
    log.info("[%s] start (schema=%s)", tid, schema)
    pg = pg_conn()
    ch = ch_client()
    new_wm = dict(tenant_wm)
    rows_total = 0

    try:
        for table, (pk, cols) in TABLES.items():
            since = tenant_wm.get(table, EPOCH)
            df = pd.read_sql(
                f"SELECT * FROM {schema}.{table} WHERE updated_at > %(since)s",
                pg, params={"since": since},
            )
            if "is_active" in df.columns:
                df["is_active"] = df["is_active"].astype("Int64")

            if len(df):
                df.insert(0, "tenant_id", tid)
                ch.insert_df(f"{RAW_DB}.{table}", df, column_names=list(df.columns))
                new_max = pd.to_datetime(df["updated_at"]).max()
                new_wm[table] = new_max.strftime("%Y-%m-%d %H:%M:%S")
                rows_total += len(df)
            log.info("[%s] %-22s +%d rows (since %s)", tid, table, len(df), since)

        return tid, new_wm, rows_total
    finally:
        pg.close()


def main():
    with open(TENANT_CFG) as fh:
        tenants = json.load(fh)["tenants"]

    ch = ch_client()
    ensure_tables(ch)
    log.info("Ensured %s.* tables exist (%d tables)", RAW_DB, len(TABLES))

    watermarks = load_watermarks()
    results = {}

    # --- concurrent fan-out: one worker per tenant (goroutine equivalent) ---
    with ThreadPoolExecutor(max_workers=len(tenants)) as pool:
        futures = {
            pool.submit(process_tenant, t, watermarks.get(t["tenant_id"], {})): t["tenant_id"]
            for t in tenants
        }
        for fut in as_completed(futures):
            tid, new_wm, rows = fut.result()  # re-raises worker exceptions
            results[tid] = new_wm
            log.info("[%s] done: %d rows loaded", tid, rows)
    # pool exit == sync.WaitGroup.Wait()

    watermarks.update(results)
    save_watermarks(watermarks)
    log.info("Watermarks persisted to %s", WATERMARK_FILE)
    log.info("Multi-tenant EL finished successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        log.exception("Multi-tenant EL failed: %s", exc)
        sys.exit(1)
