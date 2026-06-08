"""
FastAPI data service for the Intermediate dashboard.

Exposes 5 endpoints that query the dbt marts in ClickHouse and answer the
5 analytical questions. Also serves the static dashboard at "/".

Endpoints:
  GET /api/revenue-by-store        -> Q1 revenue per store per month (last 6 months)
  GET /api/promotion-effectiveness -> Q2 top promos + avg txn value promo vs non-promo
  GET /api/top-products-by-city    -> Q3 top 3 products per city by revenue
  GET /api/customer-segments       -> Q4 High/Medium/Low spender counts per city
  GET /api/transactions-by-day     -> Q5 transactions & revenue by day of week
"""

import os
from decimal import Decimal
from pathlib import Path

import clickhouse_connect
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

app = FastAPI(title="Minimarket Analytics API", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

DB = os.getenv("CLICKHOUSE_DB", "analytics")
STATIC_DIR = Path(__file__).parent / "static"


def client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def run(sql: str) -> list[dict]:
    """Execute SQL and return list of dicts with Decimals cast to float."""
    res = client().query(sql)
    cols = res.column_names
    out = []
    for row in res.result_rows:
        d = {}
        for c, v in zip(cols, row):
            d[c] = float(v) if isinstance(v, Decimal) else v
        out.append(d)
    return out


@app.get("/")
def dashboard():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/revenue-by-store")
def revenue_by_store():
    rows = run(f"""
        SELECT s.store_name AS store, d.year_month AS month, sum(f.subtotal) AS revenue
        FROM {DB}.fct_sales f
        JOIN {DB}.dim_store s   ON f.tenant_id = s.tenant_id AND f.store_id = s.store_id
        JOIN {DB}.dim_date_mt d ON f.transaction_day = d.date_day
        GROUP BY store, month
        ORDER BY month, store
    """)
    months = sorted({r["month"] for r in rows})
    last6 = months[-6:]
    stores = sorted({r["store"] for r in rows})
    lookup = {(r["store"], r["month"]): r["revenue"] for r in rows}
    series = [
        {"store": s, "data": [lookup.get((s, m), 0) for m in last6]}
        for s in stores
    ]
    return {"months": last6, "series": series}


@app.get("/api/promotion-effectiveness")
def promotion_effectiveness():
    top_promos = run(f"""
        SELECT p.promo_name AS promo, sum(u.discount_applied) AS total_discount
        FROM {DB}.fct_promotion_usage u
        JOIN {DB}.dim_promotion p ON u.tenant_id = p.tenant_id AND u.promo_id = p.promo_id
        GROUP BY promo
        ORDER BY total_discount DESC
        LIMIT 10
    """)
    comparison = run(f"""
        SELECT grp, round(avg(val), 0) AS avg_txn_value, count() AS n_transactions
        FROM (
            SELECT t.tenant_id, t.transaction_id, sum(t.subtotal) AS val,
                multiIf(
                    (t.tenant_id, t.transaction_id) IN
                        (SELECT tenant_id, transaction_id FROM {DB}.fct_promotion_usage),
                    'with_promo', 'no_promo') AS grp
            FROM {DB}.fct_sales t
            GROUP BY t.tenant_id, t.transaction_id
        )
        GROUP BY grp
        ORDER BY grp
    """)
    return {"top_promos": top_promos, "comparison": comparison}


@app.get("/api/top-products-by-city")
def top_products_by_city():
    rows = run(f"""
        SELECT city, product_name, revenue FROM (
            SELECT city, product_name, revenue,
                   row_number() OVER (PARTITION BY city ORDER BY revenue DESC) AS rn
            FROM (
                SELECT s.city AS city, pr.product_name AS product_name, sum(f.subtotal) AS revenue
                FROM {DB}.fct_sales f
                JOIN {DB}.dim_store s       ON f.tenant_id = s.tenant_id AND f.store_id = s.store_id
                JOIN {DB}.dim_product_mt pr ON f.tenant_id = pr.tenant_id AND f.product_id = pr.product_id
                GROUP BY city, product_name
            )
        )
        WHERE rn <= 3
        ORDER BY city, revenue DESC
    """)
    return {"rows": rows}


@app.get("/api/customer-segments")
def customer_segments():
    rows = run(f"""
        WITH cust AS (
            SELECT tenant_id, customer_id, sum(subtotal) AS total_spend
            FROM {DB}.fct_sales GROUP BY tenant_id, customer_id
        ),
        q AS (
            SELECT quantile(0.33)(total_spend) AS q1, quantile(0.66)(total_spend) AS q2 FROM cust
        )
        SELECT c.city AS city,
               multiIf(cust.total_spend >= q.q2, 'High',
                       cust.total_spend >= q.q1, 'Medium', 'Low') AS segment,
               count() AS n_customers
        FROM cust
        JOIN {DB}.dim_customer_mt c ON cust.tenant_id = c.tenant_id AND cust.customer_id = c.customer_id
        CROSS JOIN q
        GROUP BY city, segment
        ORDER BY city, segment
    """)
    return {"rows": rows}


@app.get("/api/transactions-by-day")
def transactions_by_day():
    rows = run(f"""
        SELECT d.day_of_week AS dow, d.day_name AS day_name,
               uniqExact(f.tenant_id, f.transaction_id) AS n_transactions,
               sum(f.subtotal) AS revenue
        FROM {DB}.fct_sales f
        JOIN {DB}.dim_date_mt d ON f.transaction_day = d.date_day
        GROUP BY dow, day_name
        ORDER BY dow
    """)
    return {"rows": rows}
