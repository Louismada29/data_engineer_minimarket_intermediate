"""
Multi-tenant seed generator (Intermediate).

Populates 3 PostgreSQL schemas (tenant_jakarta / tenant_bandung / tenant_surabaya),
each a regional franchise with its own stores, customers, products, promotions,
suppliers and ~8 months of sales.

`updated_at` is left to the column DEFAULT NOW() so it reflects load time — the
incremental EL uses it as the watermark, while analytics use transaction_date.

Run via:  docker compose exec -T airflow-scheduler python /opt/airflow/pipeline/seed_data_multitenant.py
"""

import json
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker("id_ID")

TENANT_CFG = os.getenv("TENANT_CONFIG", "/opt/airflow/pipeline/config/tenants.json")

# region -> list of cities used for that tenant's stores/customers
REGION_CITIES = {
    "jakarta":  ["Jakarta Pusat", "Jakarta Selatan", "Jakarta Barat", "Bekasi", "Depok"],
    "bandung":  ["Bandung", "Cimahi", "Sumedang", "Garut", "Tasikmalaya"],
    "surabaya": ["Surabaya", "Sidoarjo", "Gresik", "Malang", "Mojokerto"],
}
PROVINCE = {"jakarta": "DKI Jakarta", "bandung": "Jawa Barat", "surabaya": "Jawa Timur"}

GENDERS = ["Male", "Female"]
PAYMENT_METHODS = ["cash", "debit", "credit", "e-wallet"]
PAYMENT_WEIGHTS = [0.40, 0.22, 0.13, 0.25]
STORE_TYPES = ["minimarket", "supermarket", "express"]
PROMO_TYPES = ["discount", "bundle", "cashback"]

CATALOG = {
    "Beverages":   (["Aqua", "Teh Botol", "Pocari", "Coca-Cola", "Ultra"], (3500, 15000)),
    "Snacks":      (["Chitato", "Taro", "Lays", "Oreo", "Beng-Beng"],       (2000, 18000)),
    "Dairy":       (["Indomilk", "Frisian Flag", "Cimory", "Greenfields"],  (6000, 25000)),
    "Instant Food":(["Indomie", "Sedaap", "Pop Mie", "Sarimi"],             (2500, 9000)),
    "Personal Care":(["Lifebuoy", "Pepsodent", "Rexona", "Sunsilk"],        (8000, 35000)),
    "Household":   (["Rinso", "Sunlight", "Wipol", "Baygon"],               (10000, 45000)),
    "Tobacco":     (["Sampoerna", "Gudang Garam", "Marlboro"],              (20000, 40000)),
    "Frozen":      (["Fiesta", "So Good", "Champ", "Kanzler"],              (15000, 55000)),
}

# per-tenant volume
N_STORES = 4
N_SUPPLIERS = 5
N_CUSTOMERS = 150
N_PRODUCTS = 70
N_PROMOS = 8
N_TRANSACTIONS = 2500
MONTHS_BACK = 8


def money(x) -> Decimal:
    return Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "minimarket"),
        user=os.getenv("PG_USER", "minimarket"),
        password=os.getenv("PG_PASSWORD", "minimarket"),
    )


def seed_tenant(cur, tenant_id, schema, seed):
    rng = random.Random(seed)
    Faker.seed(seed)
    cities = REGION_CITIES[tenant_id]
    province = PROVINCE[tenant_id]

    print(f"  [{tenant_id}] truncating {schema} ...")
    cur.execute(
        f"TRUNCATE {schema}.transaction_promotions, {schema}.transaction_items, "
        f"{schema}.transactions, {schema}.promotions, {schema}.products, "
        f"{schema}.suppliers, {schema}.stores, {schema}.customers RESTART IDENTITY CASCADE;"
    )

    # stores
    stores = [(
        f"{tenant_id.capitalize()} Store {i+1}",
        rng.choice(cities), province, rng.choice(STORE_TYPES),
        fake.date_between(start_date="-5y", end_date="-1y"), True,
    ) for i in range(N_STORES)]
    execute_values(cur,
        f"INSERT INTO {schema}.stores (store_name, city, province, store_type, opened_at, is_active) VALUES %s",
        stores)

    # suppliers
    suppliers = [(
        fake.company(), fake.name(), rng.choice(cities), "Indonesia",
    ) for _ in range(N_SUPPLIERS)]
    execute_values(cur,
        f"INSERT INTO {schema}.suppliers (supplier_name, contact_name, city, country) VALUES %s",
        suppliers)

    # customers
    customers = [(
        fake.name(), fake.msisdn()[:13], fake.email(),
        rng.choice(GENDERS), rng.choice(cities),
    ) for _ in range(N_CUSTOMERS)]
    execute_values(cur,
        f"INSERT INTO {schema}.customers (name, phone, email, gender, city) VALUES %s",
        customers)

    # products
    products = []
    for _ in range(N_PRODUCTS):
        category = rng.choice(list(CATALOG.keys()))
        brands, (lo, hi) = CATALOG[category]
        brand = rng.choice(brands)
        price = round(rng.uniform(lo, hi) / 500) * 500
        products.append((
            f"{brand} {fake.word().capitalize()}".strip(), category, brand,
            money(price), rng.random() > 0.05,
        ))
    execute_values(cur,
        f"INSERT INTO {schema}.products (product_name, category, brand, unit_price, is_active) VALUES %s",
        products)

    # promotions
    promos = []
    for i in range(N_PROMOS):
        start = fake.date_between(start_date=f"-{MONTHS_BACK}M", end_date="-1M") \
            if False else (datetime.now() - timedelta(days=rng.randint(60, MONTHS_BACK * 30))).date()
        end = start + timedelta(days=rng.randint(14, 45))
        promos.append((
            f"Promo {rng.choice(['Gajian','Akhir Pekan','Hari Belanja','Spesial'])} {i+1}",
            rng.choice(PROMO_TYPES), money(rng.choice([5, 10, 15, 20, 25])),
            start, end, money(rng.choice([0, 25000, 50000, 100000])),
        ))
    execute_values(cur,
        f"INSERT INTO {schema}.promotions (promo_name, promo_type, discount_pct, start_date, end_date, min_purchase) VALUES %s",
        promos)

    # fetch generated ids
    cur.execute(f"SELECT customer_id FROM {schema}.customers;")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute(f"SELECT store_id FROM {schema}.stores;")
    store_ids = [r[0] for r in cur.fetchall()]
    cur.execute(f"SELECT product_id, unit_price FROM {schema}.products WHERE is_active;")
    product_rows = cur.fetchall()
    cur.execute(f"SELECT promo_id, discount_pct FROM {schema}.promotions;")
    promo_rows = cur.fetchall()

    # transactions + items
    start = datetime.now() - timedelta(days=MONTHS_BACK * 30)
    txns, items_per_txn = [], []
    for _ in range(N_TRANSACTIONS):
        days_offset = int(rng.triangular(0, MONTHS_BACK * 30, MONTHS_BACK * 30 * 0.8))
        txn_date = start + timedelta(days=days_offset, hours=rng.randint(7, 21), minutes=rng.randint(0, 59))
        status = rng.choices(["completed", "cancelled", "pending"], weights=[0.9, 0.06, 0.04])[0]
        payment = rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]

        chosen = rng.sample(product_rows, min(rng.randint(1, 6), len(product_rows)))
        total = Decimal("0.00")
        buf = []
        for pid, unit_price in chosen:
            qty = rng.randint(1, 5)
            disc_pct = rng.choice([0, 0, 0, 5, 10, 15])
            gross = money(Decimal(unit_price) * qty)
            discount = money(gross * Decimal(disc_pct) / Decimal(100))
            subtotal = money(gross - discount)
            total += subtotal
            buf.append((pid, qty, money(unit_price), money(disc_pct), subtotal))

        txns.append((rng.choice(customer_ids), rng.choice(store_ids), txn_date, total, payment, status))
        items_per_txn.append(buf)

    execute_values(cur,
        f"INSERT INTO {schema}.transactions (customer_id, store_id, transaction_date, total_amount, payment_method, status) VALUES %s",
        txns)
    cur.execute(f"SELECT transaction_id, total_amount FROM {schema}.transactions ORDER BY transaction_id;")
    txn_rows = cur.fetchall()
    txn_ids = [r[0] for r in txn_rows]

    flat_items = []
    for tid, buf in zip(txn_ids, items_per_txn):
        for pid, qty, unit_price, disc, subtotal in buf:
            flat_items.append((tid, pid, qty, unit_price, disc, subtotal))
    execute_values(cur,
        f"INSERT INTO {schema}.transaction_items (transaction_id, product_id, quantity, unit_price, discount, subtotal) VALUES %s",
        flat_items, page_size=1000)

    # transaction_promotions: ~30% of transactions get 1 promo
    txn_promos = []
    for tid, total in txn_rows:
        if rng.random() < 0.30:
            promo_id, disc_pct = rng.choice(promo_rows)
            applied = money(Decimal(total) * Decimal(disc_pct) / Decimal(100))
            txn_promos.append((tid, promo_id, applied))
    if txn_promos:
        execute_values(cur,
            f"INSERT INTO {schema}.transaction_promotions (transaction_id, promo_id, discount_applied) VALUES %s",
            txn_promos)

    return len(customer_ids), len(txn_ids), len(flat_items), len(txn_promos)


def main():
    with open(TENANT_CFG) as fh:
        tenants = json.load(fh)["tenants"]

    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()

    print("Seeding multi-tenant source...")
    for i, t in enumerate(tenants):
        c, tx, it, tp = seed_tenant(cur, t["tenant_id"], t["schema"], seed=100 + i)
        print(f"  [{t['tenant_id']:9s}] customers={c} transactions={tx} items={it} txn_promos={tp}")

    conn.commit()
    cur.close()
    conn.close()
    print("Multi-tenant seed complete.")


if __name__ == "__main__":
    main()
