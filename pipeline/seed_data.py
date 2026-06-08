"""
Seed generator for the minimarket OLTP database.

Generates ~12 months of realistic Point-of-Sale data so the analytics
charts (monthly trend, top products, payment-method mix) have signal.

Run via: scripts/run_seed.sh   (executes this inside the airflow container)
Idempotent: it TRUNCATEs the 4 tables first, then re-seeds.
"""

import os
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker("id_ID")
random.seed(42)
Faker.seed(42)

# --- volume knobs ---------------------------------------------------------
N_CUSTOMERS = 300
N_PRODUCTS = 90
N_TRANSACTIONS = 6000
MONTHS_BACK = 12

CITIES = ["Jakarta", "Bandung", "Surabaya", "Medan", "Semarang",
          "Makassar", "Yogyakarta", "Denpasar"]
GENDERS = ["Male", "Female"]
PAYMENT_METHODS = ["cash", "debit", "credit", "e-wallet"]
PAYMENT_WEIGHTS = [0.40, 0.22, 0.13, 0.25]  # cash still dominates minimarkets

# category -> (brands, price range in IDR)
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


def main():
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()

    print("Truncating existing data...")
    cur.execute(
        "TRUNCATE transaction_items, transactions, products, customers "
        "RESTART IDENTITY CASCADE;"
    )

    # ----- customers ------------------------------------------------------
    print(f"Seeding {N_CUSTOMERS} customers...")
    cust_start = datetime.now() - timedelta(days=(MONTHS_BACK + 2) * 30)
    cust_window = (datetime.now() - cust_start).days
    customers = []
    for _ in range(N_CUSTOMERS):
        created = cust_start + timedelta(
            days=random.randint(0, cust_window),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        customers.append((
            fake.name(),
            fake.msisdn()[:13],
            fake.email(),
            random.choice(GENDERS),
            random.choice(CITIES),
            created,
        ))
    execute_values(
        cur,
        "INSERT INTO customers (name, phone, email, gender, city, created_at) VALUES %s",
        customers,
    )

    # ----- products -------------------------------------------------------
    print(f"Seeding {N_PRODUCTS} products...")
    products = []
    for _ in range(N_PRODUCTS):
        category = random.choice(list(CATALOG.keys()))
        brands, (lo, hi) = CATALOG[category]
        brand = random.choice(brands)
        price = round(random.uniform(lo, hi) / 500) * 500  # round to nearest 500
        products.append((
            f"{brand} {fake.word().capitalize()} {random.choice(['', '250ml', '500ml', 'Pack', 'Refill'])}".strip(),
            category,
            brand,
            money(price),
            random.random() > 0.05,  # 95% active
        ))
    execute_values(
        cur,
        "INSERT INTO products (product_name, category, brand, unit_price, is_active) VALUES %s",
        products,
    )

    # fetch generated ids + prices to build transactions
    cur.execute("SELECT customer_id FROM customers;")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, unit_price FROM products WHERE is_active;")
    product_rows = cur.fetchall()

    # ----- transactions + items ------------------------------------------
    print(f"Seeding {N_TRANSACTIONS} transactions (+items)...")
    start = datetime.now() - timedelta(days=MONTHS_BACK * 30)

    txns, items = [], []
    for _ in range(N_TRANSACTIONS):
        # skew dates toward recent months a little for a believable upward trend
        days_offset = int(random.triangular(0, MONTHS_BACK * 30, MONTHS_BACK * 30 * 0.75))
        txn_date = start + timedelta(days=days_offset,
                                     hours=random.randint(7, 21),
                                     minutes=random.randint(0, 59))
        # 90% completed, rest cancelled/pending (filtered out later in dbt)
        status = random.choices(["completed", "cancelled", "pending"], weights=[0.9, 0.06, 0.04])[0]
        payment = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
        customer_id = random.choice(customer_ids)
        store_id = random.randint(1, 5)

        n_lines = random.randint(1, 6)
        chosen = random.sample(product_rows, min(n_lines, len(product_rows)))
        total = Decimal("0.00")
        line_buffer = []
        for pid, unit_price in chosen:
            qty = random.randint(1, 5)
            disc_pct = random.choice([0, 0, 0, 5, 10, 15])  # most lines no discount
            gross = money(Decimal(unit_price) * qty)
            discount = money(gross * Decimal(disc_pct) / Decimal(100))
            subtotal = money(gross - discount)
            total += subtotal
            line_buffer.append((pid, qty, money(unit_price), money(disc_pct), subtotal))

        txns.append((customer_id, store_id, txn_date, total, payment, status))
        # transaction_id is assigned after insert; keep line_buffer aligned by order
        items.append(line_buffer)

    execute_values(
        cur,
        "INSERT INTO transactions "
        "(customer_id, store_id, transaction_date, total_amount, payment_method, status) "
        "VALUES %s",
        txns,
    )

    # transactions got sequential ids starting at 1 (RESTART IDENTITY)
    cur.execute("SELECT transaction_id FROM transactions ORDER BY transaction_id;")
    txn_ids = [r[0] for r in cur.fetchall()]

    flat_items = []
    for txn_id, line_buffer in zip(txn_ids, items):
        for pid, qty, unit_price, disc, subtotal in line_buffer:
            flat_items.append((txn_id, pid, qty, unit_price, disc, subtotal))

    print(f"Seeding {len(flat_items)} transaction_items...")
    execute_values(
        cur,
        "INSERT INTO transaction_items "
        "(transaction_id, product_id, quantity, unit_price, discount, subtotal) "
        "VALUES %s",
        flat_items,
        page_size=1000,
    )

    conn.commit()

    # quick summary
    for tbl in ["customers", "products", "transactions", "transaction_items"]:
        cur.execute(f"SELECT count(*) FROM {tbl};")
        print(f"  {tbl:20s}: {cur.fetchone()[0]:>7,} rows")

    cur.close()
    conn.close()
    print("Seed complete.")


if __name__ == "__main__":
    main()
