-- =====================================================================
-- OLTP source schema: Sistem Kasir Minimarket (Point-of-Sale)
-- Beginner level = 4 core tables.
-- This file runs automatically on first Postgres container start.
-- =====================================================================

-- Tabel: customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id     SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    phone           VARCHAR(20),
    email           VARCHAR(100),
    gender          VARCHAR(10),
    city            VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Tabel: products
CREATE TABLE IF NOT EXISTS products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(150) NOT NULL,
    category        VARCHAR(50),
    brand           VARCHAR(50),
    unit_price      NUMERIC(12,2) NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Tabel: transactions
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   SERIAL PRIMARY KEY,
    customer_id      INT REFERENCES customers(customer_id),
    store_id         INT,
    transaction_date TIMESTAMP NOT NULL,
    total_amount     NUMERIC(14,2) NOT NULL,
    payment_method   VARCHAR(30),
    status           VARCHAR(20) DEFAULT 'completed'
);

-- Tabel: transaction_items
CREATE TABLE IF NOT EXISTS transaction_items (
    item_id         SERIAL PRIMARY KEY,
    transaction_id  INT REFERENCES transactions(transaction_id),
    product_id      INT REFERENCES products(product_id),
    quantity        INT NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    discount        NUMERIC(5,2) DEFAULT 0,
    subtotal        NUMERIC(14,2) NOT NULL
);

-- Helpful indexes for the extract step
CREATE INDEX IF NOT EXISTS idx_txn_date     ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_txn_customer ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_items_txn    ON transaction_items(transaction_id);
CREATE INDEX IF NOT EXISTS idx_items_prod   ON transaction_items(product_id);
