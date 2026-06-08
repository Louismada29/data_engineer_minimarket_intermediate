-- =====================================================================
-- INTERMEDIATE: Multi-tenant OLTP source.
-- 3 tenants simulated as 3 PostgreSQL schemas, each a regional franchise
-- with its own stores, customers, products, promotions, and sales.
-- Every table carries updated_at to support incremental (watermark) loads.
-- Runs automatically on first Postgres container start.
-- =====================================================================

-- ---- tenant_jakarta ----
CREATE SCHEMA IF NOT EXISTS tenant_jakarta;

CREATE TABLE IF NOT EXISTS tenant_jakarta.customers (
    customer_id  SERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    phone        VARCHAR(20),
    email        VARCHAR(100),
    gender       VARCHAR(10),
    city         VARCHAR(50),
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_jakarta.stores (
    store_id     SERIAL PRIMARY KEY,
    store_name   VARCHAR(100) NOT NULL,
    city         VARCHAR(50),
    province     VARCHAR(50),
    store_type   VARCHAR(30),
    opened_at    DATE,
    is_active    BOOLEAN DEFAULT TRUE,
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_jakarta.suppliers (
    supplier_id  SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100),
    contact_name VARCHAR(100),
    city         VARCHAR(50),
    country      VARCHAR(50) DEFAULT 'Indonesia',
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_jakarta.products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category     VARCHAR(50),
    brand        VARCHAR(50),
    unit_price   NUMERIC(12,2) NOT NULL,
    is_active    BOOLEAN DEFAULT TRUE,
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_jakarta.promotions (
    promo_id     SERIAL PRIMARY KEY,
    promo_name   VARCHAR(100),
    promo_type   VARCHAR(30),
    discount_pct NUMERIC(5,2),
    start_date   DATE,
    end_date     DATE,
    min_purchase NUMERIC(12,2) DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_jakarta.transactions (
    transaction_id   SERIAL PRIMARY KEY,
    customer_id      INT REFERENCES tenant_jakarta.customers(customer_id),
    store_id         INT REFERENCES tenant_jakarta.stores(store_id),
    transaction_date TIMESTAMP NOT NULL,
    total_amount     NUMERIC(14,2) NOT NULL,
    payment_method   VARCHAR(30),
    status           VARCHAR(20) DEFAULT 'completed',
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_jakarta.transaction_items (
    item_id         SERIAL PRIMARY KEY,
    transaction_id  INT REFERENCES tenant_jakarta.transactions(transaction_id),
    product_id      INT REFERENCES tenant_jakarta.products(product_id),
    quantity        INT NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    discount        NUMERIC(5,2) DEFAULT 0,
    subtotal        NUMERIC(14,2) NOT NULL,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_jakarta.transaction_promotions (
    id               SERIAL PRIMARY KEY,
    transaction_id   INT REFERENCES tenant_jakarta.transactions(transaction_id),
    promo_id         INT REFERENCES tenant_jakarta.promotions(promo_id),
    discount_applied NUMERIC(12,2),
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jakarta_txn_upd  ON tenant_jakarta.transactions(updated_at);
CREATE INDEX IF NOT EXISTS idx_jakarta_item_upd ON tenant_jakarta.transaction_items(updated_at);

-- ---- tenant_bandung ----
CREATE SCHEMA IF NOT EXISTS tenant_bandung;

CREATE TABLE IF NOT EXISTS tenant_bandung.customers (
    customer_id  SERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    phone        VARCHAR(20),
    email        VARCHAR(100),
    gender       VARCHAR(10),
    city         VARCHAR(50),
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_bandung.stores (
    store_id     SERIAL PRIMARY KEY,
    store_name   VARCHAR(100) NOT NULL,
    city         VARCHAR(50),
    province     VARCHAR(50),
    store_type   VARCHAR(30),
    opened_at    DATE,
    is_active    BOOLEAN DEFAULT TRUE,
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_bandung.suppliers (
    supplier_id  SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100),
    contact_name VARCHAR(100),
    city         VARCHAR(50),
    country      VARCHAR(50) DEFAULT 'Indonesia',
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_bandung.products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category     VARCHAR(50),
    brand        VARCHAR(50),
    unit_price   NUMERIC(12,2) NOT NULL,
    is_active    BOOLEAN DEFAULT TRUE,
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_bandung.promotions (
    promo_id     SERIAL PRIMARY KEY,
    promo_name   VARCHAR(100),
    promo_type   VARCHAR(30),
    discount_pct NUMERIC(5,2),
    start_date   DATE,
    end_date     DATE,
    min_purchase NUMERIC(12,2) DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_bandung.transactions (
    transaction_id   SERIAL PRIMARY KEY,
    customer_id      INT REFERENCES tenant_bandung.customers(customer_id),
    store_id         INT REFERENCES tenant_bandung.stores(store_id),
    transaction_date TIMESTAMP NOT NULL,
    total_amount     NUMERIC(14,2) NOT NULL,
    payment_method   VARCHAR(30),
    status           VARCHAR(20) DEFAULT 'completed',
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_bandung.transaction_items (
    item_id         SERIAL PRIMARY KEY,
    transaction_id  INT REFERENCES tenant_bandung.transactions(transaction_id),
    product_id      INT REFERENCES tenant_bandung.products(product_id),
    quantity        INT NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    discount        NUMERIC(5,2) DEFAULT 0,
    subtotal        NUMERIC(14,2) NOT NULL,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_bandung.transaction_promotions (
    id               SERIAL PRIMARY KEY,
    transaction_id   INT REFERENCES tenant_bandung.transactions(transaction_id),
    promo_id         INT REFERENCES tenant_bandung.promotions(promo_id),
    discount_applied NUMERIC(12,2),
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bandung_txn_upd  ON tenant_bandung.transactions(updated_at);
CREATE INDEX IF NOT EXISTS idx_bandung_item_upd ON tenant_bandung.transaction_items(updated_at);

-- ---- tenant_surabaya ----
CREATE SCHEMA IF NOT EXISTS tenant_surabaya;

CREATE TABLE IF NOT EXISTS tenant_surabaya.customers (
    customer_id  SERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    phone        VARCHAR(20),
    email        VARCHAR(100),
    gender       VARCHAR(10),
    city         VARCHAR(50),
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_surabaya.stores (
    store_id     SERIAL PRIMARY KEY,
    store_name   VARCHAR(100) NOT NULL,
    city         VARCHAR(50),
    province     VARCHAR(50),
    store_type   VARCHAR(30),
    opened_at    DATE,
    is_active    BOOLEAN DEFAULT TRUE,
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_surabaya.suppliers (
    supplier_id  SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100),
    contact_name VARCHAR(100),
    city         VARCHAR(50),
    country      VARCHAR(50) DEFAULT 'Indonesia',
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_surabaya.products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category     VARCHAR(50),
    brand        VARCHAR(50),
    unit_price   NUMERIC(12,2) NOT NULL,
    is_active    BOOLEAN DEFAULT TRUE,
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_surabaya.promotions (
    promo_id     SERIAL PRIMARY KEY,
    promo_name   VARCHAR(100),
    promo_type   VARCHAR(30),
    discount_pct NUMERIC(5,2),
    start_date   DATE,
    end_date     DATE,
    min_purchase NUMERIC(12,2) DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_surabaya.transactions (
    transaction_id   SERIAL PRIMARY KEY,
    customer_id      INT REFERENCES tenant_surabaya.customers(customer_id),
    store_id         INT REFERENCES tenant_surabaya.stores(store_id),
    transaction_date TIMESTAMP NOT NULL,
    total_amount     NUMERIC(14,2) NOT NULL,
    payment_method   VARCHAR(30),
    status           VARCHAR(20) DEFAULT 'completed',
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_surabaya.transaction_items (
    item_id         SERIAL PRIMARY KEY,
    transaction_id  INT REFERENCES tenant_surabaya.transactions(transaction_id),
    product_id      INT REFERENCES tenant_surabaya.products(product_id),
    quantity        INT NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    discount        NUMERIC(5,2) DEFAULT 0,
    subtotal        NUMERIC(14,2) NOT NULL,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_surabaya.transaction_promotions (
    id               SERIAL PRIMARY KEY,
    transaction_id   INT REFERENCES tenant_surabaya.transactions(transaction_id),
    promo_id         INT REFERENCES tenant_surabaya.promotions(promo_id),
    discount_applied NUMERIC(12,2),
    updated_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_surabaya_txn_upd  ON tenant_surabaya.transactions(updated_at);
CREATE INDEX IF NOT EXISTS idx_surabaya_item_upd ON tenant_surabaya.transaction_items(updated_at);
