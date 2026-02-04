-- 01_init_tables.sql

-- 1. Tabela wymiarów
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id SERIAL PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL UNIQUE,
    currency_name VARCHAR(100),
    is_active BOOLEAN DEFAULT true
);

-- 2. Tabela faktów
CREATE TABLE IF NOT EXISTS fact_exchange_rate (
    fact_id SERIAL PRIMARY KEY,
    currency_id INTEGER NOT NULL,
    rate_date DATE NOT NULL,
    rate NUMERIC(10, 4) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_currency
      FOREIGN KEY(currency_id)
      REFERENCES dim_currency(currency_id),

    CONSTRAINT unique_fact_currency_date UNIQUE (currency_id, rate_date)
);

-- Indeksy dla wydajności joinow w PowerBI
CREATE INDEX IF NOT EXISTS idx_fact_currency_id ON fact_exchange_rate(currency_id);
CREATE INDEX IF NOT EXISTS idx_fact_rate_date ON fact_exchange_rate(rate_date);