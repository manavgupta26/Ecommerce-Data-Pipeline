-- =====================================================
-- SCD TYPE 2 TABLES - Historical Tracking
-- =====================================================

-- Drop existing tables
DROP TABLE IF EXISTS dim_product_price_history CASCADE;
DROP TABLE IF EXISTS dim_customer_tier_history CASCADE;

-- 1. Product Price History (SCD Type 2)
CREATE TABLE dim_product_price_history (
    price_history_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(500),
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    price_tier VARCHAR(20),

    -- SCD Type 2 columns
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_reason VARCHAR(255)
);

CREATE INDEX idx_product_price_hist_product ON dim_product_price_history(product_id);
CREATE INDEX idx_product_price_hist_current ON dim_product_price_history(is_current);
CREATE INDEX idx_product_price_hist_dates ON dim_product_price_history(valid_from, valid_to);

-- 2. Customer Tier History (SCD Type 2)
CREATE TABLE dim_customer_tier_history (
    tier_history_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    full_name VARCHAR(255),
    email VARCHAR(255),
    loyalty_tier VARCHAR(20) NOT NULL,
    total_lifetime_spend DECIMAL(12,2),

    -- SCD Type 2 columns
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_reason VARCHAR(255)
);

CREATE INDEX idx_customer_tier_hist_customer ON dim_customer_tier_history(customer_id);
CREATE INDEX idx_customer_tier_hist_current ON dim_customer_tier_history(is_current);
CREATE INDEX idx_customer_tier_hist_dates ON dim_customer_tier_history(valid_from, valid_to);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO warehouse;



