-- =====================================================
-- SILVER LAYER - Cleaned & Validated Data Tables
-- =====================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS silver_campaigns CASCADE;
DROP TABLE IF EXISTS silver_inventory CASCADE;
DROP TABLE IF EXISTS silver_orders CASCADE;
DROP TABLE IF EXISTS silver_customers CASCADE;
DROP TABLE IF EXISTS silver_products CASCADE;

-- 1. Silver Products
CREATE TABLE silver_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(500) NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price > 0),
    description TEXT,
    category VARCHAR(100) NOT NULL,
    image_url TEXT,
    average_rating DECIMAL(3,2),
    review_count INTEGER,
    price_tier VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score INTEGER,
    source_table VARCHAR(50) DEFAULT 'bronze_products'
);

-- 2. Silver Orders
CREATE TABLE silver_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    total_amount DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    final_amount DECIMAL(10,2),
    order_date DATE NOT NULL,
    order_time TIME NOT NULL,
    order_timestamp TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('completed', 'pending', 'cancelled', 'refunded')),
    payment_method VARCHAR(50),
    promo_code VARCHAR(20),
    order_hour INTEGER,
    order_day_of_week VARCHAR(10),
    is_weekend BOOLEAN,
    is_discounted BOOLEAN,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table VARCHAR(50) DEFAULT 'bronze_orders'
);

-- 3. Silver Customers
CREATE TABLE silver_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(50) DEFAULT 'India',
    signup_date DATE NOT NULL,
    loyalty_tier VARCHAR(20) DEFAULT 'Bronze',
    total_lifetime_spend DECIMAL(12,2) DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    customer_age_days INTEGER,
    customer_segment VARCHAR(50),

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table VARCHAR(50) DEFAULT 'bronze_customers'
);

-- 4. Silver Inventory
CREATE TABLE silver_inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    warehouse_location VARCHAR(100),
    reorder_point INTEGER NOT NULL,
    cost_price DECIMAL(10,2) CHECK (cost_price > 0),
    stock_status VARCHAR(20),
    days_until_stockout INTEGER,
    inventory_value DECIMAL(12,2),
    inventory_date DATE NOT NULL,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table VARCHAR(50) DEFAULT 'bronze_inventory'
);

-- 5. Silver Campaigns
CREATE TABLE silver_campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(255),
    channel VARCHAR(100),
    promo_code VARCHAR(20),
    discount_percentage INTEGER,
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    actual_spend DECIMAL(12,2),
    is_active BOOLEAN,

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table VARCHAR(50) DEFAULT 'bronze_campaigns'
);

-- Create indexes
CREATE INDEX idx_silver_orders_customer ON silver_orders(customer_id);
CREATE INDEX idx_silver_orders_date ON silver_orders(order_date);
CREATE INDEX idx_silver_orders_product ON silver_orders(product_id);
CREATE INDEX idx_silver_inventory_product ON silver_inventory(product_id);
CREATE INDEX idx_silver_inventory_status ON silver_inventory(stock_status);
CREATE INDEX idx_silver_customers_segment ON silver_customers(customer_segment);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO warehouse;