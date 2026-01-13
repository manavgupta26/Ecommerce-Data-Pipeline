-- =====================================================
-- BRONZE LAYER - Raw Data Tables
-- =====================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS bronze_campaigns CASCADE;
DROP TABLE IF EXISTS bronze_inventory CASCADE;
DROP TABLE IF EXISTS bronze_orders CASCADE;
DROP TABLE IF EXISTS bronze_customers CASCADE;
DROP TABLE IF EXISTS bronze_products CASCADE;

-- 1. Bronze Products
CREATE TABLE bronze_products (
    product_id INTEGER NOT NULL,
    title VARCHAR(500),
    price DECIMAL(10,2),
    description TEXT,
    category VARCHAR(100),
    image_url TEXT,
    rating_rate DECIMAL(3,2),
    rating_count INTEGER,

    -- Metadata columns
    source VARCHAR(50) DEFAULT 'fakestoreapi',
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id VARCHAR(100),
    raw_json JSONB,

    PRIMARY KEY (product_id, ingestion_timestamp)
);

-- 2. Bronze Orders
CREATE TABLE bronze_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    final_amount DECIMAL(10,2),
    order_timestamp TIMESTAMP,
    status VARCHAR(20),
    payment_method VARCHAR(50),
    promo_code VARCHAR(20),
    shipping_address TEXT,

    -- Metadata columns
    source VARCHAR(50) DEFAULT 'order_generator',
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id VARCHAR(100),
    raw_json JSONB
);

-- 3. Bronze Customers
CREATE TABLE bronze_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    street_address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'India',
    signup_date DATE,
    loyalty_tier VARCHAR(20),
    total_lifetime_spend DECIMAL(12,2),
    is_active BOOLEAN DEFAULT TRUE,

    -- Metadata columns
    source VARCHAR(50) DEFAULT 'customer_generator',
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id VARCHAR(100),
    last_modified_date TIMESTAMP
);

-- 4. Bronze Inventory
CREATE TABLE bronze_inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    stock_quantity INTEGER,
    warehouse_location VARCHAR(100),
    reorder_point INTEGER,
    reorder_quantity INTEGER,
    cost_price DECIMAL(10,2),
    supplier_name VARCHAR(255),
    last_restock_date DATE,
    inventory_date DATE,

    -- Metadata columns
    source VARCHAR(50) DEFAULT 'inventory_csv',
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id VARCHAR(100),
    source_filename VARCHAR(255)
);

-- 5. Bronze Campaigns
CREATE TABLE bronze_campaigns (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(255),
    channel VARCHAR(100),
    promo_code VARCHAR(20),
    discount_percentage INTEGER,
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    actual_spend DECIMAL(12,2),

    -- Metadata columns
    source VARCHAR(50) DEFAULT 'campaign_generator',
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id VARCHAR(100)
);

-- Create indexes for better query performance
CREATE INDEX idx_bronze_orders_customer ON bronze_orders(customer_id);
CREATE INDEX idx_bronze_orders_date ON bronze_orders(order_timestamp);
CREATE INDEX idx_bronze_orders_product ON bronze_orders(product_id);
CREATE INDEX idx_bronze_inventory_product ON bronze_inventory(product_id);
CREATE INDEX idx_bronze_inventory_date ON bronze_inventory(inventory_date);
CREATE INDEX idx_bronze_customers_email ON bronze_customers(email);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO warehouse;