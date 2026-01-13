-- =====================================================
-- GOLD LAYER - Analytics Tables
-- =====================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS gold_campaign_roi CASCADE;
DROP TABLE IF EXISTS gold_inventory_health CASCADE;
DROP TABLE IF EXISTS gold_customer_segments CASCADE;
DROP TABLE IF EXISTS gold_product_performance CASCADE;
DROP TABLE IF EXISTS gold_daily_revenue CASCADE;

-- 1. Gold Daily Revenue
CREATE TABLE gold_daily_revenue (
    revenue_date DATE PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    quarter INTEGER,
    day_of_week VARCHAR(10),
    is_weekend BOOLEAN,
    total_revenue DECIMAL(15,2),
    total_orders INTEGER,
    total_items_sold INTEGER,
    average_order_value DECIMAL(10,2),
    completed_orders INTEGER,
    pending_orders INTEGER,
    cancelled_orders INTEGER,
    cancellation_rate DECIMAL(5,2),
    unique_customers INTEGER,
    new_customers INTEGER,
    returning_customers INTEGER,
    total_discount_given DECIMAL(12,2),
    orders_with_promo INTEGER,
    promo_usage_rate DECIMAL(5,2),
    revenue_vs_previous_day DECIMAL(10,2),
    revenue_growth_pct DECIMAL(5,2),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Gold Product Performance
CREATE TABLE gold_product_performance (
    product_id INTEGER,
    analysis_period VARCHAR(20),
    period_start_date DATE,
    period_end_date DATE,
    product_name VARCHAR(500),
    category VARCHAR(100),
    current_price DECIMAL(10,2),
    units_sold INTEGER,
    total_revenue DECIMAL(15,2),
    total_profit DECIMAL(15,2),
    profit_margin DECIMAL(5,2),
    category_rank INTEGER,
    overall_rank INTEGER,
    avg_stock_level INTEGER,
    stockout_days INTEGER,
    inventory_turnover DECIMAL(5,2),
    unique_customers INTEGER,
    repeat_purchase_rate DECIMAL(5,2),
    average_rating DECIMAL(3,2),
    revenue_growth_vs_previous DECIMAL(5,2),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, analysis_period, period_start_date)
);

-- 3. Gold Customer Segments
CREATE TABLE gold_customer_segments (
    customer_id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(255),
    email VARCHAR(255),
    signup_date DATE,
    recency_score INTEGER,
    frequency_score INTEGER,
    monetary_score INTEGER,
    rfm_combined_score VARCHAR(10),
    days_since_last_order INTEGER,
    total_orders INTEGER,
    total_lifetime_value DECIMAL(12,2),
    average_order_value DECIMAL(10,2),
    customer_segment VARCHAR(50),
    segment_description TEXT,
    churn_risk VARCHAR(20),
    predicted_next_purchase_days INTEGER,
    recommended_action TEXT,
    first_order_date DATE,
    last_order_date DATE,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Gold Inventory Health
CREATE TABLE gold_inventory_health (
    inventory_snapshot_id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(500),
    category VARCHAR(100),
    current_stock INTEGER,
    stock_status VARCHAR(20),
    warehouse_location VARCHAR(100),
    inventory_value DECIMAL(12,2),
    cost_price DECIMAL(10,2),
    selling_price DECIMAL(10,2),
    potential_profit DECIMAL(12,2),
    avg_daily_sales DECIMAL(10,2),
    days_of_inventory_remaining INTEGER,
    inventory_turnover_rate DECIMAL(5,2),
    is_low_stock BOOLEAN,
    is_overstock BOOLEAN,
    is_dead_stock BOOLEAN,
    reorder_recommended BOOLEAN,
    recommended_reorder_quantity INTEGER,
    stock_change_7days INTEGER,
    stock_change_30days INTEGER,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Gold Campaign ROI
CREATE TABLE gold_campaign_roi (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(255),
    channel VARCHAR(100),
    promo_code VARCHAR(20),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    actual_spend DECIMAL(12,2),
    total_orders INTEGER,
    total_revenue DECIMAL(15,2),
    total_profit DECIMAL(15,2),
    roi DECIMAL(10,2),
    roas DECIMAL(10,2),
    cost_per_order DECIMAL(10,2),
    cost_per_acquisition DECIMAL(10,2),
    new_customers_acquired INTEGER,
    promo_code_usage_count INTEGER,
    conversion_rate DECIMAL(5,2),
    campaign_rank INTEGER,
    campaign_status VARCHAR(20),
    performance_rating VARCHAR(20),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_gold_daily_revenue_date ON gold_daily_revenue(revenue_date);
CREATE INDEX idx_gold_product_perf_category ON gold_product_performance(category);
CREATE INDEX idx_gold_product_perf_rank ON gold_product_performance(overall_rank);
CREATE INDEX idx_gold_customer_segment ON gold_customer_segments(customer_segment);
CREATE INDEX idx_gold_customer_churn ON gold_customer_segments(churn_risk);
CREATE INDEX idx_gold_inventory_date ON gold_inventory_health(snapshot_date);
CREATE INDEX idx_gold_inventory_alerts ON gold_inventory_health(is_low_stock, is_overstock);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO warehouse;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO warehouse;
