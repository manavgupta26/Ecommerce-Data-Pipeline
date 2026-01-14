"""
Gold Layer Analytics DAG
Creates business-ready analytics from Silver layer data
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def create_daily_revenue(**context):
    """Create daily revenue analytics"""
    print("📊 Creating daily revenue analytics...")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Clear existing Gold data (for fresh recalculation)
    cursor.execute("TRUNCATE TABLE gold_daily_revenue")

    # Calculate daily revenue metrics
    query = """
            INSERT INTO gold_daily_revenue (revenue_date, year, month, quarter, day_of_week, is_weekend, \
                                            total_revenue, total_orders, total_items_sold, average_order_value, \
                                            completed_orders, pending_orders, cancelled_orders, cancellation_rate, \
                                            unique_customers, total_discount_given, orders_with_promo, promo_usage_rate)
            SELECT order_date, \
                   EXTRACT(YEAR FROM order_date) AS year,
            EXTRACT(MONTH FROM order_date) AS month,
            EXTRACT(QUARTER FROM order_date) AS quarter,
            order_day_of_week,
            is_weekend,
            SUM(CASE WHEN status = 'completed' THEN final_amount ELSE 0 END) AS total_revenue,
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_items_sold,
            AVG(CASE WHEN status = 'completed' THEN final_amount ELSE NULL END) AS average_order_value,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_orders,
            COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_orders,
            COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled_orders,
            ROUND(100.0 * COUNT(CASE WHEN status = 'cancelled' THEN 1 END) / COUNT(*), 2) AS cancellation_rate,
            COUNT(DISTINCT customer_id) AS unique_customers,
            SUM(discount_amount) AS total_discount_given,
            COUNT(CASE WHEN promo_code IS NOT NULL THEN 1 END) AS orders_with_promo,
            ROUND(100.0 * COUNT(CASE WHEN promo_code IS NOT NULL THEN 1 END) / COUNT(*), 2) AS promo_usage_rate
            FROM silver_orders
            GROUP BY order_date, order_day_of_week, is_weekend
            ORDER BY order_date \
            """

    cursor.execute(query)
    rows_inserted = cursor.rowcount
    conn.commit()

    print(f"✅ Created {rows_inserted} daily revenue records")

    # Calculate revenue growth
    cursor.execute("""
                   UPDATE gold_daily_revenue gdr
                   SET revenue_vs_previous_day = gdr.total_revenue - prev.total_revenue,
                       revenue_growth_pct      = ROUND(
                               100.0 * (gdr.total_revenue - prev.total_revenue) / NULLIF(prev.total_revenue, 0),
                               2
                                                 ) FROM gold_daily_revenue prev
                   WHERE gdr.revenue_date = prev.revenue_date + INTERVAL '1 day'
                   """)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Updated revenue growth metrics")
    return rows_inserted


def create_product_performance(**context):
    """Create product performance analytics"""
    print("📊 Creating product performance analytics...")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("TRUNCATE TABLE gold_product_performance")

    # Calculate product performance for last 30 days
    query = """
            INSERT INTO gold_product_performance (product_id, analysis_period, period_start_date, period_end_date, \
                                                  product_name, category, current_price, units_sold, total_revenue, \
                                                  total_profit, profit_margin, unique_customers, average_rating)
            SELECT p.product_id, \
                   'last_30_days' AS analysis_period, \
                   CURRENT_DATE - INTERVAL '30 days' AS period_start_date, CURRENT_DATE AS period_end_date, p.product_name, p.category, p.price AS current_price, COALESCE (SUM (o.quantity), 0) AS units_sold, COALESCE (SUM (o.final_amount), 0) AS total_revenue, COALESCE (SUM ((o.unit_price - i.cost_price) * o.quantity), 0) AS total_profit, CASE
                WHEN SUM (o.final_amount) > 0 THEN
                ROUND(100.0 * SUM ((o.unit_price - i.cost_price) * o.quantity) / SUM (o.final_amount), 2)
                ELSE 0
            END \
            AS profit_margin,
            COUNT(DISTINCT o.customer_id) AS unique_customers,
            p.average_rating
        FROM silver_products p
        LEFT JOIN silver_orders o ON p.product_id = o.product_id
            AND o.status = 'completed'
            AND o.order_date >= CURRENT_DATE - INTERVAL '30 days'
        LEFT JOIN silver_inventory i ON p.product_id = i.product_id
        GROUP BY p.product_id, p.product_name, p.category, p.price, p.average_rating \
            """

    cursor.execute(query)
    rows_inserted = cursor.rowcount
    conn.commit()

    print(f"✅ Created {rows_inserted} product performance records")

    # Calculate overall rankings
    cursor.execute("""
                   UPDATE gold_product_performance gpp
                   SET overall_rank = ranked.rank FROM (
            SELECT product_id,
                   RANK() OVER (ORDER BY total_revenue DESC) AS rank
            FROM gold_product_performance
            WHERE analysis_period = 'last_30_days'
        ) ranked
                   WHERE gpp.product_id = ranked.product_id
                     AND gpp.analysis_period = 'last_30_days'
                   """)

    # Calculate category rankings
    cursor.execute("""
                   UPDATE gold_product_performance gpp
                   SET category_rank = ranked.rank FROM (
            SELECT product_id,
                   RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rank
            FROM gold_product_performance
            WHERE analysis_period = 'last_30_days'
        ) ranked
                   WHERE gpp.product_id = ranked.product_id
                     AND gpp.analysis_period = 'last_30_days'
                   """)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Updated product rankings")
    return rows_inserted


def create_customer_segments(**context):
    """Create customer RFM segmentation"""
    print("📊 Creating customer segments (RFM analysis)...")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("TRUNCATE TABLE gold_customer_segments")

    # Calculate RFM metrics and segments
    query = """
            INSERT INTO gold_customer_segments (customer_id, full_name, email, signup_date, \
                                                recency_score, frequency_score, monetary_score, rfm_combined_score, \
                                                days_since_last_order, total_orders, total_lifetime_value, \
                                                average_order_value, customer_segment, first_order_date, \
                                                last_order_date)
            WITH customer_orders AS (SELECT c.customer_id, \
                                            c.full_name, \
                                            c.email, \
                                            c.signup_date, \
                                            COUNT(o.order_id)                AS total_orders, \
                                            SUM(o.final_amount)              AS total_lifetime_value, \
                                            AVG(o.final_amount)              AS average_order_value, \
                                            MAX(o.order_date)                AS last_order_date, \
                                            MIN(o.order_date)                AS first_order_date, \
                                            CURRENT_DATE - MAX(o.order_date) AS days_since_last_order \
                                     FROM silver_customers c \
                                              LEFT JOIN silver_orders o ON c.customer_id = o.customer_id \
                                         AND o.status = 'completed' \
                                     GROUP BY c.customer_id, c.full_name, c.email, c.signup_date),
                 rfm_scores AS (SELECT *, \
                                       NTILE(5) OVER (ORDER BY days_since_last_order ASC NULLS LAST) AS recency_score, NTILE(5) OVER (ORDER BY total_orders DESC) AS frequency_score, NTILE(5) OVER (ORDER BY total_lifetime_value DESC) AS monetary_score \
                                FROM customer_orders)
            SELECT customer_id, \
                   full_name, \
                   email, \
                   signup_date, \
                   recency_score, \
                   frequency_score, \
                   monetary_score, \
                   CONCAT(recency_score, frequency_score, monetary_score) AS rfm_combined_score, \
                   COALESCE(days_since_last_order, 9999)                  AS days_since_last_order, \
                   COALESCE(total_orders, 0)                              AS total_orders, \
                   COALESCE(total_lifetime_value, 0)                      AS total_lifetime_value, \
                   COALESCE(average_order_value, 0)                       AS average_order_value, \
                   CASE \
                       WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions' \
                       WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers' \
                       WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'Potential Loyalists' \
                       WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk' \
                       WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost' \
                       WHEN total_orders = 0 THEN 'New (No Orders)' \
                       ELSE 'Regular' \
                       END                                                AS customer_segment, \
                   first_order_date, \
                   last_order_date
            FROM rfm_scores \
            """

    cursor.execute(query)
    rows_inserted = cursor.rowcount
    conn.commit()

    print(f"✅ Created {rows_inserted} customer segment records")

    # Add churn risk classification
    cursor.execute("""
                   UPDATE gold_customer_segments
                   SET churn_risk = CASE
                                        WHEN days_since_last_order > 180 THEN 'High'
                                        WHEN days_since_last_order > 90 THEN 'Medium'
                                        ELSE 'Low'
                       END
                   """)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Updated churn risk classifications")
    return rows_inserted


def create_inventory_health(**context):
    """Create inventory health analytics"""
    print("📊 Creating inventory health analytics...")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("TRUNCATE TABLE gold_inventory_health")

    # Calculate inventory health
    query = """
            INSERT INTO gold_inventory_health (snapshot_date, product_id, product_name, category, \
                                               current_stock, stock_status, warehouse_location, \
                                               inventory_value, cost_price, selling_price, potential_profit, \
                                               is_low_stock, is_overstock, is_dead_stock)
            SELECT i.inventory_date                            AS snapshot_date, \
                   p.product_id, \
                   p.product_name, \
                   p.category, \
                   i.stock_quantity                            AS current_stock, \
                   i.stock_status, \
                   i.warehouse_location, \
                   i.inventory_value, \
                   i.cost_price, \
                   p.price                                     AS selling_price, \
                   (p.price - i.cost_price) * i.stock_quantity AS potential_profit, \
                   i.stock_status = 'Low Stock'                AS is_low_stock, \
                   i.stock_status = 'Overstock'                AS is_overstock, \
                   FALSE                                       AS is_dead_stock
            FROM silver_inventory i
                     JOIN silver_products p ON i.product_id = p.product_id \
            """

    cursor.execute(query)
    rows_inserted = cursor.rowcount
    conn.commit()

    print(f"✅ Created {rows_inserted} inventory health records")

    # Mark dead stock (no sales in last 30 days)
    cursor.execute("""
                   UPDATE gold_inventory_health gih
                   SET is_dead_stock = TRUE
                   WHERE NOT EXISTS (SELECT 1
                                     FROM silver_orders o
                                     WHERE o.product_id = gih.product_id
                                       AND o.order_date >= CURRENT_DATE - INTERVAL '30 days'
                       AND o.status = 'completed')
                     AND current_stock > 0
                   """)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Updated dead stock indicators")
    return rows_inserted


def create_campaign_roi(**context):
    """Create campaign ROI analytics"""
    print("📊 Creating campaign ROI analytics...")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("TRUNCATE TABLE gold_campaign_roi")

    # Calculate campaign ROI
    query = """
            INSERT INTO gold_campaign_roi (campaign_id, campaign_name, channel, promo_code, \
                                           start_date, end_date, budget, actual_spend, \
                                           total_orders, total_revenue, promo_code_usage_count, \
                                           roi, roas, cost_per_order)
            SELECT c.campaign_id, \
                   c.campaign_name, \
                   c.channel, \
                   c.promo_code, \
                   c.start_date, \
                   c.end_date, \
                   c.budget, \
                   c.actual_spend, \
                   COUNT(o.order_id)                AS total_orders, \
                   COALESCE(SUM(o.final_amount), 0) AS total_revenue, \
                   COUNT(o.order_id)                AS promo_code_usage_count, \
                   CASE \
                       WHEN c.actual_spend > 0 THEN \
                           ROUND(100.0 * (SUM(o.final_amount) - c.actual_spend) / c.actual_spend, 2) \
                       ELSE 0 \
                       END                          AS roi, \
                   CASE \
                       WHEN c.actual_spend > 0 THEN \
                           ROUND(SUM(o.final_amount) / c.actual_spend, 2) \
                       ELSE 0 \
                       END                          AS roas, \
                   CASE \
                       WHEN COUNT(o.order_id) > 0 THEN \
                           ROUND(c.actual_spend / COUNT(o.order_id), 2) \
                       ELSE 0 \
                       END                          AS cost_per_order
            FROM silver_campaigns c
                     LEFT JOIN silver_orders o ON c.promo_code = o.promo_code
                AND o.status = 'completed'
                AND o.order_date BETWEEN c.start_date AND c.end_date
            GROUP BY c.campaign_id, c.campaign_name, c.channel, c.promo_code,
                     c.start_date, c.end_date, c.budget, c.actual_spend \
            """

    cursor.execute(query)
    rows_inserted = cursor.rowcount
    conn.commit()

    print(f"✅ Created {rows_inserted} campaign ROI records")

    # Add performance ratings and status
    cursor.execute("""
                   UPDATE gold_campaign_roi
                   SET performance_rating = CASE
                                                WHEN roi > 200 THEN 'Excellent'
                                                WHEN roi > 100 THEN 'Good'
                                                WHEN roi > 0 THEN 'Average'
                                                ELSE 'Poor'
                       END,
                       campaign_status    = CASE
                                                WHEN end_date >= CURRENT_DATE THEN 'Active'
                                                ELSE 'Completed'
                           END
                   """)

    # Add campaign rankings
    cursor.execute("""
                   UPDATE gold_campaign_roi gcr
                   SET campaign_rank = ranked.rank FROM (
            SELECT campaign_id,
                   RANK() OVER (ORDER BY roi DESC) AS rank
            FROM gold_campaign_roi
        ) ranked
                   WHERE gcr.campaign_id = ranked.campaign_id
                   """)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Updated campaign performance ratings and rankings")
    return rows_inserted


# Define the DAG
with DAG(
        dag_id='gold_analytics',
        default_args=default_args,
        description='Create analytics tables in Gold layer',
        schedule_interval='*/1 * * * *',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['gold', 'analytics', 'ecommerce'],
) as dag:
    # Wait for Silver transformation to complete
    wait_for_silver = ExternalTaskSensor(
        task_id='wait_for_silver_transformation',
        external_dag_id='silver_transformation',
        external_task_id=None,  # Wait for entire DAG
        mode='poke',
        timeout=600,
        poke_interval=30
    )

    # Analytics tasks
    daily_revenue_task = PythonOperator(
        task_id='create_daily_revenue',
        python_callable=create_daily_revenue,
        provide_context=True
    )

    product_performance_task = PythonOperator(
        task_id='create_product_performance',
        python_callable=create_product_performance,
        provide_context=True
    )

    customer_segments_task = PythonOperator(
        task_id='create_customer_segments',
        python_callable=create_customer_segments,
        provide_context=True
    )

    inventory_health_task = PythonOperator(
        task_id='create_inventory_health',
        python_callable=create_inventory_health,
        provide_context=True
    )

    campaign_roi_task = PythonOperator(
        task_id='create_campaign_roi',
        python_callable=create_campaign_roi,
        provide_context=True
    )

    # Define dependencies - all analytics tasks run in parallel after Silver completes
    wait_for_silver >> [
        daily_revenue_task,
        product_performance_task,
        customer_segments_task,
        inventory_health_task,
        campaign_roi_task
    ]