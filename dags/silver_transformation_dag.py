"""
Silver Layer Transformation DAG
Cleans and validates data from Bronze to Silver layer
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import re
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def validate_email(email):
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def calculate_price_tier(price):
    """Calculate price tier based on price"""
    if price < 50:
        return 'Budget'
    elif price <= 200:
        return 'Mid-range'
    else:
        return 'Premium'


def calculate_stock_status(stock_quantity, reorder_point):
    """Calculate stock status"""
    if stock_quantity == 0:
        return 'Out of Stock'
    elif stock_quantity < reorder_point:
        return 'Low Stock'
    elif stock_quantity > reorder_point * 5:
        return 'Overstock'
    else:
        return 'In Stock'


def transform_products(**context):
    """Transform products from Bronze to Silver"""
    print("🔄 Transforming products: Bronze → Silver")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Fetch products from Bronze
    cursor.execute("""
                   SELECT DISTINCT
                   ON (product_id)
                       product_id, title, price, description, category,
                       image_url, rating_rate, rating_count
                   FROM bronze_products
                   WHERE price > 0 -- Data quality check
                   ORDER BY product_id, ingestion_timestamp DESC
                   """)

    bronze_products = cursor.fetchall()
    print(f"📦 Found {len(bronze_products)} products in Bronze")

    # Transform and insert into Silver
    insert_query = """
                   INSERT INTO silver_products (product_id, product_name, price, description, category, \
                                                image_url, average_rating, review_count, price_tier, \
                                                is_active, data_quality_score) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (product_id) DO \
                   UPDATE SET
                       product_name = EXCLUDED.product_name, \
                       price = EXCLUDED.price, \
                       description = EXCLUDED.description, \
                       category = EXCLUDED.category, \
                       image_url = EXCLUDED.image_url, \
                       average_rating = EXCLUDED.average_rating, \
                       review_count = EXCLUDED.review_count, \
                       price_tier = EXCLUDED.price_tier, \
                       updated_at = CURRENT_TIMESTAMP \
                   """

    transformed_count = 0
    for row in bronze_products:
        product_id, title, price, description, category, image_url, rating_rate, rating_count = row

        # Calculate data quality score (0-100)
        quality_score = 100
        if not title: quality_score -= 20
        if not description: quality_score -= 20
        if not image_url: quality_score -= 10
        if not rating_rate: quality_score -= 10

        # Calculate price tier
        price_tier = calculate_price_tier(price)

        try:
            cursor.execute(insert_query, (
                product_id,
                title,
                price,
                description,
                category,
                image_url,
                rating_rate,
                rating_count,
                price_tier,
                True,  # is_active
                quality_score
            ))
            transformed_count += 1
        except Exception as e:
            print(f"⚠️ Error transforming product {product_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Transformed {transformed_count} products into silver_products")
    return transformed_count


def transform_customers(**context):
    """Transform customers from Bronze to Silver"""
    print("🔄 Transforming customers: Bronze → Silver")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Fetch customers from Bronze
    cursor.execute("""
                   SELECT customer_id,
                          first_name,
                          last_name,
                          email,
                          phone,
                          city,
                          state,
                          country,
                          signup_date,
                          loyalty_tier,
                          total_lifetime_spend,
                          is_active
                   FROM bronze_customers
                   """)

    bronze_customers = cursor.fetchall()
    print(f"👤 Found {len(bronze_customers)} customers in Bronze")

    # Transform and insert into Silver
    insert_query = """
                   INSERT INTO silver_customers (customer_id, full_name, email, phone, city, state, country, \
                                                 signup_date, loyalty_tier, total_lifetime_spend, is_active, \
                                                 customer_age_days, customer_segment) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO \
                   UPDATE SET
                       full_name = EXCLUDED.full_name, \
                       email = EXCLUDED.email, \
                       phone = EXCLUDED.phone, \
                       city = EXCLUDED.city, \
                       state = EXCLUDED.state, \
                       loyalty_tier = EXCLUDED.loyalty_tier, \
                       total_lifetime_spend = EXCLUDED.total_lifetime_spend, \
                       is_active = EXCLUDED.is_active, \
                       customer_age_days = EXCLUDED.customer_age_days, \
                       customer_segment = EXCLUDED.customer_segment, \
                       updated_at = CURRENT_TIMESTAMP \
                   """

    transformed_count = 0
    skipped_count = 0

    for row in bronze_customers:
        customer_id, first_name, last_name, email, phone, city, state, country, signup_date, loyalty_tier, total_lifetime_spend, is_active = row

        # Data quality checks
        if not validate_email(email):
            print(f"⚠️ Invalid email for customer {customer_id}, skipping")
            skipped_count += 1
            continue

        # Create full name
        full_name = f"{first_name} {last_name}".strip()

        # Calculate customer age in days
        customer_age_days = (datetime.now().date() - signup_date).days

        # Determine customer segment
        if customer_age_days < 30:
            customer_segment = 'New'
        elif total_lifetime_spend > 50000:
            customer_segment = 'VIP'
        elif customer_age_days > 365:
            customer_segment = 'Loyal'
        else:
            customer_segment = 'Regular'

        if not is_active and customer_age_days > 180:
            customer_segment = 'Churned'

        try:
            cursor.execute(insert_query, (
                customer_id,
                full_name,
                email.lower(),  # Standardize email
                phone,
                city,
                state,
                country,
                signup_date,
                loyalty_tier,
                total_lifetime_spend,
                is_active,
                customer_age_days,
                customer_segment
            ))
            transformed_count += 1
        except Exception as e:
            print(f"⚠️ Error transforming customer {customer_id}: {e}")
            skipped_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Transformed {transformed_count} customers into silver_customers")
    print(f"⚠️ Skipped {skipped_count} customers due to data quality issues")
    return transformed_count


def transform_orders(**context):
    """Transform orders from Bronze to Silver"""
    print("🔄 Transforming orders: Bronze → Silver")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Fetch orders from Bronze with validation
    cursor.execute("""
                   SELECT o.order_id,
                          o.customer_id,
                          o.product_id,
                          o.quantity,
                          o.unit_price,
                          o.total_amount,
                          o.discount_amount,
                          o.final_amount,
                          o.order_timestamp,
                          o.status,
                          o.payment_method,
                          o.promo_code
                   FROM bronze_orders o
                   WHERE EXISTS (SELECT 1 FROM bronze_customers c WHERE c.customer_id = o.customer_id)
                     AND EXISTS (SELECT 1 FROM bronze_products p WHERE p.product_id = o.product_id)
                     AND o.quantity > 0
                     AND o.unit_price > 0
                   """)

    bronze_orders = cursor.fetchall()
    print(f"📦 Found {len(bronze_orders)} valid orders in Bronze")

    # Transform and insert into Silver
    insert_query = """
                   INSERT INTO silver_orders (order_id, customer_id, product_id, quantity, unit_price, \
                                              total_amount, discount_amount, final_amount, \
                                              order_date, order_time, order_timestamp, \
                                              status, payment_method, promo_code, \
                                              order_hour, order_day_of_week, is_weekend, is_discounted) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                           %s) ON CONFLICT (order_id) DO \
                   UPDATE SET
                       status = EXCLUDED.status, \
                       updated_at = CURRENT_TIMESTAMP \
                   """

    transformed_count = 0
    for row in bronze_orders:
        order_id, customer_id, product_id, quantity, unit_price, total_amount, discount_amount, final_amount, order_timestamp, status, payment_method, promo_code = row

        # Extract date and time components
        order_date = order_timestamp.date()
        order_time = order_timestamp.time()
        order_hour = order_timestamp.hour
        order_day_of_week = order_timestamp.strftime('%A')
        is_weekend = order_timestamp.weekday() >= 5
        is_discounted = discount_amount > 0

        try:
            cursor.execute(insert_query, (
                order_id,
                customer_id,
                product_id,
                quantity,
                unit_price,
                total_amount,
                discount_amount,
                final_amount,
                order_date,
                order_time,
                order_timestamp,
                status,
                payment_method,
                promo_code,
                order_hour,
                order_day_of_week,
                is_weekend,
                is_discounted
            ))
            transformed_count += 1
        except Exception as e:
            print(f"⚠️ Error transforming order {order_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Transformed {transformed_count} orders into silver_orders")
    return transformed_count


def transform_inventory(**context):
    """Transform inventory from Bronze to Silver"""
    print("🔄 Transforming inventory: Bronze → Silver")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Fetch latest inventory from Bronze
    cursor.execute("""
                   SELECT DISTINCT
                   ON (product_id)
                       product_id, stock_quantity, warehouse_location, reorder_point,
                       cost_price, inventory_date
                   FROM bronze_inventory
                   WHERE stock_quantity >= 0
                   ORDER BY product_id, inventory_date DESC, ingestion_timestamp DESC
                   """)

    bronze_inventory = cursor.fetchall()
    print(f"📊 Found {len(bronze_inventory)} inventory records in Bronze")

    # Transform and insert into Silver
    insert_query = """
                   INSERT INTO silver_inventory (product_id, stock_quantity, warehouse_location, reorder_point, \
                                                 cost_price, stock_status, inventory_value, inventory_date) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                   """

    # Clear existing Silver inventory for fresh load
    cursor.execute("DELETE FROM silver_inventory")

    transformed_count = 0
    for row in bronze_inventory:
        product_id, stock_quantity, warehouse_location, reorder_point, cost_price, inventory_date = row

        # Calculate stock status
        stock_status = calculate_stock_status(stock_quantity, reorder_point)

        # Calculate inventory value
        inventory_value = stock_quantity * cost_price if cost_price else 0

        try:
            cursor.execute(insert_query, (
                product_id,
                stock_quantity,
                warehouse_location,
                reorder_point,
                cost_price,
                stock_status,
                inventory_value,
                inventory_date
            ))
            transformed_count += 1
        except Exception as e:
            print(f"⚠️ Error transforming inventory for product {product_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Transformed {transformed_count} inventory records into silver_inventory")
    return transformed_count


def transform_campaigns(**context):
    """Transform campaigns from Bronze to Silver"""
    print("🔄 Transforming campaigns: Bronze → Silver")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Fetch campaigns from Bronze
    cursor.execute("""
                   SELECT campaign_id,
                          campaign_name,
                          channel,
                          promo_code,
                          discount_percentage,
                          start_date,
                          end_date,
                          budget,
                          actual_spend
                   FROM bronze_campaigns
                   """)

    bronze_campaigns = cursor.fetchall()
    print(f"📢 Found {len(bronze_campaigns)} campaigns in Bronze")

    # Transform and insert into Silver
    insert_query = """
                   INSERT INTO silver_campaigns (campaign_id, campaign_name, channel, promo_code, \
                                                 discount_percentage, start_date, end_date, budget, \
                                                 actual_spend, is_active) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (campaign_id) DO \
                   UPDATE SET
                       campaign_name = EXCLUDED.campaign_name, \
                       actual_spend = EXCLUDED.actual_spend, \
                       is_active = EXCLUDED.is_active, \
                       updated_at = CURRENT_TIMESTAMP \
                   """

    transformed_count = 0
    current_date = datetime.now().date()

    for row in bronze_campaigns:
        campaign_id, campaign_name, channel, promo_code, discount_percentage, start_date, end_date, budget, actual_spend = row

        # Determine if campaign is currently active
        is_active = start_date <= current_date <= end_date

        try:
            cursor.execute(insert_query, (
                campaign_id,
                campaign_name,
                channel,
                promo_code,
                discount_percentage,
                start_date,
                end_date,
                budget,
                actual_spend,
                is_active
            ))
            transformed_count += 1
        except Exception as e:
            print(f"⚠️ Error transforming campaign {campaign_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Transformed {transformed_count} campaigns into silver_campaigns")
    return transformed_count


# Define the DAG
# with DAG(
#     dag_id='silver_transformation',
#     default_args=default_args,
#     description='Transform and clean data from Bronze to Silver layer',
#     schedule_interval=None,   # 🔑 IMPORTANT
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=['silver', 'transformation', 'ecommerce'],
# ) as dag:
#
#     transform_products_task = PythonOperator(
#         task_id='transform_products',
#         python_callable=transform_products,
#     )
#
#     transform_customers_task = PythonOperator(
#         task_id='transform_customers',
#         python_callable=transform_customers,
#     )
#
#     transform_orders_task = PythonOperator(
#         task_id='transform_orders',
#         python_callable=transform_orders,
#     )
#
#     transform_inventory_task = PythonOperator(
#         task_id='transform_inventory',
#         python_callable=transform_inventory,
#     )
#
#     transform_campaigns_task = PythonOperator(
#         task_id='transform_campaigns',
#         python_callable=transform_campaigns,
#     )
#
#     [transform_products_task, transform_customers_task] >> transform_orders_task
#     transform_products_task >> transform_inventory_task


# Define the DAG
with DAG(
        dag_id='silver_transformation',
        default_args=default_args,
        description='Transform and clean data from Bronze to Silver layer',
        schedule_interval='*/1 * * * *',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['silver', 'transformation', 'ecommerce'],
) as dag:
    # Wait for Bronze ingestion to complete
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze_ingestion',
        external_dag_id='bronze_ingestion',
        external_task_id=None,  # Wait for entire DAG
        mode='poke',
        timeout=600,
        poke_interval=30
    )

    # Transform tasks
    transform_products_task = PythonOperator(
        task_id='transform_products',
        python_callable=transform_products,
        provide_context=True
    )

    transform_customers_task = PythonOperator(
        task_id='transform_customers',
        python_callable=transform_customers,
        provide_context=True
    )

    transform_orders_task = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_orders,
        provide_context=True
    )

    transform_inventory_task = PythonOperator(
        task_id='transform_inventory',
        python_callable=transform_inventory,
        provide_context=True
    )

    transform_campaigns_task = PythonOperator(
        task_id='transform_campaigns',
        python_callable=transform_campaigns,
        provide_context=True
    )

    # Define dependencies
    wait_for_bronze >> [
        transform_products_task,
        transform_customers_task,
        transform_campaigns_task
    ]

    [transform_products_task, transform_customers_task] >> transform_orders_task
    transform_products_task >> transform_inventory_task