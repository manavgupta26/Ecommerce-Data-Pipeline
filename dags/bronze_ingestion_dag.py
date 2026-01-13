"""
Bronze Layer Ingestion DAG
Fetches data from sources and loads into Bronze tables
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import sys
import json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')

from api_client import FakeStoreAPIClient
from data_generators import (
    CustomerGenerator,
    OrderGenerator,
    InventoryGenerator,
    CampaignGenerator
)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def fetch_and_load_products(**context):
    """Fetch products from Fake Store API and load to Bronze"""
    print("🔍 Fetching products from Fake Store API...")

    # Fetch products
    client = FakeStoreAPIClient()
    products = client.get_all_products()

    if not products:
        raise Exception("❌ Failed to fetch products from API")

    print(f"✅ Fetched {len(products)} products")

    # Get database connection
    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get pipeline run ID from context
    pipeline_run_id = context['run_id']

    # Insert products into Bronze table
    insert_query = """
                   INSERT INTO bronze_products (product_id, title, price, description, category, \
                                                image_url, rating_rate, rating_count, \
                                                source, ingestion_timestamp, pipeline_run_id, raw_json) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                           %s) ON CONFLICT (product_id) DO NOTHING \
                   """

    inserted_count = 0
    for product in products:
        try:
            cursor.execute(insert_query, (
                product['id'],
                product['title'],
                product['price'],
                product.get('description'),
                product['category'],
                product.get('image'),
                product.get('rating', {}).get('rate'),
                product.get('rating', {}).get('count'),
                'fakestoreapi',
                datetime.now(),
                pipeline_run_id,
                json.dumps(product)
            ))
            inserted_count += 1
        except Exception as e:
            print(f"⚠️ Error inserting product {product['id']}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {inserted_count} products into bronze_products")

    # Push products to XCom for downstream tasks
    context['task_instance'].xcom_push(key='products', value=products)

    return inserted_count


def generate_and_load_customers(**context):
    """Generate customer data and load to Bronze"""
    print("👤 Generating customer data...")

    # Generate customers
    customer_gen = CustomerGenerator()
    customers = customer_gen.generate_customers(num_customers=200)

    print(f"✅ Generated {len(customers)} customers")

    # Get database connection
    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    pipeline_run_id = context['run_id']

    # Insert customers into Bronze table
    insert_query = """
                   INSERT INTO bronze_customers (customer_id, first_name, last_name, email, phone, \
                                                 street_address, city, state, postal_code, country, \
                                                 signup_date, loyalty_tier, total_lifetime_spend, is_active, \
                                                 source, ingestion_timestamp, pipeline_run_id) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                           %s) ON CONFLICT (customer_id) DO \
                   UPDATE SET
                       total_lifetime_spend = EXCLUDED.total_lifetime_spend, \
                       loyalty_tier = EXCLUDED.loyalty_tier, \
                       is_active = EXCLUDED.is_active, \
                       last_modified_date = CURRENT_TIMESTAMP \
                   """

    inserted_count = 0
    for customer in customers:
        try:
            cursor.execute(insert_query, (
                customer['customer_id'],
                customer['first_name'],
                customer['last_name'],
                customer['email'],
                customer['phone'],
                customer['street_address'],
                customer['city'],
                customer['state'],
                customer['postal_code'],
                customer['country'],
                customer['signup_date'],
                customer['loyalty_tier'],
                customer['total_lifetime_spend'],
                customer['is_active'],
                'customer_generator',
                datetime.now(),
                pipeline_run_id
            ))
            inserted_count += 1
        except Exception as e:
            print(f"⚠️ Error inserting customer {customer['customer_id']}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {inserted_count} customers into bronze_customers")

    # Push customer IDs to XCom for orders task
    customer_ids = [c['customer_id'] for c in customers]
    context['task_instance'].xcom_push(key='customer_ids', value=customer_ids)

    return inserted_count


def generate_and_load_orders(**context):
    """Generate order data and load to Bronze"""
    print("📦 Generating order data...")

    # Pull products and customer_ids from XCom
    task_instance = context['task_instance']
    products = task_instance.xcom_pull(task_ids='fetch_products', key='products')
    customer_ids = task_instance.xcom_pull(task_ids='generate_customers', key='customer_ids')

    if not products:
        raise Exception("❌ No products found in XCom")
    if not customer_ids:
        raise Exception("❌ No customer IDs found in XCom")

    # Generate orders
    order_gen = OrderGenerator(products, customer_ids=customer_ids)
    orders = order_gen.generate_orders(num_orders=500, days_back=30)

    print(f"✅ Generated {len(orders)} orders")

    # Get database connection
    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    pipeline_run_id = context['run_id']

    # Insert orders into Bronze table
    insert_query = """
                   INSERT INTO bronze_orders (order_id, customer_id, product_id, quantity, unit_price, \
                                              total_amount, discount_amount, final_amount, order_timestamp, \
                                              status, payment_method, promo_code, shipping_address, \
                                              source, ingestion_timestamp, pipeline_run_id, raw_json) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                           %s) ON CONFLICT (order_id) DO NOTHING \
                   """

    inserted_count = 0
    for order in orders:
        try:
            cursor.execute(insert_query, (
                order['order_id'],
                order['customer_id'],
                order['product_id'],
                order['quantity'],
                order['unit_price'],
                order['total_amount'],
                order['discount_amount'],
                order['final_amount'],
                order['order_timestamp'],
                order['status'],
                order['payment_method'],
                order['promo_code'],
                order['shipping_address'],
                'order_generator',
                datetime.now(),
                pipeline_run_id,
                json.dumps(order)
            ))
            inserted_count += 1
        except Exception as e:
            print(f"⚠️ Error inserting order {order['order_id']}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {inserted_count} orders into bronze_orders")

    return inserted_count


def generate_and_load_inventory(**context):
    """Generate inventory data and load to Bronze"""
    print("📊 Generating inventory data...")

    # Pull products from XCom
    task_instance = context['task_instance']
    products = task_instance.xcom_pull(task_ids='fetch_products', key='products')

    if not products:
        raise Exception("❌ No products found in XCom")

    # Generate inventory
    inventory_gen = InventoryGenerator(products)
    inventory_records = inventory_gen.generate_inventory()

    print(f"✅ Generated {len(inventory_records)} inventory records")

    # Get database connection
    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    pipeline_run_id = context['run_id']

    # Insert inventory into Bronze table
    insert_query = """
                   INSERT INTO bronze_inventory (product_id, stock_quantity, warehouse_location, reorder_point, \
                                                 reorder_quantity, cost_price, supplier_name, last_restock_date, \
                                                 inventory_date, source, ingestion_timestamp, pipeline_run_id) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                   """

    inserted_count = 0
    for record in inventory_records:
        try:
            cursor.execute(insert_query, (
                record['product_id'],
                record['stock_quantity'],
                record['warehouse_location'],
                record['reorder_point'],
                record['reorder_quantity'],
                record['cost_price'],
                record['supplier_name'],
                record['last_restock_date'],
                record['inventory_date'],
                'inventory_generator',
                datetime.now(),
                pipeline_run_id
            ))
            inserted_count += 1
        except Exception as e:
            print(f"⚠️ Error inserting inventory for product {record['product_id']}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {inserted_count} inventory records into bronze_inventory")

    return inserted_count


def generate_and_load_campaigns(**context):
    """Generate campaign data and load to Bronze"""
    print("📢 Generating campaign data...")

    # Generate campaigns
    campaign_gen = CampaignGenerator()
    campaigns = campaign_gen.generate_campaigns(num_campaigns=5)

    print(f"✅ Generated {len(campaigns)} campaigns")

    # Get database connection
    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    pipeline_run_id = context['run_id']

    # Insert campaigns into Bronze table
    insert_query = """
                   INSERT INTO bronze_campaigns (campaign_id, campaign_name, channel, promo_code, \
                                                 discount_percentage, start_date, end_date, budget, \
                                                 actual_spend, source, ingestion_timestamp, pipeline_run_id) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (campaign_id) DO \
                   UPDATE SET
                       actual_spend = EXCLUDED.actual_spend, \
                       ingestion_timestamp = EXCLUDED.ingestion_timestamp \
                   """

    inserted_count = 0
    for campaign in campaigns:
        try:
            cursor.execute(insert_query, (
                campaign['campaign_id'],
                campaign['campaign_name'],
                campaign['channel'],
                campaign['promo_code'],
                campaign['discount_percentage'],
                campaign['start_date'],
                campaign['end_date'],
                campaign['budget'],
                campaign['actual_spend'],
                'campaign_generator',
                datetime.now(),
                pipeline_run_id
            ))
            inserted_count += 1
        except Exception as e:
            print(f"⚠️ Error inserting campaign {campaign['campaign_id']}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {inserted_count} campaigns into bronze_campaigns")

    return inserted_count


# Define the DAG
with DAG(
        dag_id='bronze_ingestion',
        default_args=default_args,
        description='Ingest data from sources into Bronze layer',
        schedule_interval='*/1 * * * *',  # Run once per day
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['bronze', 'ingestion', 'ecommerce'],
) as dag:
    # Task 1: Fetch products from API
    fetch_products = PythonOperator(
        task_id='fetch_products',
        python_callable=fetch_and_load_products,
        provide_context=True
    )

    # Task 2: Generate and load customers
    generate_customers = PythonOperator(
        task_id='generate_customers',
        python_callable=generate_and_load_customers,
        provide_context=True
    )

    # Task 3: Generate and load orders (depends on products and customers)
    generate_orders = PythonOperator(
        task_id='generate_orders',
        python_callable=generate_and_load_orders,
        provide_context=True
    )

    # Task 4: Generate and load inventory (depends on products)
    generate_inventory = PythonOperator(
        task_id='generate_inventory',
        python_callable=generate_and_load_inventory,
        provide_context=True
    )

    # Task 5: Generate and load campaigns
    generate_campaigns = PythonOperator(
        task_id='generate_campaigns',
        python_callable=generate_and_load_campaigns,
        provide_context=True
    )

    # Define task dependencies
    fetch_products >> generate_customers
    [fetch_products, generate_customers] >> generate_orders
    fetch_products >> generate_inventory
    generate_campaigns  # Independent task


# with DAG(
#     dag_id='bronze_ingestion',
#     default_args=default_args,
#     description='Ingest data from sources into Bronze layer',
#     schedule_interval='@daily',
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=['bronze', 'ingestion', 'ecommerce'],
# ) as dag:
#
#     fetch_products = PythonOperator(
#         task_id='fetch_products',
#         python_callable=fetch_and_load_products,
#     )
#
#     generate_customers = PythonOperator(
#         task_id='generate_customers',
#         python_callable=generate_and_load_customers,
#     )
#
#     generate_orders = PythonOperator(
#         task_id='generate_orders',
#         python_callable=generate_and_load_orders,
#     )
#
#     generate_inventory = PythonOperator(
#         task_id='generate_inventory',
#         python_callable=generate_and_load_inventory,
#     )
#
#     generate_campaigns = PythonOperator(
#         task_id='generate_campaigns',
#         python_callable=generate_and_load_campaigns,
#     )
#
#     trigger_silver = TriggerDagRunOperator(
#         task_id="trigger_silver_transformation",
#         trigger_dag_id="silver_transformation",
#     )
#
#     fetch_products >> generate_customers
#     [fetch_products, generate_customers] >> generate_orders
#     fetch_products >> generate_inventory
#
#     [generate_orders, generate_inventory, generate_campaigns] >> trigger_silver
