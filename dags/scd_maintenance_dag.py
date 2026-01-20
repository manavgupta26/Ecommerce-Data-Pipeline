"""
SCD Type 2 Maintenance DAG
Tracks historical changes in product prices and customer tiers
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def maintain_product_price_scd(**context):
    """Maintain SCD Type 2 for product prices"""
    print("🔄 Maintaining product price history (SCD Type 2)...")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    current_date = datetime.now().date()

    # Get current products from Silver
    cursor.execute("""
                   SELECT product_id, product_name, category, price, price_tier
                   FROM silver_products
                   WHERE is_active = TRUE
                   """)

    current_products = cursor.fetchall()
    print(f"📦 Processing {len(current_products)} products")

    new_records = 0
    updated_records = 0

    for product_id, product_name, category, price, price_tier in current_products:
        # Check if product exists in history
        cursor.execute("""
                       SELECT price_history_id, price, price_tier
                       FROM dim_product_price_history
                       WHERE product_id = %s
                         AND is_current = TRUE
                       """, (product_id,))

        existing = cursor.fetchone()

        if existing is None:
            # New product - insert first record
            cursor.execute("""
                           INSERT INTO dim_product_price_history (product_id, product_name, category, price, price_tier,
                                                                  valid_from, is_current, change_reason)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           """, (
                               product_id, product_name, category, price, price_tier,
                               current_date, True, 'Initial record'
                           ))
            new_records += 1

        else:
            price_history_id, old_price, old_tier = existing

            # Check if price or tier changed
            if old_price != price or old_tier != price_tier:
                # Close current record
                cursor.execute("""
                               UPDATE dim_product_price_history
                               SET valid_to   = %s,
                                   is_current = FALSE,
                                   updated_at = CURRENT_TIMESTAMP
                               WHERE price_history_id = %s
                               """, (current_date - timedelta(days=1), price_history_id))

                # Insert new record
                change_reason = []
                if old_price != price:
                    change_reason.append(f'Price change: ₹{old_price} → ₹{price}')
                if old_tier != price_tier:
                    change_reason.append(f'Tier change: {old_tier} → {price_tier}')

                cursor.execute("""
                               INSERT INTO dim_product_price_history (product_id, product_name, category, price,
                                                                      price_tier,
                                                                      valid_from, is_current, change_reason)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                               """, (
                                   product_id, product_name, category, price, price_tier,
                                   current_date, True, '; '.join(change_reason)
                               ))
                updated_records += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ SCD Update Complete:")
    print(f"   - New products: {new_records}")
    print(f"   - Price/tier changes: {updated_records}")

    return {'new': new_records, 'updated': updated_records}


def maintain_customer_tier_scd(**context):
    """Maintain SCD Type 2 for customer loyalty tiers"""
    print("🔄 Maintaining customer tier history (SCD Type 2)...")

    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    current_date = datetime.now().date()

    # Get current customers from Silver
    cursor.execute("""
                   SELECT customer_id, full_name, email, loyalty_tier, total_lifetime_spend
                   FROM silver_customers
                   WHERE is_active = TRUE
                   """)

    current_customers = cursor.fetchall()
    print(f"👤 Processing {len(current_customers)} customers")

    new_records = 0
    updated_records = 0

    for customer_id, full_name, email, loyalty_tier, total_lifetime_spend in current_customers:
        # Check if customer exists in history
        cursor.execute("""
                       SELECT tier_history_id, loyalty_tier, total_lifetime_spend
                       FROM dim_customer_tier_history
                       WHERE customer_id = %s
                         AND is_current = TRUE
                       """, (customer_id,))

        existing = cursor.fetchone()

        if existing is None:
            # New customer - insert first record
            cursor.execute("""
                           INSERT INTO dim_customer_tier_history (customer_id, full_name, email, loyalty_tier,
                                                                  total_lifetime_spend,
                                                                  valid_from, is_current, change_reason)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           """, (
                               customer_id, full_name, email, loyalty_tier, total_lifetime_spend,
                               current_date, True, 'Initial record'
                           ))
            new_records += 1

        else:
            tier_history_id, old_tier, old_spend = existing

            # Check if tier changed (significant spend increase might trigger tier change)
            if old_tier != loyalty_tier:
                # Close current record
                cursor.execute("""
                               UPDATE dim_customer_tier_history
                               SET valid_to   = %s,
                                   is_current = FALSE,
                                   updated_at = CURRENT_TIMESTAMP
                               WHERE tier_history_id = %s
                               """, (current_date - timedelta(days=1), tier_history_id))

                # Insert new record
                change_reason = f'Tier upgrade: {old_tier} → {loyalty_tier} (LTV: ₹{old_spend} → ₹{total_lifetime_spend})'

                cursor.execute("""
                               INSERT INTO dim_customer_tier_history (customer_id, full_name, email, loyalty_tier,
                                                                      total_lifetime_spend,
                                                                      valid_from, is_current, change_reason)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                               """, (
                                   customer_id, full_name, email, loyalty_tier, total_lifetime_spend,
                                   current_date, True, change_reason
                               ))
                updated_records += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ SCD Update Complete:")
    print(f"   - New customers: {new_records}")
    print(f"   - Tier changes: {updated_records}")

    return {'new': new_records, 'updated': updated_records}


# Define the DAG
with DAG(
        dag_id='scd_maintenance',
        default_args=default_args,
        description='Maintain SCD Type 2 historical dimensions',
        schedule_interval='@daily',
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['scd', 'dimensions', 'history'],
) as dag:
    maintain_product_prices = PythonOperator(
        task_id='maintain_product_price_history',
        python_callable=maintain_product_price_scd,
        provide_context=True
    )

    maintain_customer_tiers = PythonOperator(
        task_id='maintain_customer_tier_history',
        python_callable=maintain_customer_tier_scd,
        provide_context=True
    )

    # Both can run in parallel
    [maintain_product_prices, maintain_customer_tiers]