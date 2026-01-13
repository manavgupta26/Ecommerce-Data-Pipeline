"""
Data generators for Orders, Customers, Inventory, and Campaigns
"""
from faker import Faker
import random
from datetime import datetime, timedelta
from typing import List, Dict
import json

fake = Faker('en_IN')
# en_IN means fake data for indian names, phone numbers and adresses
#fake.name()        # Ravi Kumar
#fake.phone_number()# +91 98765 43210
#fake.city()        # Bengaluru
Faker.seed(42)  # For reproducibility


class CustomerGenerator:
    """Generate realistic customer data"""

    def __init__(self):
        self.loyalty_tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']
        self.indian_cities = [
            'Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai',
            'Kolkata', 'Pune', 'Ahmedabad', 'Jaipur', 'Surat'
        ]
        self.indian_states = [
            'Maharashtra', 'Delhi', 'Karnataka', 'Telangana', 'Tamil Nadu',
            'West Bengal', 'Gujarat', 'Rajasthan'
        ]

    def generate_customers(self, num_customers: int = 200) -> List[Dict]:
        """Generate specified number of customers"""
        customers = []

        for i in range(1, num_customers + 1):
            signup_date = fake.date_between(start_date='-2y', end_date='-30d')

            # Loyalty tier based on signup age
            days_since_signup = (datetime.now().date() - signup_date).days
            if days_since_signup > 365:
                tier = random.choices(
                    self.loyalty_tiers,
                    weights=[20, 30, 35, 15]
                )[0]
                #if the user is older then 1 year then les chance of bronze
                #Uses weighted random selection:
                # Bronze → 20% Silver → 30% Gold → 35% Platinum → 15%
            else:
                tier = random.choices(
                    self.loyalty_tiers,
                    weights=[60, 30, 8, 2]
                )[0]

            # Lifetime spend correlates with tier
            tier_spend_ranges = {
                'Bronze': (500, 5000),
                'Silver': (5000, 20000),
                'Gold': (20000, 50000),
                'Platinum': (50000, 200000)
            }

            # sett minimum spend according to the tier
            min_spend, max_spend = tier_spend_ranges[tier]

            lifetime_spend = round(random.uniform(min_spend, max_spend), 2)


            customer = {
                'customer_id': f'CUST{i:04d}',
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'street_address': fake.street_address(),
                'city': random.choice(self.indian_cities),
                'state': random.choice(self.indian_states),
                'postal_code': fake.postcode(),
                'country': 'India',
                'signup_date': signup_date.isoformat(),
                'loyalty_tier': tier,
                'total_lifetime_spend': lifetime_spend,
                'is_active': random.choices([True, False], weights=[95, 5])[0]
            }

            customers.append(customer)

        return customers


class OrderGenerator:
    """Generate realistic e-commerce orders"""

    def __init__(self, products: List[Dict], customer_ids: List[str] = None):
        self.products = products
        self.product_ids = [p['id'] for p in products]

        # Use provided customer IDs or generate default range
        if customer_ids:
            self.customer_ids = customer_ids
        else:
            # Fallback: generate range CUST0001 to CUST0200
            self.customer_ids = [f'CUST{i:04d}' for i in range(1, 201)]

        self.statuses = ['completed', 'pending', 'cancelled']
        self.payment_methods = ['credit_card', 'debit_card', 'upi', 'cod', 'wallet']
        self.promo_codes = [None, None, None, 'SAVE10', 'FLASH20', 'NEW50', 'WEEK15']

    def generate_orders(self, num_orders: int = 100, days_back: int = 7) -> List[Dict]:
        """Generate specified number of orders"""
        orders = []

        for i in range(num_orders):
            # Random product
            product = random.choice(self.products)

            # Random REAL customer (from provided list)
            customer_id = random.choice(self.customer_ids)

            # Random quantity (most orders are 1-2 items)
            quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 30, 12, 5, 3])[0]

            # Calculate amounts
            unit_price = product['price']
            total_amount = round(unit_price * quantity, 2)

            # Random promo code
            promo_code = random.choice(self.promo_codes)
            discount_amount = 0
            if promo_code == 'SAVE10':
                discount_amount = round(total_amount * 0.10, 2)
            elif promo_code == 'FLASH20':
                discount_amount = round(total_amount * 0.20, 2)
            elif promo_code == 'NEW50':
                discount_amount = round(total_amount * 0.50, 2)
            elif promo_code == 'WEEK15':
                discount_amount = round(total_amount * 0.15, 2)

            final_amount = round(total_amount - discount_amount, 2)

            # Random timestamp in the last N days
            order_timestamp = fake.date_time_between(
                start_date=f'-{days_back}d',
                end_date='now'
            )

            # Status (90% completed, 8% pending, 2% cancelled)
            status = random.choices(
                self.statuses,
                weights=[90, 8, 2]
            )[0]

            order = {
                'order_id': f'ORD{str(i + 1).zfill(6)}',
                'customer_id': customer_id,  # Now uses REAL customer ID
                'product_id': product['id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'total_amount': total_amount,
                'discount_amount': discount_amount,
                'final_amount': final_amount,
                'order_timestamp': order_timestamp.isoformat(),
                'status': status,
                'payment_method': random.choice(self.payment_methods),
                'promo_code': promo_code,
                'shipping_address': fake.address().replace('\n', ', ')
            }

            orders.append(order)

        return orders


class InventoryGenerator:
    """Generate inventory data for products"""

    def __init__(self, products: List[Dict]):
        self.products = products
        self.warehouses = [
            'Mumbai_WH1', 'Delhi_WH1', 'Bangalore_WH1',
            'Hyderabad_WH1', 'Chennai_WH1'
        ]

    def generate_inventory(self, inventory_date: str = None) -> List[Dict]:
        """Generate inventory snapshot for a given date"""
        if inventory_date is None:
            inventory_date = datetime.now().date().isoformat()

        inventory_records = []

        for product in self.products:
            # Stock quantity (some products might be low/out of stock)
            stock = random.choices(
                [0, random.randint(1, 20), random.randint(20, 100), random.randint(100, 500)],
                weights=[2, 8, 40, 50]
            )[0]

            # Reorder point based on product price
            if product['price'] > 100:
                reorder_point = random.randint(10, 30)
            else:
                reorder_point = random.randint(30, 80)

            # Cost price (60-80% of selling price)
            cost_price = round(product['price'] * random.uniform(0.60, 0.80), 2)

            record = {
                'product_id': product['id'],
                'stock_quantity': stock,
                'warehouse_location': random.choice(self.warehouses),
                'reorder_point': reorder_point,
                'reorder_quantity': reorder_point * 2,
                'cost_price': cost_price,
                'supplier_name': fake.company(),
                'last_restock_date': fake.date_between(start_date='-30d', end_date='today').isoformat(),
                'inventory_date': inventory_date
            }

            inventory_records.append(record)

        return inventory_records


class CampaignGenerator:
    """Generate marketing campaign data"""

    def __init__(self):
        self.channels = ['Email', 'Social Media', 'Google Ads', 'SMS', 'Display Ads']
        self.campaign_templates = [
            ('Flash Friday Sale', 'FLASH20', 20),
            ('Weekend Special', 'WEEK15', 15),
            ('New Customer Offer', 'NEW50', 50),
            ('Festive Bonanza', 'FEST30', 30),
            ('Summer Clearance', 'SUMMER25', 25),
            ('Monsoon Sale', 'RAIN10', 10),
        ]

    def generate_campaigns(self, num_campaigns: int = 5) -> List[Dict]:
        """Generate marketing campaigns"""
        campaigns = []

        for i in range(num_campaigns):
            name, promo_code, discount = random.choice(self.campaign_templates)

            start_date = fake.date_between(start_date='-60d', end_date='-10d')
            end_date = start_date + timedelta(days=random.randint(3, 30))

            budget = round(random.uniform(10000, 100000), 2)
            actual_spend = round(budget * random.uniform(0.70, 1.05), 2)

            campaign = {
                'campaign_id': f'CAMP{i + 1:03d}',
                'campaign_name': f"{name} {start_date.strftime('%b %Y')}",
                'channel': random.choice(self.channels),
                'promo_code': promo_code,
                'discount_percentage': discount,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'budget': budget,
                'actual_spend': actual_spend
            }

            campaigns.append(campaign)

        return campaigns


# Test function
if __name__ == "__main__":
    from api_client import FakeStoreAPIClient

    # Fetch real products
    client = FakeStoreAPIClient()
    products = client.get_all_products()

    if products:
        # Step 1: Generate customers FIRST
        customer_gen = CustomerGenerator()
        customers = customer_gen.generate_customers(num_customers=200)
        print(f"\n👤 Generated {len(customers)} customers")
        print(f"Sample customer: {json.dumps(customers[0], indent=2)}")

        # Step 2: Extract customer IDs
        customer_ids = [c['customer_id'] for c in customers]
        print(f"📋 Customer IDs range: {customer_ids[0]} to {customer_ids[-1]}")

        # Step 3: Generate orders using REAL customer IDs
        order_gen = OrderGenerator(products, customer_ids=customer_ids)
        orders = order_gen.generate_orders(num_orders=500)
        print(f"\n📦 Generated {len(orders)} orders")
        print(f"Sample order: {json.dumps(orders[0], indent=2)}")

        # Step 4: Verify data integrity
        order_customer_ids = set([o['customer_id'] for o in orders])
        customer_ids_set = set(customer_ids)
        print(f"\n✅ Data Integrity Check:")
        print(f"   - All order customer_ids exist in customers? {order_customer_ids.issubset(customer_ids_set)}")
        print(f"   - Unique customers who ordered: {len(order_customer_ids)}/{len(customer_ids)}")

        # Generate inventory
        inventory_gen = InventoryGenerator(products)
        inventory = inventory_gen.generate_inventory()
        print(f"\n📊 Generated {len(inventory)} inventory records")
        print(f"Sample inventory: {json.dumps(inventory[0], indent=2)}")

        # Generate campaigns
        campaign_gen = CampaignGenerator()
        campaigns = campaign_gen.generate_campaigns(num_campaigns=5)
        print(f"\n📢 Generated {len(campaigns)} campaigns")
        print(f"Sample campaign: {json.dumps(campaigns[0], indent=2)}")