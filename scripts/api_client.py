"""
API Client for Fake Store API
"""
import requests
import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FakeStoreAPIClient:
    """Client to interact with Fake Store API"""

    BASE_URL = "https://fakestoreapi.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Ecommerce-Data-Pipeline/1.0'
        })

    def get_all_products(self) -> List[Dict]:
        """Fetch all products from API"""
        try:
            url = f"{self.BASE_URL}/products"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            products = response.json()
            logger.info(f"✅ Fetched {len(products)} products from API")
            return products
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching products: {e}")
            return []

    def get_product_by_id(self, product_id: int) -> Optional[Dict]:
        """Fetch single product by ID"""
        try:
            url = f"{self.BASE_URL}/products/{product_id}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching product {product_id}: {e}")
            return None

    def get_categories(self) -> List[str]:
        """Fetch all product categories"""
        try:
            url = f"{self.BASE_URL}/products/categories"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            categories = response.json()
            logger.info(f"✅ Fetched {len(categories)} categories")
            return categories
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching categories: {e}")
            return []

    def get_users(self) -> List[Dict]:
        """Fetch all users (we'll use as customer base)"""
        try:
            url = f"{self.BASE_URL}/users"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            users = response.json()
            logger.info(f"✅ Fetched {len(users)} users from API")
            return users
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching users: {e}")
            return []


# Test function
if __name__ == "__main__":
    client = FakeStoreAPIClient()

    # Test fetching products
    products = client.get_all_products()
    print(f"\n📦 Sample Product:")
    if products:
        print(products[0])

    # Test fetching categories
    categories = client.get_categories()
    print(f"\n📂 Categories: {categories}")

    # Test fetching users
    users = client.get_users()
    print(f"\n👤 Sample User:")
    if users:
        print(users[0])