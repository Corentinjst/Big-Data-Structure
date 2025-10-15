"""
Statistics for the database
"""

from dataclasses import dataclass
from typing import Dict

@dataclass
class Statistics:
    """Database statistics"""
    num_clients: int = 10_000_000  # 10^7
    num_products: int = 100_000     # 10^5
    num_order_lines: int = 4_000_000_000  # 4*10^9
    num_warehouses: int = 200
    
    # Derived statistics
    orders_per_customer: int = 100
    products_per_customer: int = 20
    categories_per_product_avg: int = 2
    num_brands: int = 5_000
    products_per_brand_apple: int = 50
    num_dates: int = 365
    num_servers: int = 1_000
    
    # Custom statistics
    custom_stats: Dict[str, int] = None
    
    def __post_init__(self):
        if self.custom_stats is None:
            self.custom_stats = {}
        
        # Calculate derived statistics
        self.num_stock_entries = self.num_products * self.num_warehouses
        self.avg_order_lines_per_product = self.num_order_lines // self.num_products
    
    def get_stat(self, key: str) -> int:
        """Get a statistic by key"""
        if hasattr(self, key):
            return getattr(self, key)
        return self.custom_stats.get(key, 0)