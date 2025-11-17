"""
Main program to demonstrate the calculations
"""

import json
from models.schema import Schema, Field, Database
from models.collection import Collection
from models.statistics import Statistics
from parsers.schema_parser import SchemaParser
from calculators.size_calculator import SizeCalculator
from calculators.shard_calculator import ShardCalculator

def create_product_schema() -> Schema:
    """Create Product schema programmatically"""
    schema = Schema("Product")
    
    # Basic fields
    schema.add_field(Field("IDP", "integer"))
    schema.add_field(Field("name", "string"))
    schema.add_field(Field("brand", "string"))
    schema.add_field(Field("description", "longstring"))
    schema.add_field(Field("image_url", "string"))
    
    # Price nested object
    price_schema = Schema("price")
    price_schema.add_field(Field("amount", "number"))
    price_schema.add_field(Field("currency", "string"))
    price_schema.add_field(Field("vat_rate", "number"))
    schema.add_field(Field("price", "object", nested_schema=price_schema))
    
    # Categories array
    cat_schema = Schema("category")
    cat_schema.add_field(Field("title", "string"))
    schema.add_field(Field("categories", "array", array_item_schema=cat_schema))
    
    # Supplier nested object
    supp_schema = Schema("supplier")
    supp_schema.add_field(Field("IDS", "integer"))
    supp_schema.add_field(Field("name", "string"))
    supp_schema.add_field(Field("SIRET", "string"))
    supp_schema.add_field(Field("headOffice", "string"))
    supp_schema.add_field(Field("revenue", "integer"))
    schema.add_field(Field("supplier", "object", nested_schema=supp_schema))
    
    return schema

def create_stock_schema() -> Schema:
    """Create Stock schema"""
    schema = Schema("Stock")
    schema.add_field(Field("IDP", "integer"))
    schema.add_field(Field("IDW", "integer"))
    schema.add_field(Field("quantity", "integer"))
    schema.add_field(Field("location", "string"))
    return schema

def create_warehouse_schema() -> Schema:
    """Create Warehouse schema"""
    schema = Schema("Warehouse")
    schema.add_field(Field("IDW", "integer"))
    schema.add_field(Field("address", "string"))
    schema.add_field(Field("capacity", "integer"))
    return schema

def create_orderline_schema() -> Schema:
    """Create OrderLine schema"""
    schema = Schema("OrderLine")
    schema.add_field(Field("IDC", "integer"))
    schema.add_field(Field("IDP", "integer"))
    schema.add_field(Field("date", "date"))
    schema.add_field(Field("quantity", "integer"))
    schema.add_field(Field("deliveryDate", "date"))
    schema.add_field(Field("comment", "longstring"))
    schema.add_field(Field("grade", "integer"))
    return schema

def create_client_schema() -> Schema:
    """Create Client schema"""
    schema = Schema("Client")
    schema.add_field(Field("IDC", "integer"))
    schema.add_field(Field("ln", "string"))
    schema.add_field(Field("fn", "string"))
    schema.add_field(Field("address", "longstring"))
    schema.add_field(Field("nationality", "string"))
    schema.add_field(Field("birthDate", "date"))
    schema.add_field(Field("email", "string"))
    return schema

def create_db1(stats: Statistics) -> Database:
    """Create DB1: Prod{[Cat],Supp}, St, Wa, OL, Cl"""
    db = Database("DB1")
    
    # Product collection
    prod_schema = create_product_schema()
    prod_collection = Collection(
        name="Product",
        schema=prod_schema,
        document_count=stats.num_products
    )
    db.add_collection(prod_collection)
    
    # Stock collection
    stock_schema = create_stock_schema()
    stock_collection = Collection(
        name="Stock",
        schema=stock_schema,
        document_count=stats.num_stock_entries
    )
    db.add_collection(stock_collection)
    
    # Warehouse collection
    warehouse_schema = create_warehouse_schema()
    warehouse_collection = Collection(
        name="Warehouse",
        schema=warehouse_schema,
        document_count=stats.num_warehouses
    )
    db.add_collection(warehouse_collection)
    
    # OrderLine collection
    ol_schema = create_orderline_schema()
    ol_collection = Collection(
        name="OrderLine",
        schema=ol_schema,
        document_count=stats.num_order_lines
    )
    db.add_collection(ol_collection)
    
    # Client collection
    client_schema = create_client_schema()
    client_collection = Collection(
        name="Client",
        schema=client_schema,
        document_count=stats.num_clients
    )
    db.add_collection(client_collection)
    
    return db

def main():
    """Main execution"""
    print("=" * 60)
    print("Chapter 2 Homework - NoSQL Database Analysis")
    print("=" * 60)
    
    # Initialize statistics
    stats = Statistics()
    
    # Create calculators
    size_calc = SizeCalculator(stats)
    shard_calc = ShardCalculator(stats)
    
    # Create DB1
    db1 = create_db1(stats)
    
    print("\n### Document Sizes ###\n")
    
    # Calculate Product document size
    prod_collection = db1.get_collection("Product")
    prod_doc_size = size_calc.calculate_document_size(
        prod_collection.schema,
        array_sizes={'categories': 2}  # Avg 2 categories
    )
    print(f"Product document size: {prod_doc_size} bytes ({prod_doc_size/1024:.2f} KB)")
    
    # Calculate Stock document size
    stock_collection = db1.get_collection("Stock")
    stock_doc_size = size_calc.calculate_document_size(stock_collection.schema)
    print(f"Stock document size: {stock_doc_size} bytes")
    
    # Calculate all collection sizes
    print("\n### Collection Sizes ###\n")
    
    # Product
    prod_size = size_calc.calculate_collection_size(
        prod_collection,
        array_sizes={'categories': 2}
    )
    print(f"Product collection: {size_calc.bytes_to_human_readable(prod_size)}")
    
    # Stock
    stock_size = size_calc.calculate_collection_size(stock_collection)
    print(f"Stock collection: {size_calc.bytes_to_human_readable(stock_size)}")
    
    # Warehouse
    warehouse_collection = db1.get_collection("Warehouse")
    warehouse_size = size_calc.calculate_collection_size(warehouse_collection)
    print(f"Warehouse collection: {size_calc.bytes_to_human_readable(warehouse_size)}")
    
    # OrderLine
    ol_collection = db1.get_collection("OrderLine")
    ol_size = size_calc.calculate_collection_size(ol_collection)
    print(f"OrderLine collection: {size_calc.bytes_to_human_readable(ol_size)}")
    
    # Client
    client_collection = db1.get_collection("Client")
    client_size = size_calc.calculate_collection_size(client_collection)
    print(f"Client collection: {size_calc.bytes_to_human_readable(client_size)}")
    
    # Database size
    print("\n### Database Size ###\n")
    db_size = size_calc.calculate_database_size(db1)
    print(f"DB1 total size: {size_calc.bytes_to_human_readable(db_size)}")
    print(f"DB1 total size: {size_calc.bytes_to_gb(db_size):.2f} GB")
    
    # Sharding analysis
    print("\n### Sharding Analysis ###\n")
    
    # Stock sharding strategies
    print("Stock Collection Sharding:\n")
    stock_strategies = {
        'IDP': stats.num_products,
        'IDW': stats.num_warehouses
    }
    stock_results = shard_calc.compare_sharding_strategies(
        stock_collection,
        stock_strategies
    )
    
    for key, metrics in stock_results.items():
        print(f"\nSharding by {key}:")
        print(f"  Avg docs/server: {metrics['avg_docs_per_server']:,.0f}")
        print(f"  Avg distinct values/server: {metrics['avg_distinct_per_server']:.1f}")
        print(f"  Server utilization: {metrics['server_utilization']*100:.1f}%")
        if metrics['skew_warning']:
            print(f"  ⚠️  Warning: Low server utilization!")
    
    # OrderLine sharding strategies
    print("\n\nOrderLine Collection Sharding:\n")
    ol_strategies = {
        'IDC': stats.num_clients,
        'IDP': stats.num_products
    }
    ol_results = shard_calc.compare_sharding_strategies(
        ol_collection,
        ol_strategies
    )
    
    for key, metrics in ol_results.items():
        print(f"\nSharding by {key}:")
        print(f"  Avg docs/server: {metrics['avg_docs_per_server']:,.0f}")
        print(f"  Avg distinct values/server: {metrics['avg_distinct_per_server']:,.0f}")
        print(f"  Server utilization: {metrics['server_utilization']*100:.1f}%")
    
    # Product sharding strategies
    print("\n\nProduct Collection Sharding:\n")
    prod_strategies = {
        'IDP': stats.num_products,
        'brand': stats.num_brands
    }
    prod_results = shard_calc.compare_sharding_strategies(
        prod_collection,
        prod_strategies
    )
    
    for key, metrics in prod_results.items():
        print(f"\nSharding by {key}:")
        print(f"  Avg docs/server: {metrics['avg_docs_per_server']:,.1f}")
        print(f"  Avg distinct values/server: {metrics['avg_distinct_per_server']:.1f}")
        print(f"  Server utilization: {metrics['server_utilization']*100:.1f}%")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()