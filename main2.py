"""
Main program to demonstrate the calculations using JSON schema parser
Analyzes all 5 database designs (DB1-DB5) with different denormalization strategies
"""

from typing import Dict, Optional
from models.schema import Schema, Field, Database
from models.collection import Collection
from models.statistics import Statistics
from parsers.schema_parser2 import SchemaParser2
from calculators.size_calculator import SizeCalculator
from calculators.shard_calculator import ShardCalculator


def build_db_from_json(db_index: int, stats: Statistics) -> Database:
    """
    Build a Database instance by loading schemas from JSON file
    
    Args:
        db_index: Database index (1-5)
        stats: Statistics instance
    
    Returns:
        Database instance with all collections
    """
    db = Database(f"DB{db_index}")
    
    # Load all schemas from the corresponding JSON file
    schemas = SchemaParser2.parse_multiple_from_file(f"schemas/db{db_index}.json")
    
    # Map collection names to document counts
    collection_counts = {
        "Product": stats.num_products,
        "Stock": stats.num_stock_entries,
        "Warehouse": stats.num_warehouses,
        "OrderLine": stats.num_order_lines,
        "Client": stats.num_clients
    }
    
    # Create collections for each schema in the JSON file
    for schema_name, schema in schemas.items():
        # Extract collection name from schema name (e.g., "Product_DB1" -> "Product")
        collection_name = schema_name.rsplit('_', 1)[0]
        
        # Get the appropriate document count
        document_count = collection_counts.get(collection_name, 0)
        
        # Create and add collection
        collection = Collection(
            name=collection_name,
            schema=schema,
            document_count=document_count
        )
        db.add_collection(collection)
    
    return db


def get_array_sizes_for_collection(collection_name: str, stats: Statistics) -> Dict[str, int]:
    """
    Get appropriate array sizes for a collection's embedded arrays
    
    Args:
        collection_name: Name of the collection
        stats: Statistics instance
    
    Returns:
        Dictionary of array field names to their average sizes
    """
    array_sizes = {}
    
    if collection_name == "Product":
        array_sizes['categories'] = 2  # Average 2 categories per product
        # Check if this is Product_DB2 with stocks or Product_DB5 with orderLines
        # For DB2: stocks array has 200 warehouses per product
        # For DB5: orderLines array has ~40,000 orderlines per product
    elif collection_name == "Stock":
        array_sizes['categories'] = 2  # For embedded product categories in DB3
    elif collection_name == "OrderLine":
        array_sizes['categories'] = 2  # For embedded product categories in DB4
    
    return array_sizes


def print_db_analysis(db: Database, db_index: int, stats: Statistics, 
                     size_calc: SizeCalculator, shard_calc: ShardCalculator):
    """
    Print complete analysis for a database including sizes and sharding
    
    Args:
        db: Database instance to analyze
        db_index: Database index (1-5)
        stats: Statistics instance
        size_calc: SizeCalculator instance
        shard_calc: ShardCalculator instance
    """
    print("\n" + "=" * 60)
    print(f"Analysis for DB{db_index} (JSON)")
    print("=" * 60)
    
    # Get denormalization signature
    signatures = {
        1: "Prod{[Cat],Supp}, St, Wa, OL, Cl (Normalized)",
        2: "Prod{[Cat], Supp, [St]}, Wa, OL, Cl",
        3: "St{Prod{[Cat], Supp}}, Wa, OL, Cl",
        4: "St, Wa, OL{Prod{[Cat], Supp}}, Cl",
        5: "Prod{[Cat], Supp, [OL]}, St, Wa, Cl"
    }
    print(f"Signature: {signatures.get(db_index, 'Unknown')}")
    
    # Document Sizes
    print("\n### Document Sizes ###\n")
    
    for collection in db.collections.values():
        # Get array sizes for this collection
        array_sizes = get_array_sizes_for_collection(collection.name, stats)
        
        # Special handling for embedded arrays
        if db_index == 2 and collection.name == "Product":
            array_sizes['stocks'] = stats.num_warehouses  # DB2: Product embeds stocks
        elif db_index == 5 and collection.name == "Product":
            array_sizes['orderLines'] = stats.orders_per_customer  # DB5: Product embeds orderlines
        
        doc_size = size_calc.calculate_document_size(collection.schema, array_sizes)
        print(f"{collection.name} document size: {doc_size:,} bytes ({doc_size/1024:.2f} KB)")
    
    # Collection Sizes
    print("\n### Collection Sizes ###\n")
    
    for collection in db.collections.values():
        # Get array sizes for this collection
        array_sizes = get_array_sizes_for_collection(collection.name, stats)
        
        # Special handling for embedded arrays
        if db_index == 2 and collection.name == "Product":
            array_sizes['stocks'] = stats.num_warehouses
        elif db_index == 5 and collection.name == "Product":
            array_sizes['orderLines'] = stats.orders_per_customer
        
        coll_size = size_calc.calculate_collection_size(collection, array_sizes)
        print(f"{collection.name} collection: {size_calc.bytes_to_human_readable(coll_size)}")
    
    # Database Size
    print("\n### Database Size ###\n")
    db_size = size_calc.calculate_database_size(db)
    print(f"DB{db_index} total size: {size_calc.bytes_to_human_readable(db_size)}")
    print(f"DB{db_index} total size: {size_calc.bytes_to_gb(db_size):.2f} GB")
    
    # Sharding Analysis
    print("\n### Sharding Analysis ###\n")
    
    # Stock collection sharding (if present)
    stock_collection = db.get_collection("Stock")
    if stock_collection:
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
    
    # OrderLine collection sharding (if present)
    ol_collection = db.get_collection("OrderLine")
    if ol_collection:
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
            if metrics['skew_warning']:
                print(f"  ⚠️  Warning: Low server utilization!")
    
    # Product collection sharding (if present)
    prod_collection = db.get_collection("Product")
    if prod_collection:
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
            if metrics['skew_warning']:
                print(f"  ⚠️  Warning: Low server utilization!")


def main():
    """Main execution - analyze all 5 database designs"""
    print("=" * 60)
    print("Chapter 2 Homework - NoSQL Database Analysis")
    print("Comparing 5 Database Denormalization Strategies")
    print("=" * 60)
    
    # Initialize statistics
    stats = Statistics()
    
    # Create calculators
    size_calc = SizeCalculator(stats)
    shard_calc = ShardCalculator(stats)
    
    # Analyze each database design (DB1 through DB5)
    for i in range(1, 6):
        db = build_db_from_json(i, stats)
        print_db_analysis(db, i, stats, size_calc, shard_calc)
    
    print("\n" + "=" * 60)
    print("Analysis Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
