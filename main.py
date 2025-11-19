"""
Main program to demonstrate the calculations using JSON schema parser
Analyzes all 5 database designs (DB1-DB5) with different denormalization strategies
"""

from typing import Dict, Optional
from models.schema import Schema, Field, Database, Collection
from models.statistics import Statistics
from parsers.schema_parser import SchemaParser
from calculators.size_calculator import SizeCalculator
from calculators.shard_calculator import ShardCalculator


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
    """
    elif collection_name == "Stock":
        array_sizes['categories'] = 2  # For embedded product categories in DB3
    elif collection_name == "OrderLine":
        array_sizes['categories'] = 2  # For embedded product categories in DB4
    
    return array_sizes
    """


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
    


def main():
    """Main execution - analyze all 5 database designs"""
    
    # Initialize statistics
    stats = Statistics()
    
    # Create calculators
    size_calc = SizeCalculator(stats)
    shard_calc = ShardCalculator(stats)
    
    # Demander à l'utilisateur de choisir une base de données
    while True:
        try:
            db_choice = int(input("\nEntrez un numéro de base de données (1-5) ou 0 pour analyser toutes : "))
            if 0 <= db_choice <= 5:
                break
            else:
                print("Veuillez entrer un nombre entre 0 et 5.")
        except ValueError:
            print("Entrée invalide. Veuillez entrer un nombre.")
    
    if db_choice == 0:
        # Analyser toutes les bases de données
        for i in range(1, 6):
            db = SchemaParser.build_db_from_json(i, stats)
            print_db_analysis(db, i, stats, size_calc, shard_calc)
    else:
        # Analyser la base de données choisie
        db = SchemaParser.build_db_from_json(db_choice, stats)
        print_db_analysis(db, db_choice, stats, size_calc, shard_calc)

if __name__ == "__main__":
    main()
