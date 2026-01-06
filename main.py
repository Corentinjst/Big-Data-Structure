"""
Main program to demonstrate the calculations using JSON schema parser
Analyzes all 5 database designs (DB1-DB5) with different denormalization strategies
Includes filter and join query testing capabilities
"""

from typing import Dict, Optional
from models.schema import Schema, Field, Database, Collection
from models.statistics import Statistics
from parsers.schema_parser import SchemaParser
from calculators.size_calculator import SizeCalculator
from calculators.shard_calculator import ShardCalculator
from operators import QueryExecutor


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

    # Get Array Average sizes
    avg_sizes = {
        1: {"categories": 2},
        2: {"categories": 2, "stocks": 200},
        3: {"categories": 2},
        4: {"categories": 2},
        5: {"categories": 2, "orderLines": 5}
    }
    array_sizes = avg_sizes.get(db_index)
    print(f"Average array sizes: {array_sizes}")
    
    # Document Sizes
    print("\n### Document Sizes ###\n")
    
    for collection in db.collections.values():
        doc_size = size_calc.calculate_document_size(collection.schema, array_sizes)
        print(f"{collection.name} document size: {doc_size:,} bytes")
    
    # Collection Sizes
    print("\n### Collection Sizes ###\n")
    
    for collection in db.collections.values():        
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


def print_filter_result(query_name: str, result, sharding_strategy: str):
    """Print filter query results in TD2 correction format"""
    print(f"\n{'='*70}")
    print(f"{query_name} - Sharding Strategy: {sharding_strategy}")
    print(f"{'='*70}")

    print("\n--- TD2 Correction Format ---")
    print(f"{'Column':<15} {'Value':<20}")
    print(f"{'-'*35}")
    print(f"{'Sharding':<15} {sharding_strategy}")
    print(f"{'S1 (docs)':<15} {result.s1:,}")
    print(f"{'O1 (docs)':<15} {result.o1:,}")
    print(f"Input Size: {result.input_doc_size_bytes:,} bytes ({result.input_doc_size_bytes/1024/1024:.2f} MB)")
    print(f"Output Size: {result.output_size_bytes:,} bytes ({result.output_size_bytes/1024/1024:.2f} MB)")
    print(f"{'C1 (bytes)':<15} {result.c1_volume_bytes:,} ({result.c1_volume_bytes/1024/1024:.2f} MB)")    

    print(f"\n--- Costs ---")
    print(result.cost)


def print_aggregate_result(query_name: str, result, sharding_strategy: str):
    """Print join query results in TD2 correction format"""
    print(f"\n{'='*70}")
    print(f"{query_name} - Sharding Strategy: {sharding_strategy}")
    print(f"Join Key: {result.join_key}")
    print(f"{'='*70}")

    print("\n--- TD2 Correction Format ---")
    print(f"{'Column':<20} {'Value':<25}")
    print(f"{'-'*45}")
    print(f"{'Sharding':<20} {sharding_strategy}")

    # C1 section
    print(f"\n{'--- C1 Phase ---':<20}")
    print(f"Group_By Key: {result.right_group_by_key}")
    print(f"{'S1 (docs)':<20} {result.s1:,}")
    print(f"{'O1 (docs)':<20} {result.o1:,}")
    print(f"{'Shuffle1 (docs)':<20} {result.shuffle1:,}")
    print(f"Input Size: {result.input_size_bytes1:,} bytes ({result.input_size_bytes1/1024/1024:.2f} MB)")
    print(f"Output Size: {result.output_size_bytes1:,} bytes ({result.output_size_bytes1/1024/1024:.2f} MB)")
    print(f"Shuffle Size: {result.shuffle_size_bytes1:,} bytes ({result.shuffle_size_bytes1/1024/1024:.2f} MB)")

    # C2 section
    print(f"\n{'--- C2 Phase ---':<20}")
    print(f"{'Loops':<20} {result.num_loops:,}")
    if(result.left_group_by_key):print(f"Group_By Key: {result.left_group_by_key}")
    print(f"{'S2 (docs)':<20} {result.s2:,}")
    print(f"{'O2 (docs)':<20} {result.o2:,}")
    print(f"{'Shuffle2 (docs)':<20} {result.shuffle2:,}")
    print(f"Input Size: {result.input_size_bytes2:,} bytes ({result.input_size_bytes2/1024/1024:.2f} MB)")
    print(f"Output Size: {result.output_size_bytes2:,} bytes ({result.output_size_bytes2/1024/1024:.2f} MB)")
    print(f"Shuffle Size: {result.shuffle_size_bytes2:,} bytes ({result.shuffle_size_bytes2/1024/1024:.2f} MB)")

    # Volumes
    print(f"\n{'--- Volumes ---':<20}")
    print(f"{'C1 (bytes)':<20} {result.c1_volume_bytes:,} ({result.c1_volume_bytes/1024/1024:.4f} MB)")
    print(f"{'C2 (bytes)':<20} {result.c2_volume_bytes:,} ({result.c2_volume_bytes/1024/1024:.4f} MB)")
    print(f"{'Total Vt':<20} {result.c1_volume_bytes + result.num_loops * result.c2_volume_bytes:,} bytes")
    
    # Costs
    print(f"\n--- Costs ---")
    print(result.cost)



def print_join_result(query_name: str, result, sharding_strategy: str):
    """Print join query results in TD2 correction format"""
    print(f"\n{'='*70}")
    print(f"{query_name} - Sharding Strategy: {sharding_strategy}")
    print(f"Join Key: {result.join_key}")
    print(f"{'='*70}")

    print("\n--- TD2 Correction Format ---")
    print(f"{'Column':<20} {'Value':<25}")
    print(f"{'-'*45}")
    print(f"{'Sharding':<20} {sharding_strategy}")

    # C1 section
    print(f"\n{'--- C1 Phase ---':<20}")
    print(f"{'S1 (docs)':<20} {result.s1:,}")
    print(f"{'O1 (docs)':<20} {result.o1:,}")
    print(f"Input Size: {result.input_size_bytes1:,} bytes ({result.input_size_bytes1/1024/1024:.2f} MB)")
    print(f"Output Size: {result.output_size_bytes1:,} bytes ({result.output_size_bytes1/1024/1024:.2f} MB)")

    # C2 section
    print(f"\n{'--- C2 Phase ---':<20}")
    print(f"{'Loops':<20} {result.num_loops:,}")
    print(f"{'S2 (docs)':<20} {result.s2:,}")
    print(f"{'O2 (docs)':<20} {result.o2:,}")
    print(f"Input Size: {result.input_size_bytes2:,} bytes ({result.input_size_bytes2/1024/1024:.2f} MB)")
    print(f"Output Size: {result.output_size_bytes2:,} bytes ({result.output_size_bytes2/1024/1024:.2f} MB)")

    # Volumes
    print(f"\n{'--- Volumes ---':<20}")
    print(f"{'C1 (bytes)':<20} {result.c1_volume_bytes:,} ({result.c1_volume_bytes/1024/1024:.4f} MB)")
    print(f"{'C2 (bytes)':<20} {result.c2_volume_bytes:,} ({result.c2_volume_bytes/1024/1024:.4f} MB)")
    print(f"{'Total Vt':<20} {result.c1_volume_bytes + result.num_loops * result.c2_volume_bytes:,} bytes")
    
    # Costs
    print(f"\n--- Costs ---")
    print(result.cost)


def run_query_tests(db_num: int, query_choice: str):
    """Run query tests for specified database and query"""
    stats = Statistics()
    db = SchemaParser.build_db_from_json(db_num, stats, f"schemas/db{db_num}.json")
    executor = QueryExecutor(db, stats)
    
    array_sizes = {
        1: {"categories": 2},
        2: {"categories": 2, "stocks": 200},
        3: {"categories": 2},
        4: {"categories": 2},
        5: {"categories": 2, "orderLines": 5}
    }
    
    if query_choice in ['1', 'all']:
        # Q1: Stock query
        print(f"\n{'#'*70}")
        print(f"# Testing Q1 on DB{db_num}")
        print(f"{'#'*70}")
        
        sharding_strategies = [
            ("Stock sharded by IDP", {"Stock": "IDP"}),
            ("Stock sharded by IDW", {"Stock": "IDW"}),
        ]
        
        for strategy_name, sharding_dict in sharding_strategies:
            result = executor.execute_q1(
                sharding_strategy=sharding_dict,
                array_sizes=array_sizes.get(db_num)
            )
            print_filter_result("Q1", result, strategy_name)
    
    if query_choice in ['2', 'all']:
        # Q2: Product brand query
        print(f"\n{'#'*70}")
        print(f"# Testing Q2 on DB{db_num}")
        print(f"{'#'*70}")
        
        sharding_strategies = [
            ("Product sharded by brand", {"Product": "brand"}),
            ("Product sharded by IDP", {"Product": "IDP"}),
        ]
        
        for strategy_name, sharding_dict in sharding_strategies:
            result = executor.execute_q2(
                brand="Apple",
                sharding_strategy=sharding_dict,
                array_sizes=array_sizes.get(db_num)
            )
            print_filter_result("Q2", result, strategy_name)
    
    if query_choice in ['3', 'all']:
        # Q3: OrderLine date query
        print(f"\n{'#'*70}")
        print(f"# Testing Q3 on DB{db_num}")
        print(f"{'#'*70}")
        
        sharding_strategies = [
            ("OrderLine sharded by IDC", {"OrderLine": "IDC"}),
            ("OrderLine sharded by IDP", {"OrderLine": "IDP"}),
        ]
        
        for strategy_name, sharding_dict in sharding_strategies:
            result = executor.execute_q3(
                sharding_strategy=sharding_dict,
                array_sizes=array_sizes.get(db_num)
            )
            print_filter_result("Q3", result, strategy_name)
    
    if query_choice in ['4', 'all']:
        # Q4: Stock join query
        print(f"\n{'#'*70}")
        print(f"# Testing Q4 on DB{db_num}")
        print(f"{'#'*70}")
        
        sharding_strategies = [
            ("Stock(IDW), Product(IDP)", {"Stock": "IDW", "Product": "IDP"}),
            ("Stock(IDP), Product(IDP)", {"Stock": "IDP", "Product": "IDP"}),
        ]
        
        for strategy_name, sharding_dict in sharding_strategies:
            result = executor.execute_q4(
                sharding_strategy=sharding_dict,
                array_sizes=array_sizes.get(db_num)
            )
            print_join_result("Q4", result, strategy_name)
    
    if query_choice in ['5', 'all']:
        # Q5: Product stock join query
        print(f"\n{'#'*70}")
        print(f"# Testing Q5 on DB{db_num}")
        print(f"{'#'*70}")
        
        sharding_strategies = [
            ("Product(brand),Stock(IDP)", {"Product": "brand", "Stock": "IDP"}),
            ("Product(IDP),Stock(IDP)", {"Product": "IDP", "Stock": "IDP"}),
        ]
        
        for strategy_name, sharding_dict in sharding_strategies:
            result = executor.execute_q5(
                brand="Apple",
                sharding_strategy=sharding_dict,
                array_sizes=array_sizes.get(db_num)
            )
            print_join_result("Q5", result, strategy_name)
    
    if query_choice in ['6', 'all']:
        # Q6: Aggregate + Join query (product with highest total quantity)
        print(f"\n{'#'*70}")
        print(f"# Testing Q6 on DB{db_num}")
        print(f"{'#'*70}")
        
        sharding_strategies = [
            ("OrderLine(IDC), Product(IDP)", {"OrderLine": "IDC", "Product": "IDP"}),
            ("OrderLine(IDP), Product(brand)", {"OrderLine": "IDP", "Product": "branc"}),
        ]
        
        for strategy_name, sharding_dict in sharding_strategies:
            agg_result = executor.execute_q6(
                sharding_strategy=sharding_dict,
                array_sizes=array_sizes.get(db_num)
            )
            print_aggregate_result("Q6", agg_result, strategy_name)
    
    if query_choice in ['7', 'all']:
        # Q7: Aggregate + Join query with client filter
        print(f"\n{'#'*70}")
        print(f"# Testing Q7 on DB{db_num}")
        print(f"{'#'*70}")
        
        sharding_strategies = [
            ("OrderLine(IDC), Product(IDP)", {"OrderLine": "IDC", "Product": "IDP"}),
            ("OrderLine(IDP), Product(IDP)", {"OrderLine": "IDP", "Product": "IDP"}),
        ]
        
        for strategy_name, sharding_dict in sharding_strategies:
            agg_result = executor.execute_q7(
                sharding_strategy=sharding_dict,
                array_sizes=array_sizes.get(db_num)
            )
            print_aggregate_result("Q7", agg_result, strategy_name)


def main():
    """Main execution - analyze databases or run query tests"""
    
    print("="*70)
    print("Big Data Structure Analysis Tool")
    print("="*70)
    
    print("\nSelect mode:")
    print("1. Database Analysis (sizes, sharding)")
    print("2. Query Testing (filters, joins)")
    
    mode = input("\nEnter mode (1 or 2): ").strip()
    
    if mode == '1':
        # Database analysis mode
        stats = Statistics()
        size_calc = SizeCalculator(stats)
        shard_calc = ShardCalculator(stats)
        
        db_choice = input("\nEnter database number (1-5) or 0 for all: ").strip()
        
        try:
            db_num = int(db_choice)
            if db_num == 0:
                for i in range(1, 6):
                    db = SchemaParser.build_db_from_json(i, stats, f"schemas/db{i}.json")
                    print_db_analysis(db, i, stats, size_calc, shard_calc)
            elif 1 <= db_num <= 5:
                db = SchemaParser.build_db_from_json(db_num, stats, f"schemas/db{db_num}.json")
                print_db_analysis(db, db_num, stats, size_calc, shard_calc)
            else:
                print("Invalid database number.")
        except ValueError:
            print("Invalid input.")
    
    elif mode == '2':
        # Query testing mode
        print("\nAvailable databases: 1-5")
        print("Available queries: 1 (Q1), 2 (Q2), 3 (Q3), 4 (Q4), 5 (Q5), 6 (Q6), 7 (Q7)")
        
        db_choice = input("\nEnter database number (1-5) or 'all': ").strip()
        query_choice = input("Enter query number (1-7) or 'all': ").strip()
        
        if db_choice.lower() == 'all':
            databases = [1, 2, 3, 4, 5]
        else:
            try:
                db_num = int(db_choice)
                if 1 <= db_num <= 5:
                    databases = [db_num]
                else:
                    print("Invalid database number. Using DB1.")
                    databases = [1]
            except ValueError:
                print("Invalid input. Using DB1.")
                databases = [1]
        
        for db_num in databases:
            try:
                run_query_tests(db_num, query_choice)
            except Exception as e:
                print(f"\nError testing DB{db_num}: {e}")
                import traceback
                traceback.print_exc()
        
        print("\n" + "="*70)
        print("Testing Complete!")
        print("="*70)
    
    else:
        print("Invalid mode selection.")


if __name__ == "__main__":
    main()