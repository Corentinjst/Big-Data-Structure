"""
Test script for Chapter 3 - Filter and Join Queries
Demonstrates the usage of query operators on different database designs
"""

import sys
import os
# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from models.statistics import Statistics
from parsers.schema_parser import SchemaParser
from operators import QueryExecutor, FilterOperator, NestedLoopJoinOperator


def print_filter_result(query_name: str, result, sharding_strategy: str):
    """Print filter query results"""
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


def print_join_result(query_name: str, result, sharding_strategy: str):
    """Print join query results"""
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


def test_q1_stock_query(db_num: int):
    """Test Q1: Stock of a given product in a given warehouse"""
    print(f"\n\n{'#'*70}")
    print(f"# Testing Q1 on DB{db_num}")
    print(f"{'#'*70}")

    stats = Statistics()
    db = SchemaParser.build_db_from_json(db_num, stats, f"../schemas/db{db_num}.json")
    executor = QueryExecutor(db, stats)

    # Define sharding strategies for Stock collection
    sharding_strategies = [
        ("Stock sharded by IDP", {"Stock": "IDP"}),
        ("Stock sharded by IDW", {"Stock": "IDW"}),
    ]

    array_sizes = {2: {"categories": 2, "stocks": 200}}

    for strategy_name, sharding_dict in sharding_strategies:
        result = executor.execute_q1(
            sharding_strategy=sharding_dict,
            array_sizes=array_sizes.get(db_num, {"categories": 2})
        )
        print_filter_result("Q1", result, strategy_name)


def test_q2_product_brand_query(db_num: int):
    """Test Q2: Products from a given brand"""
    print(f"\n\n{'#'*70}")
    print(f"# Testing Q2 on DB{db_num}")
    print(f"{'#'*70}")

    stats = Statistics()
    db = SchemaParser.build_db_from_json(db_num, stats, f"../schemas/db{db_num}.json")
    executor = QueryExecutor(db, stats)

    # Define sharding strategies for Product collection
    sharding_strategies = [
        ("Product sharded by brand", {"Product": "brand"}),
        ("Product sharded by IDP", {"Product": "IDP"}),
    ]

    array_sizes = {
        2: {"categories": 2, "stocks": 200},
        5: {"categories": 2, "orderLines": 5}
    }
    

    for strategy_name, sharding_dict in sharding_strategies:
        result = executor.execute_q2(
            brand="Apple",
            sharding_strategy=sharding_dict,
            array_sizes=array_sizes.get(db_num, {"categories": 2})
        )
        print_filter_result("Q2", result, strategy_name)


def test_q3_orderline_date_query(db_num: int):
    """Test Q3: Order lines from a given date"""
    print(f"\n\n{'#'*70}")
    print(f"# Testing Q3 on DB{db_num}")
    print(f"{'#'*70}")

    stats = Statistics()
    db = SchemaParser.build_db_from_json(db_num, stats, f"../schemas/db{db_num}.json")
    executor = QueryExecutor(db, stats)

    # Define sharding strategies for OrderLine collection
    sharding_strategies = [
        ("OrderLine sharded by IDC", {"OrderLine": "IDC"}),
        ("OrderLine sharded by IDP", {"OrderLine": "IDP"}),
    ]

    array_sizes = {
        2: {"categories": 2, "stocks": 200},
        5: {"categories": 2, "orderLines": 5}
    }


    for strategy_name, sharding_dict in sharding_strategies:
        result = executor.execute_q3(
            sharding_strategy=sharding_dict,
            array_sizes=array_sizes.get(db_num, {"categories": 2})
        )
        print_filter_result("Q3", result, strategy_name)


def test_q4_stock_join_query(db_num: int):
    """Test Q4: Stock from a given warehouse with product names"""
    print(f"\n\n{'#'*70}")
    print(f"# Testing Q4 on DB{db_num}")
    print(f"{'#'*70}")

    stats = Statistics()
    db = SchemaParser.build_db_from_json(db_num, stats, f"../schemas/db{db_num}.json")
    executor = QueryExecutor(db, stats)

    # Define sharding strategies
    sharding_strategies = [
        ("Stock(IDW), Product(IDP)", {"Stock": "IDW", "Product": "IDP"}),
        ("Stock(IDP), Product(IDP)", {"Stock": "IDP", "Product": "IDP"}),
    ]

    array_sizes = {
        2: {"categories": 2, "stocks": 200},
        3: {"categories": 2},
        5: {"categories": 2, "orderLines": 5}
    }

    for strategy_name, sharding_dict in sharding_strategies:
        result = executor.execute_q4(
            sharding_strategy=sharding_dict,
            array_sizes=array_sizes.get(db_num, {"categories": 2})
        )
        print_join_result("Q4", result, strategy_name)


def test_q5_product_stock_join_query(db_num: int):
    """Test Q5: Distribution of Apple products in warehouses"""
    print(f"\n\n{'#'*70}")
    print(f"# Testing Q5 on DB{db_num}")
    print(f"{'#'*70}")

    stats = Statistics()
    db = SchemaParser.build_db_from_json(db_num, stats, f"../schemas/db{db_num}.json")
    executor = QueryExecutor(db, stats)

    # INVERSER PAR RAPPORT A l'ENONCE MAIS DANS CE SENS POUR LA CORRECTION
    # Define sharding strategies
    sharding_strategies = [
        ("Product(brand),Stock(IDP)", {"Product": "brand","Stock": "IDP"}),
        ("Product(IDP),Stock(IDP)", {"Product": "IDP","Stock": "IDP"}),
    ]

    array_sizes = {
        2: {"categories": 2, "stocks": 200},
        3: {"categories": 2},
        5: {"categories": 2, "orderLines": 5}
    }

    for strategy_name, sharding_dict in sharding_strategies:
        result = executor.execute_q5(
            brand="Apple",
            sharding_strategy=sharding_dict,
            array_sizes=array_sizes.get(db_num, {"categories": 2})
        )
        print_join_result("Q5", result, strategy_name)


def main():
    """Main test function"""
    print("="*70)
    print("Chapter 3: Filter and Join Queries - Test Suite")
    print("="*70)

    # Ask user which database and query to test
    print("\nAvailable databases: DB1, DB2, DB3, DB4, DB5")
    print("Available queries: Q1, Q2, Q3, Q4, Q5")

    db_choice = input("\nEnter database number (1-5) or 'all' to test all: ").strip()
    query_choice = input("Enter query number (1-5) or 'all' to test all: ").strip()
    # Determine which databases to test
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

    # Determine which queries to test
    query_functions = {
        'Q1': test_q1_stock_query,
        'Q2': test_q2_product_brand_query,
        'Q3': test_q3_orderline_date_query,
        'Q4': test_q4_stock_join_query,
        'Q5': test_q5_product_stock_join_query
    }

    if query_choice.lower() == 'all':
        queries_to_test = list(query_functions.keys())
    else:
        try:
            query_choice_num = int(query_choice)
            if 1 <= query_choice_num <= 5:
                queries_to_test = [f'Q{query_choice_num}']
            else:
                print(f"Invalid query choice. Testing Q1.")
                queries_to_test = ['Q1']
        except ValueError:
            print(f"Invalid Input. Testing Q1.")
            queries_to_test = ['Q1']

    # Run tests
    for db_num in databases:
        for query_name in queries_to_test:
            try:
                query_functions[query_name](db_num)
            except Exception as e:
                print(f"\nError testing {query_name} on DB{db_num}: {e}")
                import traceback
                traceback.print_exc()

    print("\n\n" + "="*70)
    print("Testing Complete!")
    print("="*70)


if __name__ == "__main__":
    main()
