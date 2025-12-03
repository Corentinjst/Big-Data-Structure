"""
Example script showing TD2 correction format output
Demonstrates S1, O1, S2, O2, Loops, C1, C2 as shown in TP2_correction.pdf
"""

from models.statistics import Statistics
from parsers.schema_parser import SchemaParser
from operators import QueryExecutor


def example_filter_query():
    """
    Example: Q2 - Products from "Apple" brand on DB1
    Matches format from TP2_correction.pdf page 4
    """
    print("\n" + "="*80)
    print("FILTER QUERY EXAMPLE: Q2 - Products from 'Apple' brand")
    print("="*80)

    stats = Statistics()
    db = SchemaParser.build_db_from_json(1, stats, "schemas/db1.json")
    executor = QueryExecutor(db, stats)

    # Test with different sharding strategies
    strategies = [
        ("Product sharded by #brand", {"Product": "brand"}),
        ("Product sharded by #IDP", {"Product": "IDP"}),
    ]

    array_sizes = {"categories": 2}

    for strategy_name, sharding_dict in strategies:
        result = executor.execute_q2(
            brand="Apple",
            sharding_strategy=sharding_dict,
            array_sizes=array_sizes
        )

        print(f"\n--- Strategy: {strategy_name} ---")
        print(f"S1 (input docs):      {result.s1:>15,}")
        print(f"O1 (output docs):     {result.o1:>15,}")
        print(f"Loops:                {result.loops:>15,}")
        print(f"Servers accessed:     {result.num_servers_accessed:>15,}")
        print(f"C1 volume:            {result.c1_volume_bytes:>15,} bytes")
        print(f"\nTime: {result.cost.time_ms:.2f} ms")
        print(f"Carbon: {result.cost.carbon_gco2:.2f} gCO2")
        print(f"Price: ${result.cost.price_usd:.6f}")


def example_join_query():
    """
    Example: Q4 - Stock from warehouse with product names on DB1
    Matches format from TP2_correction.pdf page 4
    """
    print("\n\n" + "="*80)
    print("JOIN QUERY EXAMPLE: Q4 - Stock from warehouse with product names")
    print("="*80)

    stats = Statistics()
    db = SchemaParser.build_db_from_json(1, stats, "schemas/db1.json")
    executor = QueryExecutor(db, stats)

    # Test with different sharding strategies (matching TD2 correction R4.1 and R4.2)
    strategies = [
        ("Stock(#IDW), Product(#IDP)", {"Stock": "IDW", "Product": "IDP"}),
        ("Stock(#IDP), Product(#IDP)", {"Stock": "IDP", "Product": "IDP"}),
    ]

    array_sizes = {"categories": 2}

    for strategy_name, sharding_dict in strategies:
        result = executor.execute_q4(
            idw_value=42,
            sharding_strategy=sharding_dict,
            array_sizes=array_sizes
        )

        print(f"\n--- Strategy: {strategy_name} ---")
        print(f"\nC1 Phase (Left collection):")
        print(f"  S1 (input docs):    {result.s1:>15,}")
        print(f"  O1 (output docs):   {result.o1:>15,}")

        print(f"\nC2 Phase (Right collection per loop):")
        print(f"  Loops:              {result.num_loops:>15,}")
        print(f"  S2 (docs/loop):     {result.s2:>15,}")
        print(f"  O2 (docs/loop):     {result.o2:>15,}")

        print(f"\nVolumes:")
        print(f"  C1 volume:          {result.c1_volume_bytes:>15,} bytes ({result.c1_volume_bytes/1024/1024:.2f} MB)")
        print(f"  C2 volume:          {result.c2_volume_bytes:>15,} bytes ({result.c2_volume_bytes/1024/1024:.2f} MB)")
        total_vt = result.c1_volume_bytes + result.num_loops * result.c2_volume_bytes
        print(f"  Total Vt:           {total_vt:>15,} bytes ({total_vt/1024/1024:.2f} MB)")
        print(f"  # Messages:         {result.num_messages:>15,}")

        print(f"\nCosts:")
        print(f"  Time: {result.cost.time_ms:.2f} ms")
        print(f"  Carbon: {result.cost.carbon_gco2:.2f} gCO2")
        print(f"  Price: ${result.cost.price_usd:.6f}")


def comparison_table():
    """
    Generate a comparison table similar to TD2 correction format
    """
    print("\n\n" + "="*80)
    print("COMPARISON TABLE (TD2 Correction Format)")
    print("="*80)

    stats = Statistics()
    db = SchemaParser.build_db_from_json(1, stats, "schemas/db1.json")
    executor = QueryExecutor(db, stats)

    strategies = [
        ("R4.1", "Stock(#IDW), Product(#IDP)", {"Stock": "IDW", "Product": "IDP"}),
        ("R4.2", "Stock(#IDP), Product(#IDP)", {"Stock": "IDP", "Product": "IDP"}),
    ]

    print(f"\n{'ID':<8} {'Sharding':<30} {'S1':<12} {'O1':<12} {'Loops':<12} {'S2':<12} {'O2':<12} {'#msgs':<12}")
    print("-" * 110)

    for row_id, strategy_name, sharding_dict in strategies:
        result = executor.execute_q4(
            idw_value=42,
            sharding_strategy=sharding_dict,
            array_sizes={"categories": 2}
        )

        print(f"{row_id:<8} {strategy_name:<30} {result.s1:<12,} {result.o1:<12,} "
              f"{result.num_loops:<12,} {result.s2:<12,} {result.o2:<12} {result.num_messages:<12,}")


def main():
    """Run all examples"""
    print("\n" + "="*80)
    print("TD2 CORRECTION FORMAT OUTPUT EXAMPLES")
    print("Demonstrates S1, O1, S2, O2, Loops, C1, C2, #msgs")
    print("="*80)

    example_filter_query()
    example_join_query()
    comparison_table()

    print("\n" + "="*80)
    print("Examples Complete!")
    print("="*80)
    print("\nNote: These values match the format shown in TP2_correction.pdf")
    print("where:")
    print("  S1 = Number of input documents from first collection")
    print("  O1 = Number of output documents from first phase")
    print("  S2 = Number of documents from second collection (per loop)")
    print("  O2 = Number of output documents per loop")
    print("  Loops = Number of loop iterations")
    print("  C1 = #S1 * size(S1) + #O1 * size(O1)")
    print("  C2 = #S2 * size(S2) + #O2 * size(O2)")
    print("  Vt = C1 + Loops * C2")
    print("  #msgs = Number of messages exchanged between servers")


if __name__ == "__main__":
    main()
