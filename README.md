# NoSQL Database Analysis Tool

## Overview

This project is a **NoSQL Database Analysis Tool** designed for analyzing and optimizing NoSQL database designs, particularly for MongoDB-style document databases. It was created as part of a Big Data Structure course.

The tool models an **e-commerce database system** with multiple denormalization strategies and provides:
- **Document size calculations** based on field types and nested structures
- **Collection size estimations** for different database designs
- **Database sizing analysis** comparing 5 different denormalization approaches
- **Sharding strategy comparisons** to optimize horizontal scaling
- **Query cost analysis** for filter and join operations
- **Performance optimization recommendations** based on server utilization metrics
- **JSON Schema parsing** for flexible schema definition

## Project Structure

```
Big-Data-Structure/
├── models/                      # Data models
│   ├── __init__.py
│   ├── schema.py                # Schema, Field, Collection, and Database classes
│   └── statistics.py            # Database statistics and constants
├── parsers/                     # JSON Schema parsers
│   ├── __init__.py
│   └── schema_parser.py         # Parse JSON schemas into internal models
├── calculators/                 # Size and sharding calculators
│   ├── __init__.py
│   ├── size_calculator.py       # Document/collection/database size calculations
│   └── shard_calculator.py      # Sharding distribution analysis
├── operators/                   # Query operators (NEW - Chapter 3)
│   ├── __init__.py
│   ├── cost_model.py            # Cost calculation model (time, carbon, price)
│   ├── filter_operator.py       # Filter query execution
│   ├── join_operator.py         # Nested loop join execution
│   ├── aggregate_operator.py    # Aggregate query execution (GROUP BY)
│   └── query_executor.py        # High-level query executor
├── config/                      # Configuration
│   ├── __init__.py
│   └── constants.py             # Data type sizes and cost constants
├── schemas/                     # JSON Schema definitions
│   ├── db1.json                 # DB1: Normalized design
│   ├── db2.json                 # DB2: Product embeds Stock array
│   ├── db3.json                 # DB3: Stock embeds Product
│   ├── db4.json                 # DB4: OrderLine embeds Product
│   └── db5.json                 # DB5: Product embeds OrderLine array
├── tests/                       # Tests and notebooks
│   ├── TD1.ipynb                # Jupyter notebook for TD1 exercises
│   └── test_TD2.py              # Query testing suite
├── main.py                      # Main program with interactive menu
└── README.md                    # This file
```

## Core Components

### 1. Models (`models/`)

#### Schema & Field (`schema.py`)

**Field** - Represents a single field in a document schema:
- `name`: Field name
- `field_type`: Type identifier (`integer`, `number`, `string`, `date`, `longstring`, `object`, `array`)
- `is_required`: Whether the field is required
- `nested_schema`: Schema for nested objects (when `field_type='object'`)
- `array_item_schema`: Schema for array items (when `field_type='array'`)

**Schema** - Represents a complete document schema:
- `name`: Schema name
- `fields`: List of Field objects
- `add_field()`: Add a field to the schema
- `get_field()`: Retrieve a field by name

**Collection** - Represents a MongoDB-style collection:
- `name`: Collection name
- `schema`: Schema definition
- `document_count`: Total number of documents
- `sharding_key`: Field used for sharding (optional)
- `distinct_shard_values`: Number of distinct values for the sharding key (optional)

**Database** - Represents a complete database:
- `name`: Database name
- `collections`: Dictionary of Collection objects
- `add_collection()`: Add a collection
- `get_collection()`: Retrieve a collection by name

#### Statistics (`statistics.py`)

Contains database statistics and business assumptions for the e-commerce system:

**Core Statistics:**
- `num_clients`: 10,000,000 (10^7) - Total customers
- `num_products`: 100,000 (10^5) - Total products in catalog
- `num_order_lines`: 4,000,000,000 (4×10^9) - Total order line items
- `num_warehouses`: 200 - Physical warehouse locations
- `num_servers`: 1,000 - Available servers for sharding

**Derived Statistics:**
- `orders_per_customer`: 100 - Average orders per client
- `products_per_customer`: 20 - Average unique products per client
- `categories_per_product_avg`: 2 - Average categories per product
- `num_brands`: 5,000 - Total brands in catalog
- `products_per_brand_apple`: 50 - Products for Apple brand
- `num_dates`: 365 - Number of unique dates
- `num_stock_entries`: 20,000,000 (`num_products × num_warehouses`)
- `avg_order_lines_per_product`: 40,000 (`num_order_lines ÷ num_products`)

### 2. Parsers (`parsers/`)

#### SchemaParser (`schema_parser.py`)

Parses JSON Schema definitions into internal Schema objects.

**Main Methods:**
- `parse_from_dict(schema_dict, name)`: Parse JSON Schema into Schema object
- `parse_multiple_from_file(filepath)`: Load multiple schemas from JSON
- `build_db_from_json(db_index, stats, filepath)`: Build complete Database instance

**Features:**
- Handles nested objects and arrays
- Supports JSON Schema `format` property (`date`, `longstring`)
- Respects `required` arrays
- Maps collections to document counts

### 3. Calculators (`calculators/`)

#### SizeCalculator (`size_calculator.py`)

Calculates storage requirements for documents, collections, and databases.

**Main Methods:**
- `calculate_document_size(schema, array_sizes)`: Document size in bytes
- `calculate_collection_size(collection, array_sizes)`: Collection size
- `calculate_database_size(database)`: Total database size

**Utility Methods:**
- `bytes_to_gb(bytes_size)`: Convert to gigabytes
- `bytes_to_human_readable(bytes_size)`: Format as B/KB/MB/GB/TB

#### ShardCalculator (`shard_calculator.py`)

Analyzes sharding strategies for horizontal scaling.

**Main Methods:**
- `calculate_distribution(collection, sharding_key, distinct_values)`: Single strategy analysis
- `compare_sharding_strategies(collection, strategies)`: Compare multiple strategies

**Metrics:**
- Average documents per server
- Average distinct values per server
- Server utilization percentage
- Skew warnings

### 4. Operators (`operators/`) **[NEW - Chapter 3]**

Query execution operators for analyzing filter and join performance.

#### CostModel (`cost_model.py`)

Calculates query execution costs with multiple metrics.

**QueryCost** - Dataclass representing costs:
- `time_ms`: Execution time in milliseconds
- `carbon_gco2`: Carbon footprint in grams of CO2
- `price_usd`: Financial cost in USD
- `data_volume_bytes`: Total data transferred
- `num_documents`: Documents processed
- `num_servers`: Servers involved

**Cost Calculation Methods:**

`calculate_communication_cost(data_volume_bytes, num_servers, num_documents)`
- Calculates network transfer costs
- Formula: `time = data_volume / BANDWIDTH_SPEED_BYTES_PER_MS`
- Includes server processing costs

`calculate_scan_cost(num_documents, doc_size_bytes, use_index, num_servers)`
- Calculates document scanning costs
- With index: `time = num_documents × INDEX_ACCESS_TIME_MS` (0.1 ms/doc)
- Without index: `time = num_documents × FULL_SCAN_TIME_PER_DOC_MS` (0.001 ms/doc)

`calculate_filter_cost(total_document_accessed, doc_size_bytes, c1, use_index, num_servers)`
- Calculates filter operation costs
- Combines scan cost + communication cost
- Accounts for sharding distribution

`calculate_nested_loop_join_cost(total_document_accessed, doc_size_bytes, c1, c2, num_loops, ...)`
- Calculates nested loop join costs
- Returns `(QueryCost, num_messages)` tuple
- C1: Left collection transfer
- C2: Right collection transfer (× num_loops)
- Distinguishes co-located vs broadcast joins

**Cost Constants** (in `config/constants.py`):
- `BANDWIDTH_SPEED`: 15,000,000 bytes/s (15 MB/s)
- `COST_PER_GB_TRANSFER`: 0.01 USD/GB
- `CARBON_PER_GB_TRANSFER`: 0.5 gCO2/GB
- `COST_PER_SERVER_MS`: 0.0001 USD/ms/server
- `CARBON_PER_SERVER_MS`: 0.001 gCO2/ms/server
- `INDEX_ACCESS_TIME_MS`: 0.1 ms/document
- `FULL_SCAN_TIME_PER_DOC_MS`: 0.001 ms/document

#### FilterOperator (`filter_operator.py`)

Executes filter queries (WHERE clauses) with optional sharding.

**FilterResult** - Dataclass with results:
- `output_size_bytes`: Output document size
- `cost`: QueryCost object
- `s1`: Number of documents scanned
- `o1`: Number of output documents
- `c1_volume_bytes`: C1 volume (#S1 × size(S1) + #O1 × size(O1))
- `num_servers_accessed`: Servers queried
- `input_doc_size_bytes`: Input document size
- `sharding_key`: Sharding key used
- `index_used`: Whether index was used

**Main Method:**

`filter(collection, filter_keys, output_keys, sharding_key, selectivity, use_index, array_sizes)`
- Executes filter query with cost analysis
- **Parameters:**
  - `collection`: Target collection
  - `filter_keys`: List of keys used in WHERE clause
  - `output_keys`: Keys to include in output (SELECT)
  - `sharding_key`: Collection's sharding key
  - `selectivity`: Fraction of documents matching filter
  - `use_index`: Whether an index exists on filter key
  - `array_sizes`: Average array sizes

**Logic:**
- **Sharding on filter key:** Query goes to 1 server
- **No sharding match:** Query broadcasts to all servers
- Calculates input/output document sizes
- Computes C1 volume and total cost

#### NestedLoopJoinOperator (`join_operator.py`)

Executes nested loop joins between two collections.

**JoinResult** - Dataclass with results:
- `cost`: QueryCost object
- `join_key`: Key used for join
- `num_loops`: Number of loop iterations
- `s1`, `o1`: Left collection metrics
- `s2`, `o2`: Right collection metrics
- `c1_volume_bytes`: C1 volume
- `c2_volume_bytes`: C2 volume
- `input_size_bytes1`, `output_size_bytes1`: Left sizes
- `input_size_bytes2`, `output_size_bytes2`: Right sizes
- `left_sharding_key`, `right_sharding_key`: Sharding keys
- `num_messages`: Messages exchanged

**Main Method:**

`nested_loop_join(left_collection, right_collection, join_key, left_output_keys, right_output_keys, ...)`
- Executes nested loop join with cost analysis
- **Algorithm:**
  ```
  For each document in left_collection (O1 loops):
      Find matching documents in right_collection
      Join and output
  ```

**Sharding Optimizations:**
- **Co-located join** (both sharded on join key): 1 message, parallel execution
- **Broadcast join** (one side not sharded): O1 messages, sequential broadcast
- Calculates servers accessed for each side (s1, s2)

**Parameters:**
- `left_collection`, `right_collection`: Collections to join
- `join_key`: Field to join on
- `left_output_keys`, `right_output_keys`: Output fields
- `left_sharding_key`, `right_sharding_key`: Sharding keys
- `left_filter_keys`, `right_filter_keys`: Pre-filter keys
- `left_filter_selectivity`, `right_filter_selectivity`: Filter selectivity
- `array_sizes`: Average array sizes

#### AggregateOperator (`aggregate_operator.py`)

Executes aggregate queries with GROUP BY and optional joins.

**AggregateResult** - Dataclass with results:
- `output_size_bytes1`, `output_size_bytes2`: Output document sizes
- `input_size_bytes1`, `input_size_bytes2`: Input document sizes
- `shuffle_size_bytes1`, `shuffle_size_bytes2`: Shuffle data sizes
- `cost`: QueryCost object
- `s1`, `s2`: Number of servers accessed
- `o1`, `o2`: Number of output documents
- `shuffle1`, `shuffle2`: Number of documents shuffled
- `c1_volume_bytes`, `c2_volume_bytes`: Data volumes
- `num_loops`: Number of loop iterations
- `join_key`: Key used for join
- `left_group_by_key`, `right_group_by_key`: GROUP BY keys
- `left_sharding_key`, `right_sharding_key`: Sharding keys

**Main Method:**

`aggregator(left_collection, right_collection, join_key, limit, left_output_keys, right_output_keys, ...)`
- Executes aggregate query with GROUP BY and join
- **Parameters:**
  - `left_collection`, `right_collection`: Collections to aggregate
  - `join_key`: Field to join on
  - `limit`: Number of results to return
  - `left_output_keys`, `right_output_keys`: Output fields
  - `left_group_by_key`, `right_group_by_key`: Fields to group by
  - `left_sharding_key`, `right_sharding_key`: Sharding keys
  - `left_filter_keys`, `right_filter_keys`: Pre-filter keys
  - `left_filter_selectivity`, `right_filter_selectivity`: Filter selectivity
  - `array_sizes`: Average array sizes

**Logic:**
- Applies filters on both collections
- Computes GROUP BY aggregation
- Handles shuffle phase when GROUP BY key differs from sharding key
- Performs join between aggregated results
- **Shuffle optimization:** No shuffle if GROUP BY key matches sharding key
- **Sharding on filter key:** Query goes to subset of servers
- Calculates C1 and C2 volumes including shuffle cost

**Shuffle Phase:**
- If `group_by_key == sharding_key`: No shuffle needed (0 documents)
- Otherwise: `shuffle = output_docs × (num_servers - 1)`
- Shuffle cost included in total data volume

#### QueryExecutor (`query_executor.py`)

High-level executor for predefined queries (Q1-Q5).

**Implemented Queries:**

**Q1** - `execute_q1(sharding_strategy, array_sizes)`
```sql
SELECT S.IDW, S.quantity
FROM Stock S
WHERE S.IDP = 12345 AND S.IDW = 42
```

**Q2** - `execute_q2(brand, sharding_strategy, array_sizes)`
```sql
SELECT P.name, P.price
FROM Product P
WHERE P.brand = "Apple"
```

**Q3** - `execute_q3(sharding_strategy, array_sizes)`
```sql
SELECT OL.IDP, OL.quantity, OL.price
FROM OrderLine OL
WHERE OL.date = "2024-01-15"
```

**Q4** - `execute_q4(sharding_strategy, array_sizes)`
```sql
SELECT S.IDW, S.quantity, P.name
FROM Stock S JOIN Product P ON S.IDP = P.IDP
WHERE S.IDW = 42
```

**Q5** - `execute_q5(brand, sharding_strategy, array_sizes)`
```sql
SELECT P.name, P.price, S.IDW, S.quantity
FROM Product P JOIN Stock S ON P.IDP = S.IDP
WHERE P.brand = "Apple"
```

**Q6** - `execute_q6(sharding_strategy, array_sizes)`
```sql
SELECT P.name, P.price, OL.NB
FROM Product P JOIN (
    SELECT O.IDP, SUM(O.quantity) AS NB
    FROM OrderLine O
    GROUP BY O.IDP
) OL ON P.IDP = OL.IDP
ORDER BY OL.NB DESC
LIMIT 100
```

**Q7** - `execute_q7(sharding_strategy, array_sizes)`
```sql
SELECT P.name, P.price, OL.NB
FROM Product P JOIN (
    SELECT O.IDP, SUM(O.quantity) AS NB
    FROM OrderLine O
    WHERE O.IDC = 125
    GROUP BY O.IDP
) OL ON P.IDP = OL.IDP
ORDER BY OL.NB DESC
LIMIT 1
```

**Features:**
- Uses FilterOperator, NestedLoopJoinOperator, and AggregateOperator
- Supports multiple sharding strategies
- Calculates selectivity based on statistics
- Returns detailed cost breakdown
- Q6 and Q7 demonstrate aggregate queries with GROUP BY and joins

### 5. Configuration (`config/`)

#### Constants (`constants.py`)

**Data Type Sizes:**
- `INTEGER_SIZE = 8` bytes
- `NUMBER_SIZE = 8` bytes
- `STRING_SIZE = 80` bytes
- `DATE_SIZE = 20` bytes
- `LONG_STRING_SIZE = 200` bytes
- `KEY_VALUE_OVERHEAD = 12` bytes/field
- `ARRAY_OVERHEAD = 12` bytes

**Network & Query Constants:**
- `BANDWIDTH_SPEED = 15,000,000` bytes/s
- `BANDWIDTH_SPEED_BYTES_PER_MS = 15,000` bytes/ms

**Cost Constants:**
- `COST_PER_GB_TRANSFER = 0.01` USD/GB
- `CARBON_PER_GB_TRANSFER = 0.5` gCO2/GB
- `COST_PER_SERVER_MS = 0.0001` USD/ms/server
- `CARBON_PER_SERVER_MS = 0.001` gCO2/ms/server

**Query Execution Constants:**
- `INDEX_ACCESS_TIME_MS = 0.1` ms/document
- `FULL_SCAN_TIME_PER_DOC_MS = 0.001` ms/document
- `COMPARISON_TIME_MS = 0.0001` ms/comparison

## Running the Program

### Interactive Main Program

Execute the main program for interactive analysis:

```bash
python main.py
```

**Mode 1: Database Analysis (TD1)**
- Analyze document/collection/database sizes
- Compare sharding strategies
- View server utilization metrics

**Mode 2: Query Testing (TD2/TD3)**
- Test filter queries (Q1, Q2, Q3)
- Test join queries (Q4, Q5)
- Test aggregate queries with GROUP BY (Q6, Q7)
- Compare different sharding strategies
- View detailed cost breakdowns (time, carbon, price)

**Interactive Options:**
- Select database (1-5) or 'all'
- Select query (1-5) or 'all'
- View formatted results with TD2 correction format

### Example Output - Filter Query (Q2)

```
======================================================================
Q2 - Sharding Strategy: Product sharded by brand
======================================================================

--- TD2 Correction Format ---
Column          Value               
-----------------------------------
Sharding        Product sharded by brand
S1 (docs)       10,000
O1 (docs)       50
Input Size: 1,288 bytes (0.00 MB)
Output Size: 200 bytes (0.00 MB)
C1 (bytes)      12,890,000 (12.29 MB)

--- Costs ---
QueryCost(
  Time: 103.120 ms (0.103 s)
  Carbon: 103.23 gCO2
  Price: $0.010312 USD
  Data Volume: 12,890,000 bytes (12.29 MB)
  Documents accessed: 10,000
  Servers involved: 1
)
```

### Example Output - Join Query (Q5)

```
======================================================================
Q5 - Sharding Strategy: Product(IDP),Stock(IDP)
Join Key: IDP
======================================================================

--- TD2 Correction Format ---
Column               Value                    
---------------------------------------------
Sharding             Product(IDP),Stock(IDP)

--- C1 Phase ---
S1 (docs)            10,000
O1 (docs)            50
Input Size: 1,288 bytes (0.00 MB)
Output Size: 200 bytes (0.00 MB)

--- C2 Phase ---
Loops                50
S2 (docs)            200
O2 (docs)            1
Input Size: 44 bytes (0.00 MB)
Output Size: 32 bytes (0.00 MB)

--- Volumes ---
C1 (bytes)           12,890,000 (12.2900 MB)
C2 (bytes)           9,000 (0.0086 MB)
Total Vt             13,340,000 bytes
#Messages            50

--- Costs ---
QueryCost(
  Time: 106.720 ms (0.107 s)
  Carbon: 106.83 gCO2
  Price: $0.010672 USD
  Data Volume: 13,340,000 bytes (12.72 MB)
  Documents accessed: 10,400
  Servers involved: 1000
)
```

### Example Output - Aggregate Query (Q7)

```
======================================================================
Q7 - Sharding Strategy: OrderLine(IDC),Product(IDP)
Join Key: IDP
======================================================================

--- TD2 Correction Format ---
Column               Value                    
---------------------------------------------
Sharding             OrderLine(IDC),Product(IDP)

--- Right Collection (OrderLine - GROUP BY) ---
S1 (servers)         1
O1 (docs)            20
Shuffle1 (docs)      0
Input Size: 32 bytes
Output Size: 12 bytes
Shuffle Size: 12 bytes

--- Left Collection (Product) ---
S2 (servers)         1000
O2 (docs)            1
Shuffle2 (docs)      0
Input Size: 200 bytes
Output Size: 188 bytes
Shuffle Size: 12 bytes

--- Volumes ---
C1 (bytes)           12,880 (0.0123 MB)
C2 (bytes)           200,188 (0.1909 MB)
Total Vt             213,068 bytes
Loops                1

--- Costs ---
QueryCost(
  Time: 95.421 ms (0.095 s)
  Carbon: 95.52 gCO2
  Price: $0.009542 USD
  Data Volume: 213,068 bytes (0.20 MB)
  Documents accessed: 4,000,021
  Servers involved: 1001
)
```

### Jupyter Notebook (TD1)

For interactive exploration:

```bash
jupyter notebook tests/TD1.ipynb
```

### Testing Suite

Run automated tests:

```bash
python tests/test_TD2.py
```

## Database Designs (DB1-DB5)

The project compares **5 different denormalization strategies**:

**DB1 - Normalized (Baseline)**
- `Prod{[Cat], Supp}, St, Wa, OL, Cl`
- Smallest storage, more joins

**DB2 - Product Embeds Stocks**
- `Prod{[Cat], Supp, [St]}, Wa, OL, Cl`
- Reduced joins for inventory queries

**DB3 - Stock Embeds Product**
- `St{Prod{[Cat], Supp}}, Wa, OL, Cl`
- Product data duplicated 200× (per warehouse)

**DB4 - OrderLine Embeds Product**
- `St, Wa, OL{Prod{[Cat], Supp}}, Cl`
- Fast order queries, massive duplication (4B order lines)

**DB5 - Product Embeds OrderLines**
- `Prod{[Cat], Supp, [OL]}, St, Wa, Cl`
- Huge Product documents, complex updates

## Key Concepts

### Query Cost Analysis

**Cost Components:**
1. **Scan Cost**: Time to read documents from storage
   - Index scan: Fast (0.1 ms/doc)
   - Full scan: Slower (0.001 ms/doc)

2. **Communication Cost**: Network transfer time
   - Based on data volume and bandwidth
   - Multiplied by number of servers

3. **Processing Cost**: Server CPU time
   - Proportional to execution time
   - Multiplied by number of servers

**Total Cost Formula:**
```
Total Cost = Scan Cost + Communication Cost + Processing Cost
```

### Filter Query Optimization

**Sharding on Filter Key:**
- Query goes to **1 server** (or few servers)
- Fast execution, minimal data transfer
- Example: Filter by `IDP` when sharded by `IDP`

**No Sharding Match:**
- Query broadcast to **all servers** (e.g., 1000)
- Parallel execution but more network traffic
- Example: Filter by `date` when sharded by `IDP`

### Join Query Optimization

**Co-located Join:**
- Both collections sharded on **same join key**
- Data for matching keys on **same server**
- **1 message** to coordinator
- Parallel execution across all servers
- **Best performance**

**Broadcast Join:**
- Collections sharded on **different keys**
- Must broadcast right collection for each left document
- **O1 messages** (number of left documents)
- Sequential or semi-parallel execution
- **Higher cost**

### Sharding Principles

**Cardinality Impact:**
- **High cardinality** (e.g., Product ID: 100,000):
  - 100% server utilization
  - Even distribution
- **Low cardinality** (e.g., Warehouse ID: 200):
  - 20% server utilization
  - Potential hotspots

## Use Cases

1. **Database Design:** Compare denormalization strategies
2. **Capacity Planning:** Estimate storage requirements
3. **Query Optimization:** Choose optimal sharding keys
4. **Cost Estimation:** Calculate infrastructure costs (storage, compute, network)
5. **Performance Analysis:** Understand query execution patterns
6. **Academic Learning:** Understand NoSQL concepts and trade-offs

## Learning Outcomes

By working with this tool, you will understand:

1. **NoSQL Design Trade-offs:**
   - Normalization vs denormalization
   - Read performance vs write complexity
   - Storage efficiency vs query simplicity

2. **Query Performance:**
   - Impact of sharding on query execution
   - Co-located vs broadcast joins
   - Index usage benefits

3. **Cost Modeling:**
   - Time, carbon footprint, and financial costs
   - Network transfer vs compute costs
   - Scalability implications

4. **Capacity Planning:**
   - Storage requirements estimation
   - Server utilization optimization
   - Cost projections for growing datasets

## Technical Details

**Technology Stack:**
- Python 3.7+
- Standard library only (no external dependencies)
- Type hints throughout
- Modular, testable architecture

**Design Patterns:**
- Dataclasses for immutable models
- Strategy pattern for calculators
- Factory pattern for schema parsing

## License

Educational project for Big Data Structure course.

## Conclusion

This comprehensive tool provides analysis capabilities for both **storage design** (TD1) and **query execution** (TD2/TD3), enabling informed decisions about NoSQL database architecture, sharding strategies, and performance optimization.
