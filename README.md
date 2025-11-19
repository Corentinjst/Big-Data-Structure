# NoSQL Database Analysis Tool

## Overview

This project is a **NoSQL Database Analysis Tool** designed for analyzing and optimizing NoSQL database designs, particularly for MongoDB-style document databases. It was created as part of a Big Data Structure course homework assignment (Chapter 2 - TD1).

The tool models an **e-commerce database system** with multiple denormalization strategies and provides:
- **Document size calculations** based on field types and nested structures
- **Collection size estimations** for different database designs
- **Database sizing analysis** comparing 5 different denormalization approaches
- **Sharding strategy comparisons** to optimize horizontal scaling
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
├── config/                      # Configuration
│   ├── __init__.py
│   └── constants.py             # Data type size constants (bytes)
├── schemas/                     # JSON Schema definitions
│   ├── db1.json                 # DB1: Normalized design
│   ├── db2.json                 # DB2: Product embeds Stock array
│   ├── db3.json                 # DB3: Stock embeds Product
│   ├── db4.json                 # DB4: OrderLine embeds Product
│   └── db5.json                 # DB5: Product embeds OrderLine array
├── tests/                       # Tests and notebooks
│   └── TD1.ipynb                # Jupyter notebook for TD1 exercises
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
- `_doc_size`: Cached document size (calculated)
- `_collection_size`: Cached collection size (calculated)

**Database** - Represents a complete database:
- `name`: Database name
- `collections`: Dictionary of Collection objects
- `add_collection()`: Add a collection to the database
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
- `products_per_brand_apple`: 50 - Products for a specific brand (Apple)
- `num_dates`: 365 - Number of unique dates
- `num_stock_entries`: Calculated as `num_products × num_warehouses` (20,000,000)
- `avg_order_lines_per_product`: Calculated as `num_order_lines ÷ num_products` (40,000)

### 2. Parsers (`parsers/`)

#### SchemaParser (`schema_parser.py`)

Parses JSON Schema definitions into internal Schema objects with the following capabilities:

**Main Methods:**
- `parse_from_dict(schema_dict, name)`: Parse a JSON Schema dictionary into a Schema object
- `parse_multiple_from_file(filepath)`: Load multiple schemas from a single JSON file (returns dict mapping schema names to Schema objects)
- `build_db_from_json(db_index, stats)`: Build a complete Database instance from a JSON file (e.g., `db1.json`, `db2.json`)

**Features:**
- Handles nested objects (creates nested Schema instances)
- Handles arrays (stores item schema in `array_item_schema`)
- Identifies field types from JSON Schema `type` property
- Supports JSON Schema `format` property for semantic types:
  - `format: "date"` → Field type: `date`
  - `format: "longstring"` → Field type: `longstring`
- Respects `required` arrays in JSON Schema
- Maps collection names to appropriate document counts using Statistics

**JSON Schema Format:**
```json
{
  "Product_DB1": {
    "type": "object",
    "properties": {
      "IDP": { "type": "integer" },
      "name": { "type": "string" },
      "description": { "type": "string", "format": "longstring" },
      "categories": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "title": { "type": "string" }
          }
        }
      }
    },
    "required": ["IDP", "name"]
  }
}
```

### 3. Calculators (`calculators/`)

#### SizeCalculator (`size_calculator.py`)

Calculates storage requirements for documents, collections, and databases.

**Main Methods:**

`calculate_document_size(schema, array_sizes=None)`
- Calculates the size of a single document in bytes
- Iterates through all fields in the schema
- `array_sizes`: Optional dict mapping array field names to their average item counts

**Field Size Calculation Logic:**
- Base overhead: 12 bytes per key-value pair (`KEY_VALUE_OVERHEAD`)
- Type-specific sizes (from `constants.py`):
  - `integer`: 8 bytes
  - `number`: 8 bytes (float/double)
  - `string`: 80 bytes (standard string)
  - `date`: 20 bytes
  - `longstring`: 200 bytes (descriptions, addresses)
- Nested objects: Recursive calculation of nested schema
- Arrays: `ARRAY_OVERHEAD` (12 bytes) + (item_count × item_size)

`calculate_collection_size(collection, array_sizes=None)`
- Calculates total collection size: `document_size × document_count`
- Caches results in `collection._doc_size` and `collection._collection_size`

`calculate_database_size(database)`
- Sums all collection sizes in the database

**Utility Methods:**
- `bytes_to_gb(bytes_size)`: Convert bytes to decimal gigabytes (÷ 10^9)
- `bytes_to_human_readable(bytes_size)`: Format as B, KB, MB, GB, TB (decimal units: 1000-based)

**Note:** The tool uses **decimal units** (1 KB = 1000 bytes) rather than binary units (1 KiB = 1024 bytes) for consistency with academic exercises.

#### ShardCalculator (`shard_calculator.py`)

Analyzes sharding strategies for horizontal scaling across multiple servers.

**Main Methods:**

`calculate_distribution(collection, sharding_key, distinct_values)`
- Calculates sharding distribution metrics for a single strategy
- Returns dictionary with:
  - `sharding_key`: The key being analyzed
  - `total_documents`: Total documents in collection
  - `distinct_values`: Number of distinct values for the sharding key
  - `num_servers`: Available servers (from Statistics)
  - `avg_docs_per_server`: Average documents per server (`total_documents ÷ num_servers`)
  - `avg_distinct_per_server`: Average distinct values per server (`distinct_values ÷ num_servers`)
  - `servers_with_data`: Actual servers with data (min of `distinct_values` and `num_servers`)
  - `server_utilization`: Percentage of servers utilized (`servers_with_data ÷ num_servers`)
  - `skew_warning`: Boolean flag if utilization < 50%

`compare_sharding_strategies(collection, strategies)`
- Compares multiple sharding key candidates
- `strategies`: Dict mapping `sharding_key` → `distinct_values`
- Returns dict of results for each strategy

**Key Insights:**
- **High cardinality** (many distinct values) → Better server utilization
- **Low cardinality** (few distinct values) → Poor distribution, idle servers
- **Ideal sharding key:** `distinct_values ≥ num_servers` for 100% utilization

### 4. Configuration (`config/`)

#### Constants (`constants.py`)

Defines byte sizes for different data types based on MongoDB BSON storage format:

**Type Sizes:**
- `INTEGER_SIZE = 8` - 64-bit integers
- `NUMBER_SIZE = 8` - Floating-point numbers (double precision)
- `STRING_SIZE = 80` - Standard strings (average)
- `DATE_SIZE = 20` - Date/timestamp fields
- `LONG_STRING_SIZE = 200` - Long text fields (descriptions, comments, addresses)

**Overhead Constants:**
- `KEY_VALUE_OVERHEAD = 12` - Bytes per field (field name + metadata)
- `ARRAY_OVERHEAD = 12` - Array structure overhead

**TYPE_SIZES Dictionary:**
```python
TYPE_SIZES = {
    'integer': 8,
    'number': 8,
    'string': 80,
    'date': 20,
    'longstring': 200
}
```

## How It Works

### Workflow

1. **Define Database Schemas**
   - Create JSON Schema files in `schemas/` directory
   - Define collections with properties, types, and nested structures
   - Use `format` property for semantic types (`date`, `longstring`)
   - Specify required fields in `required` arrays

2. **Parse Schemas**
   - Use `SchemaParser.build_db_from_json(db_index, stats)` to load database
   - Parser reads JSON, creates Schema/Field objects
   - Automatically maps collections to document counts from Statistics

3. **Calculate Sizes**
   ```
   Document Size = Σ(field_size + KEY_VALUE_OVERHEAD)
   Collection Size = document_size × document_count
   Database Size = Σ(collection_sizes)
   ```
   - Use `SizeCalculator.calculate_document_size(schema, array_sizes)`
   - Handle nested objects recursively
   - Handle arrays with configurable item counts

4. **Analyze Sharding Strategies**
   - Define strategies: `{'sharding_key': distinct_values}`
   - Use `ShardCalculator.compare_sharding_strategies(collection, strategies)`
   - Compare average docs/server and server utilization
   - Identify optimal sharding keys

5. **Compare Database Designs**
   - Analyze multiple denormalization approaches (DB1-DB5)
   - Compare storage requirements
   - Evaluate sharding performance
   - Select optimal design based on use case

### Example: Product Collection

The tool models a Product collection with:
- Basic fields: IDP (ID), name, brand, description, image_url
- Nested object: price (amount, currency, vat_rate)
- Array: categories (each with title)
- Nested object: supplier (IDS, name, SIRET, headOffice, revenue)

**Size Calculation**:
```
Field sizes:
- IDP (integer): 8 + 12 = 20 bytes
- name (string): 80 + 12 = 92 bytes
- brand (string): 80 + 12 = 92 bytes
- description (long string): 200 + 12 = 212 bytes
- image_url (string): 80 + 12 = 92 bytes
- price (object): 3 fields × ~20 bytes = ~72 bytes
- categories (array): 2 items × ~92 bytes + 12 = ~196 bytes
- supplier (object): 5 fields × ~100 bytes = ~512 bytes

Total: ~1,288 bytes per product document
```

**Collection Size**:
```
100,000 products × 1,288 bytes ≈ 122.8 MB
```

### Example: Sharding Analysis

For the **Stock** collection (20 million entries):

**Strategy 1: Shard by IDP (Product ID)**
- Distinct values: 100,000 products
- Servers: 1,000
- Avg docs/server: 20,000
- Utilization: 100% (all servers used)
- ✅ Good distribution

**Strategy 2: Shard by IDW (Warehouse ID)**
- Distinct values: 200 warehouses
- Servers: 1,000
- Avg docs/server: 20,000
- Utilization: 20% (only 200 servers used)
- ⚠️ Poor distribution - many idle servers

**Recommendation**: Shard by IDP for better server utilization

## Use Cases

This tool is useful for:

1. **Database Design**: Estimate storage requirements before deployment
2. **Capacity Planning**: Project storage needs based on expected data volume
3. **Sharding Strategy**: Choose optimal sharding keys for horizontal scaling
4. **Performance Optimization**: Identify bottlenecks and distribution issues
5. **Cost Estimation**: Calculate infrastructure costs based on storage needs
6. **Academic Learning**: Understand NoSQL database concepts and calculations

## Running the Program

### Interactive Main Program

Execute the main program for interactive database analysis:

```bash
python main.py
```

**Interactive Menu:**
- Enter `0`: Analyze all 5 database designs (DB1-DB5)
- Enter `1-5`: Analyze a specific database design

**Output for Each Database:**
- **Denormalization Signature:** Shows which collections are embedded
- **Document Sizes:** Size of each document type in bytes/KB
- **Collection Sizes:** Total size of each collection (KB/MB/GB)
- **Database Size:** Total database size in human-readable format
- **Sharding Analysis:** 
  - Stock collection strategies (IDP, IDW)
  - OrderLine collection strategies (IDC, IDP)
  - Product collection strategies (IDP, brand)
  - Server utilization metrics for each strategy

### Jupyter Notebook (TD1)

For interactive exploration and exercises, use the Jupyter notebook:

```bash
jupyter notebook tests/TD1.ipynb
```

The notebook contains:
- Manual schema construction examples
- Size calculation exercises
- Sharding strategy comparisons
- Side-by-side comparison with JSON parser results

## Database Designs (DB1-DB5)

The project compares **5 different denormalization strategies** for an e-commerce system:

### Collections Overview
- **Product (Prod):** Product catalog with categories and supplier
- **Stock (St):** Inventory levels per product per warehouse
- **Warehouse (Wa):** Warehouse information
- **OrderLine (OL):** Individual line items in orders
- **Client (Cl):** Customer information

### Denormalization Strategies

**DB1 - Normalized (Baseline)**
- `Prod{[Cat], Supp}, St, Wa, OL, Cl`
- Product embeds categories array and supplier object
- All other collections separate
- **Trade-off:** More joins, smaller storage

**DB2 - Product Embeds Stocks**
- `Prod{[Cat], Supp, [St]}, Wa, OL, Cl`
- Product embeds entire stocks array (200 warehouses per product)
- **Trade-off:** Reduced joins for inventory queries, larger Product collection

**DB3 - Stock Embeds Product**
- `St{Prod{[Cat], Supp}}, Wa, OL, Cl`
- Each stock entry embeds complete product information
- **Trade-off:** Product data duplicated 200 times (once per warehouse)

**DB4 - OrderLine Embeds Product**
- `St, Wa, OL{Prod{[Cat], Supp}}, Cl`
- Each order line embeds product information
- **Trade-off:** Fast order queries, massive duplication (4 billion order lines)

**DB5 - Product Embeds OrderLines**
- `Prod{[Cat], Supp, [OL]}, St, Wa, Cl`
- Product embeds array of order lines (average 100 per product per customer)
- **Trade-off:** Huge Product documents, complex updates

## Key Concepts

### Document-Oriented Storage
- Data stored as JSON-like BSON documents
- Flexible schema (schema-on-read vs schema-on-write)
- Support for nested objects and arrays
- Trade-off between normalization and denormalization

### Size Calculation Principles
- **Field overhead:** Every field has name + metadata (12 bytes)
- **Type sizes:** Integers/numbers (8 bytes), strings (80 bytes avg), dates (20 bytes)
- **Nested structures:** Recursively calculated
- **Arrays:** Overhead (12 bytes) + items × item_size
- **Duplication cost:** Embedding increases document size but reduces joins

### Horizontal Sharding (Partitioning)
- **Goal:** Distribute data across multiple servers for scalability
- **Sharding key:** Field used to determine which server stores a document
- **Ideal distribution:** Even spread of documents across all servers
- **Metrics:**
  - Avg docs/server = `total_documents ÷ num_servers`
  - Server utilization = `min(distinct_values, num_servers) ÷ num_servers`

### Cardinality and Sharding
- **Cardinality:** Number of distinct values for a field
- **High cardinality** (e.g., Product ID: 100,000):
  - ✅ Excellent for sharding
  - ✅ 100% server utilization (all 1,000 servers used)
  - ✅ Even distribution
- **Low cardinality** (e.g., Warehouse ID: 200):
  - ⚠️ Poor for sharding
  - ⚠️ Only 20% server utilization (only 200 of 1,000 servers used)
  - ⚠️ Potential hotspots and imbalance

## Use Cases

This tool is designed for:

1. **Academic Learning:** Understand NoSQL database design principles and trade-offs
2. **Database Design:** Compare denormalization strategies before implementation
3. **Capacity Planning:** Estimate storage requirements for different designs
4. **Sharding Strategy Selection:** Choose optimal sharding keys for horizontal scaling
5. **Performance Optimization:** Identify bottlenecks and distribution issues
6. **Cost Estimation:** Calculate infrastructure costs based on storage and server needs
7. **What-If Analysis:** Evaluate impact of schema changes on storage and performance

## Extending the Tool

To add new functionality:

1. **New Database Designs:** 
   - Create new JSON file in `schemas/` (e.g., `db6.json`)
   - Define collections with different embedding strategies
   - Update `main.py` to include new design in analysis

2. **New Collections:**
   - Add collection definition in JSON schema
   - Update `collection_counts` mapping in `schema_parser.py`
   - Add corresponding statistics in `statistics.py`

3. **Custom Field Types:**
   - Add new type constants in `config/constants.py`
   - Update `_calculate_field_size()` in `size_calculator.py`
   - Use `format` property in JSON Schema to indicate type

4. **Additional Metrics:**
   - Extend `ShardCalculator` with new distribution algorithms
   - Add methods for query pattern analysis
   - Implement cost models for different operations

5. **Alternative Storage Models:**
   - Modify type size constants for different databases (Cassandra, Couchbase)
   - Adjust overhead constants based on storage format
   - Create database-specific calculator subclasses

## Example: Analyzing a Collection

### Product Collection (DB1)

**Schema:**
```json
{
  "IDP": "integer",
  "name": "string",
  "brand": "string", 
  "description": "longstring",
  "image_url": "string",
  "price": {
    "amount": "number",
    "currency": "string",
    "vat_rate": "number"
  },
  "categories": [
    {"title": "string"}
  ],
  "supplier": {
    "IDS": "integer",
    "name": "string",
    "SIRET": "string",
    "headOffice": "string",
    "revenue": "number"
  }
}
```

**Size Calculation:**
- IDP: 8 + 12 = 20 bytes
- name: 80 + 12 = 92 bytes
- brand: 80 + 12 = 92 bytes
- description: 200 + 12 = 212 bytes
- image_url: 80 + 12 = 92 bytes
- price (nested): 3 fields × ~26 bytes = 78 bytes
- categories (array, 2 items): 12 + (2 × 92) = 196 bytes
- supplier (nested): 5 fields × ~100 bytes = 512 bytes
- **Total: ~1,294 bytes per document**

**Collection Size:**
- 100,000 products × 1,294 bytes ≈ **123 MB**

### Sharding Analysis: Stock Collection

**Collection Stats:**
- Total documents: 20,000,000 (100,000 products × 200 warehouses)
- Available servers: 1,000

**Strategy 1: Shard by IDP (Product ID)**
- Distinct values: 100,000 products
- Avg docs/server: 20,000,000 ÷ 1,000 = **20,000 docs/server**
- Avg distinct/server: 100,000 ÷ 1,000 = **100 distinct values/server**
- Server utilization: **100%** (all 1,000 servers used)
- ✅ **Excellent distribution**

**Strategy 2: Shard by IDW (Warehouse ID)**
- Distinct values: 200 warehouses
- Avg docs/server: 20,000,000 ÷ 1,000 = **20,000 docs/server**
- Avg distinct/server: 200 ÷ 1,000 = **0.2 distinct values/server**
- Server utilization: **20%** (only 200 of 1,000 servers used)
- ⚠️ **Poor distribution - 80% of servers idle**

**Recommendation:** Shard by IDP for optimal distribution

## Technical Details

### Technology Stack
- **Language:** Python 3.7+
- **Dependencies:** Standard library only
  - `json` - JSON Schema parsing
  - `dataclasses` - Clean data models
  - `typing` - Type annotations
- **Architecture:** Modular design with separation of concerns
- **Design Patterns:**
  - Dataclasses for immutable models
  - Strategy pattern for calculators
  - Factory pattern for schema parsing

### Code Quality
- Type hints throughout codebase
- Docstrings for all public methods
- Modular, testable components
- No external dependencies (easy deployment)

## Limitations and Assumptions

### Assumptions
- **Uniform distribution:** Data evenly distributed across sharding key values
- **Average sizes:** Uses average/typical sizes for variable-length fields
- **Static workload:** Document counts and patterns are stable
- **Ideal sharding:** No hotspots or skew in access patterns

### Not Included
- **Indexes:** B-tree, geospatial, text indexes add overhead
- **Compression:** Real systems may compress data (10-50% reduction)
- **Replication:** Multi-replica setups multiply storage by replication factor
- **Network overhead:** Data transfer costs not modeled
- **Query performance:** Focus on storage, not query execution time
- **Write amplification:** Updates to embedded arrays cause document rewrites
- **Working set:** Memory requirements for active dataset

### Simplified Sharding Model
- Assumes perfect hash distribution
- Doesn't model chunk migration costs
- Ignores zone sharding and geo-distribution
- No modeling of compound sharding keys

## Learning Outcomes

By working with this tool, you will understand:

1. **NoSQL Design Trade-offs:**
   - Normalization vs denormalization
   - Read performance vs write complexity
   - Storage efficiency vs query simplicity

2. **Document Size Impact:**
   - How embedding affects document size
   - Overhead of field names and metadata
   - Cost of data duplication

3. **Sharding Principles:**
   - Importance of cardinality in shard key selection
   - Balancing data distribution
   - Avoiding idle servers and hotspots

4. **Capacity Planning:**
   - Estimating storage requirements
   - Scaling strategies for growing datasets
   - Cost implications of design choices

## Contributing

This is an academic project. To contribute:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request with clear description

## License

This project is created for educational purposes as part of a Big Data Structure course (Chapter 2 - TD1).

## Conclusion

This tool provides a comprehensive framework for analyzing NoSQL database designs with a focus on storage sizing and sharding strategies. It demonstrates key concepts in distributed database design and provides practical calculations for capacity planning in document-oriented databases.

The ability to compare multiple denormalization strategies helps understand the trade-offs between storage efficiency, query performance, and system complexity in real-world NoSQL applications.
