# NoSQL Database Analysis Tool - Explanations

## Overview

This project is a **NoSQL Database Analysis Tool** designed for analyzing and optimizing database designs, particularly for MongoDB-style document databases. It was created as part of a Big Data Structure course homework assignment (Chapter 2).

The tool models an e-commerce database system and provides:
- Document size calculations
- Collection size estimations
- Database sizing analysis
- Sharding strategy comparisons
- Performance optimization recommendations

## Project Structure

```
Big-Data-Structure/
├── models/              # Data models for schemas, collections, and databases
│   ├── schema.py        # Schema, Field, and Collections definitions
│   └── statistics.py    # Database statistics
├── parsers/             # Parse JSON schemas
│   └── schema_parser.py # JSON Schema parser
├── calculators/         # Size and sharding calculations
│   ├── size_calculator.py   # Document/collection size calculator
│   └── shard_calculator.py  # Sharding distribution analyzer
├── config/              # Configuration constants
│   └── constants.py     # Data type size constants
├── tests/               # Test suite
│   └── TD1.ipynb        # Test on Jupyter for TD1
└── main.py              # Main demo program
```

## Core Components

### 1. Models (`models/`)

#### Schema & Field (`schema.py`)
- **Field**: Represents a single field in a document schema
  - Supports basic types: `integer`, `number`, `string`
  - Supports complex types: `object` (nested documents), `array`
  - Tracks metadata: required status, long strings, nested schemas

- **Schema**: Represents a complete document schema for a collection
  - Contains a list of fields
  - Supports nested schemas for embedded documents

- **Database**: Represents a complete database with multiple collections
  - Manages multiple collections
  - Provides collection lookup by name

#### Collection (`collection.py`)
- Represents a MongoDB-style collection with metadata
- Tracks:
  - Schema definition
  - Document count
  - Sharding configuration (key and distinct values)
  - Calculated sizes (cached)

#### Statistics (`statistics.py`)
- Contains database statistics and assumptions:
  - **10 million clients** (10^7)
  - **100,000 products** (10^5)
  - **4 billion order lines** (4×10^9)
  - **200 warehouses**
  - **1,000 servers** for sharding
  - Other derived statistics

### 2. Parsers (`parsers/`)

#### SchemaParser (`schema_parser.py`)
Parses JSON Schema definitions into internal Schema objects:
- Reads JSON Schema files
- Converts schema dictionaries into Field and Schema objects
- Handles nested objects and arrays
- Identifies long strings (description, comment, address fields)
- Supports both file-based and dictionary-based parsing

### 3. Calculators (`calculators/`)

#### SizeCalculator (`size_calculator.py`)
Calculates storage requirements for documents and collections:

**Document Size Calculation**:
- Iterates through all fields in a schema
- Adds type-specific sizes (from `constants.py`):
  - Integer: 8 bytes
  - Number (float): 8 bytes
  - String: 80 bytes (default)
  - Date: 20 bytes
  - Long string: 200 bytes
  - Key-value overhead: 12 bytes per field
- Handles nested objects recursively
- Handles arrays with configurable item counts

**Collection Size Calculation**:
- Multiplies document size by document count
- Caches calculated sizes in the collection object

**Database Size Calculation**:
- Sums all collection sizes in the database

**Utility Functions**:
- `bytes_to_gb()`: Convert bytes to gigabytes
- `bytes_to_human_readable()`: Format bytes as KB/MB/GB/TB

#### ShardCalculator (`shard_calculator.py`)
Analyzes sharding strategies for horizontal scaling:

**Distribution Calculation**:
- Calculates average documents per server
- Calculates average distinct values per server
- Determines server utilization (what percentage of servers have data)
- Flags under-utilization (when distinct values < servers)

**Strategy Comparison**:
- Compares multiple sharding key candidates
- Provides metrics for each strategy:
  - Total documents
  - Distinct values for the sharding key
  - Average docs per server
  - Server utilization percentage
  - Skew warnings

**Recommendation Engine**:
- Scores sharding strategies based on:
  - Server utilization (70% weight)
  - Distinct value distribution (30% weight)
- Recommends optimal sharding key

### 4. Configuration (`config/`)

#### Constants (`constants.py`)
Defines byte sizes for different data types based on MongoDB storage:
- Basic types: integers (8 bytes), numbers (8 bytes)
- Strings: regular (80 bytes), dates (20 bytes), long strings (200 bytes)
- Overhead: key-value pairs (12 bytes), arrays (12 bytes)

## How It Works

### Workflow

1. **Define Schemas**
   - Create schema objects programmatically or parse from JSON
   - Define fields with types and metadata
   - Handle nested objects and arrays

2. **Create Collections**
   - Associate schemas with collections
   - Specify document counts
   - Configure sharding parameters

3. **Calculate Sizes**
   ```
   Document Size = Σ(field_size + key_value_overhead)
   Collection Size = document_size × document_count
   Database Size = Σ(collection_sizes)
   ```

4. **Analyze Sharding**
   - Compare different sharding keys
   - Calculate distribution metrics
   - Identify optimal strategies

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

## Running the Demo

Execute the main program to see calculations for a sample e-commerce database:

```bash
python main.py
```

The demo outputs:
- Document sizes for each collection
- Collection sizes in human-readable format
- Total database size
- Sharding analysis for Stock, OrderLine, and Product collections
- Recommendations and warnings

## Key Concepts

### Document-Oriented Storage
- Data stored as JSON-like documents
- Each document can have different fields (schema-less)
- Nested objects and arrays supported

### Size Calculation
- Accounts for field names, values, and metadata overhead
- Considers data type sizes
- Handles variable-length strings with averages

### Horizontal Sharding
- Distributes data across multiple servers
- Uses a sharding key to determine document placement
- Goals: even distribution, high utilization, query efficiency

### Cardinality
- Number of distinct values for a field
- High cardinality (e.g., Product ID): good for sharding
- Low cardinality (e.g., Warehouse ID): poor for sharding

## Extending the Tool

To add new functionality:

1. **New Collections**: Create schema functions in `main.py`
2. **Custom Calculators**: Add modules to `calculators/`
3. **Alternative Storage Models**: Modify constants in `config/constants.py`
4. **JSON Schema Import**: Use `SchemaParser.parse_from_file()`
5. **Additional Metrics**: Extend `ShardCalculator` with new algorithms

## Technical Details

- **Language**: Python 3
- **Dependencies**: Standard library only (json, dataclasses, typing)
- **Data Structures**: Dataclasses for clean, type-safe models
- **Architecture**: Modular design with separation of concerns
- **Extensibility**: Easy to add new calculators and analyzers

## Limitations

- Assumes uniform data distribution
- Uses average sizes (actual documents may vary)
- Does not account for:
  - Indexes
  - Compression
  - Replication overhead
  - Network costs
  - Query performance
- Simplified sharding model (real-world has more complexity)

## Conclusion

This tool provides a framework for analyzing NoSQL database designs with a focus on storage sizing and sharding strategies. It demonstrates key concepts in distributed database design and provides practical calculations for capacity planning.
