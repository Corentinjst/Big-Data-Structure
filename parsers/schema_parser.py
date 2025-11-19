"""
Parse Json Schema to Collection
"""

import json
from typing import Dict, Any
from models.schema import Schema, Field, Database, Collection
from models.statistics import Statistics

class SchemaParser:
    """Parse JSON Schema into internal Schema objects (strict version)"""
    
    @staticmethod
    def parse_from_dict(schema_dict: Dict[str, Any], name: str = "root") -> Schema:
        """
        Parses a JSON Schema dictionary and converts it into an internal `Schema` object.

        Args:
            schema_dict :
                The JSON Schema definition to convert.
            name : 
                The name assigned to the resulting `Schema` (useful when handling nested schemas).

        Returns:
            Schema
                The fully constructed internal representation of the JSON Schema.
        """
        schema = Schema(name=name)
        
        if 'properties' not in schema_dict:
            return schema
        
        properties = schema_dict['properties']
        required = schema_dict.get('required', [])
        
        for field_name, field_def in properties.items():
            field = SchemaParser._parse_field(
                field_name, 
                field_def, 
                is_required=field_name in required
            )
            schema.add_field(field)
        
        return schema
    
    @staticmethod
    def parse_multiple_from_file(filepath: str) -> Dict[str, Schema]:
        """
        Reads a JSON file containing multiple schemas at its top level
        and returns a dict mapping schema names to Schema objects.
        
        Expected JSON format:
        {
            "Product_DB1": { ...schema... },
            "Stock_DB1": { ...schema... },
            "Warehouse_DB1": { ... },
            ...
        }
        
        Returns:
        {
            "Product_DB1": Schema(...),
            "Stock_DB1": Schema(...),
            ...
        }
        """
        with open(filepath, 'r') as f:
            schemas_dict = json.load(f)
        
        result: Dict[str, Schema] = {}
        for schema_name, schema_dict in schemas_dict.items():
            result[schema_name] = SchemaParser.parse_from_dict(schema_dict, name=schema_name)
        
        return result
    
    @staticmethod
    def _parse_field(name: str, definition: Dict[str, Any], is_required: bool = True) -> Field:
        """
        Parses a single field from a JSON Schema and converts it into a `Field` object.

        Args:
            name : 
                The name of the field.
            definition : 
                The JSON Schema definition of the field.
            is_required : 
                Whether this field is marked as required in the parent schema.

        Returns:
            Field
                The fully interpreted field, including nested schemas if applicable.
        """
        field_type = definition.get('type', 'string')
        
        # Handle nested objects
        if field_type == 'object':
            nested_schema = SchemaParser.parse_from_dict(definition, name=name)
            return Field(
                name=name,
                field_type=field_type,
                is_required=is_required,
                nested_schema=nested_schema
            )
        
        # Handle arrays
        elif field_type == 'array':
            items_def = definition.get('items', {})
            array_item_schema = SchemaParser.parse_from_dict(items_def, name=f"{name}_item")
            return Field(
                name=name,
                field_type=field_type,
                is_required=is_required,
                array_item_schema=array_item_schema
            )
        
        # Handle basic types
        else:
            fmt = definition.get('format')

            # Explicit semantic types based on JSON Schema "format"
            if field_type == 'string':
                if fmt == 'date':
                    field_type = 'date'
                elif fmt == 'longstring':
                    field_type = 'longstring'
                # otherwise keep field_type = 'string'
            
            return Field(
                name=name,
                field_type=field_type,
                is_required=is_required
            )

    @staticmethod
    def build_db_from_json(db_index: int, stats: Statistics, file_path: str) -> Database:
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
        schemas = SchemaParser.parse_multiple_from_file(file_path)
        
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