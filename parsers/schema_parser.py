"""
Parse JSON Schema files into internal representation (Version 2)

Changes from schema_parser.py:
- Does NOT auto-convert fields to "longstring" based on field name
- Respects JSON "type" field exactly as written
- Converts to "date" or "longstring" only if JSON explicitly contains
  "format": "date" or "format": "longstring"
- Supports nested objects and arrays as before
- New function parse_multiple_from_file() for multi-schema JSON files
"""

import json
from typing import Dict, Any
from models.schema import Schema, Field

class SchemaParser:
    """Parse JSON Schema into internal Schema objects (strict version)"""
    
    @staticmethod
    def parse_from_file(filepath: str) -> Schema:
        """Load and parse JSON Schema from file"""
        with open(filepath, 'r') as f:
            schema_dict = json.load(f)
        return SchemaParser.parse_from_dict(schema_dict)
    
    @staticmethod
    def parse_from_dict(schema_dict: Dict[str, Any], name: str = "root") -> Schema:
        """Parse JSON Schema dictionary into Schema object"""
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
        """Parse a single field definition"""
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
