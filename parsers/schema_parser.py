"""
Parse JSON Schema files into internal representation
"""

import json
from typing import Dict, Any
from models.schema import Schema, Field

class SchemaParser:
    """Parse JSON Schema into internal Schema objects"""
    
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
            # Check if it's a long string (description, comment, etc.)
            is_long = name.lower() in ['description', 'comment', 'address']
            
            return Field(
                name=name,
                field_type=field_type,
                is_required=is_required,
                is_long_string=is_long
            )