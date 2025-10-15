"""
Representation of JSON Schema and related metadata
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

@dataclass
class Field:
    """Represents a single field in a schema"""
    name: str
    field_type: str  # 'integer', 'string', 'object', 'array', etc.
    is_required: bool = True
    is_long_string: bool = False  # For descriptions, comments, etc.
    nested_schema: Optional['Schema'] = None
    array_item_schema: Optional['Schema'] = None
    
@dataclass
class Schema:
    """Represents a JSON Schema for a collection"""
    name: str
    fields: List[Field] = field(default_factory=list)
    
    def add_field(self, field: Field):
        """Add a field to the schema"""
        self.fields.append(field)
    
    def get_field(self, name: str) -> Optional[Field]:
        """Get a field by name"""
        for field in self.fields:
            if field.name == name:
                return field
        return None

@dataclass
class Database:
    """Represents a complete database with multiple collections"""
    name: str
    collections: Dict[str, 'Collection'] = field(default_factory=dict)
    
    def add_collection(self, collection: 'Collection'):
        """Add a collection to the database"""
        self.collections[collection.name] = collection
    
    def get_collection(self, name: str) -> Optional['Collection']:
        """Get a collection by name"""
        return self.collections.get(name)