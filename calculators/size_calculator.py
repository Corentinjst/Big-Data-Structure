"""
Calculate sizes of documents, collections, and databases
"""

from typing import Dict, Optional
from models.schema import Schema, Field
from models.collection import Collection
from models.statistics import Statistics
from config.constants import *

class SizeCalculator:
    """Calculate sizes for schemas and collections"""
    
    def __init__(self, statistics: Statistics):
        self.stats = statistics
    
    def calculate_document_size(self, schema: Schema, 
                               array_sizes: Optional[Dict[str, int]] = None) -> int:
        """
        Calculate the size of a single document in bytes
        
        Args:
            schema: The schema to calculate size for
            array_sizes: Dictionary of array field names to their average sizes
        
        Returns:
            Size in bytes
        """
        if array_sizes is None:
            array_sizes = {}
        
        total_size = 0
        
        for field in schema.fields:
            field_size = self._calculate_field_size(field, array_sizes)
            total_size += field_size
        
        return total_size
    
    def _calculate_field_size(self, field: Field, 
                             array_sizes: Dict[str, int]) -> int:
        """Calculate size of a single field"""
        
        # Add key-value overhead
        size = KEY_VALUE_OVERHEAD
        
        # Basic types
        if field.field_type == 'integer':
            size += INTEGER_SIZE
        
        elif field.field_type == 'number':
            size += NUMBER_SIZE
        
        elif field.field_type == 'string':
            if field.name == 'date' or field.name.endswith('Date'):
                size += DATE_SIZE
            elif field.is_long_string:
                size += LONG_STRING_SIZE
            else:
                size += STRING_SIZE
        
        # Nested objects
        elif field.field_type == 'object' and field.nested_schema:
            nested_size = self.calculate_document_size(
                field.nested_schema, 
                array_sizes
            )
            size += nested_size
        
        # Arrays
        elif field.field_type == 'array' and field.array_item_schema:
            array_count = array_sizes.get(field.name, 1)
            item_size = self.calculate_document_size(
                field.array_item_schema,
                array_sizes
            )
            size += ARRAY_OVERHEAD + (array_count * item_size)
        
        return size
    
    def calculate_collection_size(self, collection: Collection,
                                  array_sizes: Optional[Dict[str, int]] = None) -> int:
        """
        Calculate total collection size in bytes
        
        Args:
            collection: Collection to calculate size for
            array_sizes: Dictionary of array field names to their average sizes
        
        Returns:
            Size in bytes
        """
        doc_size = self.calculate_document_size(collection.schema, array_sizes)
        collection._doc_size = doc_size
        
        total_size = doc_size * collection.document_count
        collection._collection_size = total_size
        
        return total_size
    
    def calculate_database_size(self, database) -> int:
        """
        Calculate total database size in bytes
        
        Returns:
            Size in bytes
        """
        total_size = 0
        
        for collection in database.collections.values():
            if collection._collection_size is None:
                # Need to calculate collection size first
                # Note: This is simplified - in reality you'd need array_sizes
                self.calculate_collection_size(collection)
            
            total_size += collection._collection_size
        
        return total_size
    
    @staticmethod
    def bytes_to_gb(bytes_size: int) -> float:
        """Convert bytes to gigabytes"""
        return bytes_size / (1024 ** 3)
    
    @staticmethod
    def bytes_to_human_readable(bytes_size: int) -> str:
        """Convert bytes to human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.2f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.2f} PB"