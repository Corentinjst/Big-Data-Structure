"""
Collection metadata including schema and statistics
"""

from dataclasses import dataclass
from typing import Dict, Optional
from .schema import Schema

@dataclass
class Collection:
    """Represents a collection with schema and statistics"""
    name: str
    schema: Schema
    document_count: int
    sharding_key: Optional[str] = None
    distinct_shard_values: Optional[int] = None
    
    def __post_init__(self):
        # Calculated fields
        self._doc_size: Optional[int] = None
        self._collection_size: Optional[int] = None