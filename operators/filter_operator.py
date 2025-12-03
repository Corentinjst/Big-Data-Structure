"""
Filter operator for query execution
Supports filtering with and without sharding
"""

from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from models.schema import Collection, Schema, Field
from models.statistics import Statistics
from calculators.size_calculator import SizeCalculator
from .cost_model import CostModel, QueryCost


@dataclass
class FilterResult:
    """Result of a filter operation"""
    output_documents: int
    output_size_bytes: int
    cost: QueryCost
    sharding_key: Optional[str] = None
    index_used: bool = False

    # TD2 Correction format fields
    s1: int = 0  # Number of input documents scanned
    o1: int = 0  # Number of output documents
    loops: int = 1  # Number of loop iterations (usually 1 for filters)
    c1_volume_bytes: int = 0  # C1 = #S1 * size(S1) + #O1 * size(O1)
    num_servers_accessed: int = 1  # Number of servers accessed


class FilterOperator:
    """
    Filter operator for executing filter queries on collections
    """

    def __init__(self, statistics: Statistics):
        """
        Initialize the filter operator

        Args:
            statistics: Database statistics
        """
        self.statistics = statistics
        self.size_calculator = SizeCalculator(statistics)

    def calculate_output_size(
        self,
        collection: Collection,
        output_keys: List[str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> int:
        """
        Calculate the size of output documents with only selected keys

        Args:
            collection: Source collection
            output_keys: Keys to include in output
            array_sizes: Average sizes for arrays

        Returns:
            Size in bytes of output document
        """
        # Create a schema with only the output keys
        output_schema = Schema(name=f"{collection.schema.name}_output")

        for key in output_keys:
            field = collection.schema.get_field(key)
            if field:
                output_schema.add_field(field)

        # Calculate size
        return self.size_calculator.calculate_document_size(output_schema, array_sizes or {})

    def filter_with_sharding(
        self,
        collection: Collection,
        filter_key: str,
        output_keys: List[str],
        sharding_key: str,
        selectivity: Optional[float] = None,
        use_index: bool = True,
        array_sizes: Optional[Dict[str, int]] = None
    ) -> FilterResult:
        """
        Execute a filter query with sharding optimization

        Args:
            collection: Target collection
            filter_key: Key to filter on
            output_keys: Keys to include in output
            sharding_key: Sharding key for the collection
            selectivity: Filter selectivity (fraction of documents matching)
            use_index: Whether an index exists on the filter key
            array_sizes: Average sizes for arrays

        Returns:
            FilterResult with output metrics and costs
        """
        # Calculate document sizes
        input_doc_size = self.size_calculator.calculate_document_size(
            collection.schema,
            array_sizes or {}
        )
        output_doc_size = self.calculate_output_size(collection, output_keys, array_sizes)

        # Determine selectivity
        if selectivity is None:
            # Default selectivity based on filter key
            if filter_key == sharding_key:
                # Exact match on sharding key - very selective
                selectivity = 1 / collection.document_count
            else:
                # Assume moderate selectivity
                selectivity = 0.01

        # Calculate output documents
        output_documents = int(collection.document_count * selectivity)

        # Determine if sharding helps
        shard_helps = (filter_key == sharding_key)

        if shard_helps:
            # Query goes to specific shard(s)
            num_servers_accessed = max(1, int(self.statistics.num_servers * selectivity))
        else:
            # Query must go to all servers
            num_servers_accessed = self.statistics.num_servers

        # Calculate cost
        cost = CostModel.calculate_filter_cost(
            total_documents=collection.document_count,
            output_documents=output_documents,
            doc_size_bytes=input_doc_size,
            output_doc_size_bytes=output_doc_size,
            use_sharding=True,
            use_index=use_index and (filter_key == sharding_key or use_index),
            num_servers=num_servers_accessed,
            selectivity=selectivity
        )

        # Calculate C1 volume: #S1 * size(S1) + #O1 * size(O1)
        c1_volume = collection.document_count * input_doc_size + output_documents * output_doc_size

        return FilterResult(
            output_documents=output_documents,
            output_size_bytes=output_documents * output_doc_size,
            cost=cost,
            sharding_key=sharding_key,
            index_used=use_index,
            # TD2 Correction format fields
            s1=collection.document_count,
            o1=output_documents,
            loops=1,
            c1_volume_bytes=c1_volume,
            num_servers_accessed=num_servers_accessed
        )

    def filter_without_sharding(
        self,
        collection: Collection,
        filter_key: str,
        output_keys: List[str],
        selectivity: Optional[float] = None,
        use_index: bool = False,
        array_sizes: Optional[Dict[str, int]] = None
    ) -> FilterResult:
        """
        Execute a filter query without sharding optimization

        Args:
            collection: Target collection
            filter_key: Key to filter on
            output_keys: Keys to include in output
            selectivity: Filter selectivity (fraction of documents matching)
            use_index: Whether an index exists on the filter key
            array_sizes: Average sizes for arrays

        Returns:
            FilterResult with output metrics and costs
        """
        # Calculate document sizes
        input_doc_size = self.size_calculator.calculate_document_size(
            collection.schema,
            array_sizes or {}
        )
        output_doc_size = self.calculate_output_size(collection, output_keys, array_sizes)

        # Determine selectivity
        if selectivity is None:
            selectivity = 0.01  # Default 1% selectivity

        # Calculate output documents
        output_documents = int(collection.document_count * selectivity)

        # Calculate cost without sharding benefit
        cost = CostModel.calculate_filter_cost(
            total_documents=collection.document_count,
            output_documents=output_documents,
            doc_size_bytes=input_doc_size,
            output_doc_size_bytes=output_doc_size,
            use_sharding=False,
            use_index=use_index,
            num_servers=1,
            selectivity=selectivity
        )

        # Calculate C1 volume: #S1 * size(S1) + #O1 * size(O1)
        c1_volume = collection.document_count * input_doc_size + output_documents * output_doc_size

        return FilterResult(
            output_documents=output_documents,
            output_size_bytes=output_documents * output_doc_size,
            cost=cost,
            sharding_key=None,
            index_used=use_index,
            # TD2 Correction format fields
            s1=collection.document_count,
            o1=output_documents,
            loops=1,
            c1_volume_bytes=c1_volume,
            num_servers_accessed=1
        )

    def execute_filter(
        self,
        collection: Collection,
        filter_key: str,
        output_keys: List[str],
        sharding_key: Optional[str] = None,
        selectivity: Optional[float] = None,
        use_index: bool = True,
        array_sizes: Optional[Dict[str, int]] = None
    ) -> FilterResult:
        """
        Execute a filter query (automatically chooses with/without sharding)

        Args:
            collection: Target collection
            filter_key: Key to filter on
            output_keys: Keys to include in output
            sharding_key: Sharding key for the collection (if any)
            selectivity: Filter selectivity (fraction of documents matching)
            use_index: Whether an index exists on the filter key
            array_sizes: Average sizes for arrays

        Returns:
            FilterResult with output metrics and costs
        """
        if sharding_key:
            return self.filter_with_sharding(
                collection=collection,
                filter_key=filter_key,
                output_keys=output_keys,
                sharding_key=sharding_key,
                selectivity=selectivity,
                use_index=use_index,
                array_sizes=array_sizes
            )
        else:
            return self.filter_without_sharding(
                collection=collection,
                filter_key=filter_key,
                output_keys=output_keys,
                selectivity=selectivity,
                use_index=use_index,
                array_sizes=array_sizes
            )
