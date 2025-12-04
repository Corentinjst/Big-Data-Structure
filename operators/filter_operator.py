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
    
    # Required fields (no default)
    output_size_bytes: int
    cost: QueryCost
    
    # Optional fields (with defaults)
    s1: int = 0  # Number of input documents scanned
    input_doc_size_bytes: int = 0  # Size of input documents
    o1: int = 0  # Number of output documents
    c1_volume_bytes: int = 0  # C1 = #S1 * size(S1) + #O1 * size(O1)
    num_servers_accessed: int = 1  # Number of servers accessed
    sharding_key: Optional[str] = None
    index_used: bool = False
    
    


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
        return self.size_calculator.calculate_document_size(output_schema, {})
    
    def calculate_input_size(
        self,
        collection: Collection,
        output_keys: List[str],
        filter_keys: List[str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> int:
        """
        Calculate the size of output documents with only selected keys

        Args:
            collection: Source collection
            output_keys: Keys to include in input
            filter_keys: Keys to include in input
            array_sizes: Average sizes for arrays

        Returns:
            Size in bytes of input document
        """
        # Create a schema with only the output keys
        input_schema = Schema(name=f"{collection.schema.name}_input")

        for key in output_keys:
            field = collection.schema.get_field(key)
            if field:
                input_schema.add_field(field)
        
        for key in filter_keys:
            field = collection.schema.get_field(key)
            if field:
                input_schema.add_field(field)

        # Calculate size
        return self.size_calculator.calculate_document_size(input_schema, {})

    def filter(
        self,
        collection: Collection,
        filter_keys: str,
        output_keys: List[str],
        sharding_key: Optional[str],
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
        use_sharding = False
        if sharding_key:
            use_sharding = True


        #calculate to how much server the query is sent
        if use_sharding and sharding_key in filter_keys : 
            s1 = 1
        else:
            s1 = self.statistics.num_servers

        # Calculate output documents
        o1 = int(collection.document_count * selectivity)

        # Calculate document sizes
        input_doc_size = self.calculate_input_size(collection,output_keys,filter_keys,array_sizes)
        output_doc_size = self.calculate_output_size(collection, output_keys, array_sizes)

        # Calculate cost
        cost = CostModel.calculate_filter_cost(
            total_documents=collection.document_count,
            output_documents=o1,
            doc_size_bytes=input_doc_size,
            output_doc_size_bytes=output_doc_size,
            use_sharding=use_sharding,
            use_index=use_sharding and (sharding_key in filter_keys), # First condition to return false if no sharding
            num_servers=s1,
            selectivity=selectivity
        )

        # Calculate C1 volume: #S1 * size(S1) + #O1 * size(O1)
        c1_volume = s1 * input_doc_size + o1 * output_doc_size

        return FilterResult(
            output_size_bytes=output_doc_size,
            input_doc_size_bytes =input_doc_size,
            cost=cost,
            sharding_key=sharding_key,
            index_used=use_index and (sharding_key in filter_keys), # First condition to return false if no sharding
            s1=s1,
            o1=o1,
            c1_volume_bytes=c1_volume
        )

