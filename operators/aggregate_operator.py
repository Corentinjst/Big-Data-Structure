"""
Aggregate operator for query execution
Supports joins with and without sharding
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from models.schema import Collection, Schema, Field
from models.statistics import Statistics
from calculators.size_calculator import SizeCalculator
from .cost_model import CostModel, QueryCost
from .filter_operator import FilterOperator, FilterResult


@dataclass
class AggregateResult:
    """Result of a Aggregate operation"""
    output_size_bytes1: int
    input_size_bytes1: int
    shuffle_size_bytes1: int
    output_size_bytes2: int
    input_size_bytes2: int
    shuffle_size_bytes2: int
    cost: QueryCost
    left_sharding_key: Optional[str] = None
    right_sharding_key: Optional[str] = None
    join_key: str = ""
    left_group_by_key: Optional[str] = None
    right_group_by_key: Optional[str] = None
    num_loops: int = 0

    s1: int = 0  # Number of documents from left collection
    o1: int = 0  # Number of output documents from left (after filter)
    shuffle1: int = 0
    s2: int = 0  # Number of documents from right collection per loop
    o2: int = 0  # Number of output documents from right per loop
    shuffle2: int = 0
    c1_volume_bytes: int = 0  # C1 = #S1 * size(S1) + #O1 * size(O1)
    c2_volume_bytes: int = 0  # C2 = #S2 * size(S2) + #O2 * size(O2)


class AggregateOperator:
    """
    Aggregate operator for executing join queries
    """

    def __init__(self, statistics: Statistics):
        """
        Initialize the Aggregate operator

        Args:
            statistics: Database statistics
        """
        self.statistics = statistics
        self.size_calculator = SizeCalculator(statistics)
        self.filter_operator = FilterOperator(statistics)
    
    def calculate_aggregate_input_size(
        self,
        collection: Collection,
        join_key : str,
        output_keys: List[str],
        filter_keys: Optional[List[str]] = None,
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

        if filter_keys :
            for key in filter_keys:
                field = collection.schema.get_field(key)
                if field:
                    output_schema.add_field(field)
        
        field = collection.schema.get_field(join_key)
        if field:
            output_schema.add_field(field)

        # Calculate size
        return self.size_calculator.calculate_document_size(output_schema, {})
    
    def calculate_aggregate_output_size(
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
    
    def calculate_aggregate_shuffle_size(
        self,
        collection: Collection,
        group_by_key : str,
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

        output_schema = Schema(name=f"{collection.schema.name}_output")
        
        # Create a schema with only the group_by key
        field = collection.schema.get_field(group_by_key)
        if field:
            output_schema.add_field(field)

        # Calculate size
        return self.size_calculator.calculate_document_size(output_schema, {})

    def aggregator(
        self,
        left_collection: Collection,
        right_collection: Collection,
        join_key: str,
        limit: int,
        left_output_keys: Optional[List[str]],
        right_output_keys: Optional[List[str]],
        left_sharding_key: Optional[str] = None,
        right_sharding_key: Optional[str] = None,
        left_filter_keys: Optional[List[str]] = None,
        right_filter_keys: Optional[List[str]] = None,
        left_group_by_key: Optional[List[str]] = None,
        right_group_by_key: Optional[List[str]] = None,
        left_filter_selectivity: Optional[float] = None,
        right_filter_selectivity: Optional[float] = None,
        array_sizes: Optional[Dict[str, int]] = None
    ) -> AggregateResult:
        """
        Execute a Aggregate with optional sharding optimization

        Args:
            left_collection: Left collection in join
            right_collection: Right collection in join
            join_key: Key to join on
            output_keys: Keys to include in output
            left_sharding_key: Sharding key for left collection (if any)
            right_sharding_key: Sharding key for right collection (if any)
            left_filter_key: Filter keys on left collection (if any)
            left_filter_selectivity: Filter selectivity on left collection
            array_sizes: Average sizes for arrays

        Returns:
            AggregateResult with output metrics and costs
        """
        # Determine if sharding is used
        use_sharding = bool(left_sharding_key and right_sharding_key)

        #compute to how much server the inside part of the agregate query is sent
        if use_sharding and right_filter_keys and right_sharding_key in right_filter_keys  : 
            s1 = 1
            total_document_accessed_inside = right_collection.document_count/self.statistics.num_servers
        else:
            s1 = self.statistics.num_servers
            total_document_accessed_inside = right_collection.document_count
        
        #compute to how much server the outside part of the agregate query is sent
        if use_sharding and ((left_filter_keys and left_sharding_key in left_filter_keys) or left_sharding_key == join_key): 
            s2 = 1
            total_document_accessed_outside = left_collection.document_count/self.statistics.num_servers
        else:
            s2 = self.statistics.num_servers
            total_document_accessed_outside = left_collection.document_count

        # Compute effective inside documents documents after filter
        o1 = int(right_collection.document_count * right_filter_selectivity)

        # Compute effective outside documents after filter
        o2 = int(left_collection.document_count * left_filter_selectivity)

        # Compute the Shuffle 1
        if not(right_group_by_key) or (use_sharding and right_group_by_key and right_sharding_key == right_group_by_key) : 
            shuffle1 = 0
        else:
            # ATTENTION PAS SURE DU S1
            shuffle1 = o1 * (s1-1)

        # Compute the Shuffle 2
        if not(left_group_by_key) or (use_sharding and left_group_by_key and left_sharding_key == left_group_by_key) : 
            shuffle2 = 0
        else:
            # ATTENTION PAS SURE DU S2
            shuffle2 = o2 * (s2-1) 
        

        #Compute document size
        input_doc_size_1 = self.calculate_aggregate_input_size(right_collection,join_key,right_output_keys,right_filter_keys,array_sizes)
        output_doc_size_1 = self.calculate_aggregate_output_size(right_collection, right_output_keys, array_sizes)
        shuffle_doc_size_1 = self.calculate_aggregate_shuffle_size(right_collection,right_group_by_key)

        input_doc_size_2 = self.calculate_aggregate_input_size(left_collection,join_key,left_output_keys,left_filter_keys,array_sizes)
        output_doc_size_2 = self.calculate_aggregate_output_size(left_collection, left_output_keys, array_sizes)
        shuffle_doc_size_2 = self.calculate_aggregate_shuffle_size(left_collection,left_group_by_key)

        # Compute c1 and c2 volume
        c1_volume = s1 * input_doc_size_1 + o1 * output_doc_size_1 + shuffle1* shuffle_doc_size_1
        c2_volume = s2 * input_doc_size_2 + o2 * output_doc_size_2 + shuffle2*shuffle_doc_size_2

        if limit: num_loops=limit
        else : num_loops = o1

        #Compute cost

        cost = CostModel.calculate_nested_loop_join_cost(
            total_document_accessed_left=total_document_accessed_outside,
            total_document_accessed_right=total_document_accessed_inside,
            doc_size_bytes_left=input_doc_size_1,
            doc_size_bytes_right=input_doc_size_2,
            c1=c1_volume,
            c2=c2_volume,
            num_loops=num_loops,
            use_index=False,
            num_servers_s1=s1,
            num_servers_s2=s2
        )

        return AggregateResult(
            output_size_bytes1=output_doc_size_1,
            input_size_bytes1=input_doc_size_1,
            shuffle_size_bytes1=shuffle_doc_size_1,
            output_size_bytes2=output_doc_size_2,
            input_size_bytes2=input_doc_size_2,
            shuffle_size_bytes2=shuffle_doc_size_2,
            cost=cost,
            left_sharding_key=left_sharding_key,
            right_sharding_key=right_sharding_key,
            join_key=join_key,
            right_group_by_key=right_group_by_key,
            left_group_by_key=left_group_by_key,
            num_loops=num_loops,
            s1=s1,
            o1=o1,
            shuffle1=shuffle1,
            s2=s2,
            o2=o2, 
            shuffle2=shuffle2,
            c1_volume_bytes=c1_volume,
            c2_volume_bytes=c2_volume,
        )