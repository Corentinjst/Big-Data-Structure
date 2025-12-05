"""
Nested loop join operator for query execution
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
class JoinResult:
    """Result of a join operation"""
    output_documents: int
    output_size_bytes: int
    cost: QueryCost
    left_sharding_key: Optional[str] = None
    right_sharding_key: Optional[str] = None
    join_key: str = ""
    num_loops: int = 0

    # TD2 Correction format fields (matching table columns)
    s1: int = 0  # Number of documents from left collection
    o1: int = 0  # Number of output documents from left (after filter)
    s2: int = 0  # Number of documents from right collection per loop
    o2: int = 0  # Number of output documents from right per loop
    c1_volume_bytes: int = 0  # C1 = #S1 * size(S1) + #O1 * size(O1)
    c2_volume_bytes: int = 0  # C2 = #S2 * size(S2) + #O2 * size(O2)
    num_messages: int = 0  # Number of messages exchanged


class NestedLoopJoinOperator:
    """
    Nested loop join operator for executing join queries
    """

    def __init__(self, statistics: Statistics):
        """
        Initialize the nested loop join operator

        Args:
            statistics: Database statistics
        """
        self.statistics = statistics
        self.size_calculator = SizeCalculator(statistics)
        self.filter_operator = FilterOperator(statistics)

    def calculate_join_output_size(
        self,
        left_collection: Collection,
        right_collection: Collection,
        output_keys: List[str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> int:
        """
        Calculate the size of join output documents

        Args:
            left_collection: Left collection in join
            right_collection: Right collection in join
            output_keys: Keys to include in output
            array_sizes: Average sizes for arrays

        Returns:
            Size in bytes of output document
        """
        output_schema = Schema(name="join_output")

        for key in output_keys:
            field = left_collection.schema.get_field(key)
            if field:
                output_schema.add_field(field)
            else:
                field = right_collection.schema.get_field(key)
                if field:
                    output_schema.add_field(field)

        return self.size_calculator.calculate_document_size(
            output_schema, array_sizes or {}
        )

    # ----------------------------------------------------------
    # JOIN WITH SHARDING
    # ----------------------------------------------------------
    def nested_loop_join_with_sharding(
        self,
        left_collection: Collection,
        right_collection: Collection,
        join_key: str,
        output_keys: List[str],
        left_sharding_key: str,
        right_sharding_key: str,
        left_filter_key: Optional[str] = None,
        left_filter_selectivity: Optional[float] = None,
        array_sizes: Optional[Dict[str, int]] = None
    ) -> JoinResult:
        """
        Execute a nested loop join with sharding optimization
        """

        left_doc_size = self.size_calculator.calculate_document_size(
            left_collection.schema, array_sizes or {}
        )
        right_doc_size = self.size_calculator.calculate_document_size(
            right_collection.schema, array_sizes or {}
        )
        output_doc_size = self.calculate_join_output_size(
            left_collection, right_collection, output_keys, array_sizes
        )

        # Filter selectivity
        if left_filter_key and left_filter_selectivity:
            effective_left_docs = int(left_collection.document_count * left_filter_selectivity)
        else:
            effective_left_docs = left_collection.document_count
            left_filter_selectivity = 1.0

        both_sharded_on_join = (
            left_sharding_key == join_key and right_sharding_key == join_key
        )

        num_servers = self.statistics.num_servers

        if both_sharded_on_join:
            # Co-located join → in TD2 correction, #msg = 1
            output_documents = effective_left_docs
            num_loops = max(1, effective_left_docs // num_servers)
        else:
            # Broadcast join
            output_documents = effective_left_docs
            num_loops = effective_left_docs

        # NEW: retrieve cost + metadata (C1, C2, loops, messages)
        cost, meta = CostModel.calculate_nested_loop_join_cost(
            left_documents=effective_left_docs,
            right_documents=right_collection.document_count,
            left_doc_size=left_doc_size,
            right_doc_size=right_doc_size,
            output_documents=output_documents,
            output_doc_size=output_doc_size,
            use_sharding=True,
            left_sharded=(left_sharding_key == join_key),
            right_sharded=(right_sharding_key == join_key),
            num_servers=num_servers,
            num_loops=num_loops
        )

        return JoinResult(
            output_documents=output_documents,
            output_size_bytes=output_documents * output_doc_size,
            cost=cost,
            left_sharding_key=left_sharding_key,
            right_sharding_key=right_sharding_key,
            join_key=join_key,
            num_loops=num_loops,

            # TD2 Correction format fields
            s1=left_collection.document_count,
            o1=effective_left_docs,
            s2=right_collection.document_count,
            o2=1,  # Typically 1 matching document per loop
            c1_volume_bytes=meta["c1"],
            c2_volume_bytes=meta["c2"],
            num_messages=meta["messages"]
        )

    # ----------------------------------------------------------
    # JOIN WITHOUT SHARDING
    # ----------------------------------------------------------
    def nested_loop_join_without_sharding(
        self,
        left_collection: Collection,
        right_collection: Collection,
        join_key: str,
        output_keys: List[str],
        left_filter_key: Optional[str] = None,
        left_filter_selectivity: Optional[float] = None,
        array_sizes: Optional[Dict[str, int]] = None
    ) -> JoinResult:
        """
        Execute a nested loop join without sharding optimization
        """

        left_doc_size = self.size_calculator.calculate_document_size(
            left_collection.schema, array_sizes or {}
        )
        right_doc_size = self.size_calculator.calculate_document_size(
            right_collection.schema, array_sizes or {}
        )
        output_doc_size = self.calculate_join_output_size(
            left_collection, right_collection, output_keys, array_sizes
        )

        if left_filter_key and left_filter_selectivity:
            effective_left_docs = int(left_collection.document_count * left_filter_selectivity)
        else:
            effective_left_docs = left_collection.document_count

        output_documents = effective_left_docs
        num_loops = effective_left_docs

        # No sharding → everything is scanned/broadcast
        cost, meta = CostModel.calculate_nested_loop_join_cost(
            left_documents=effective_left_docs,
            right_documents=right_collection.document_count,
            left_doc_size=left_doc_size,
            right_doc_size=right_doc_size,
            output_documents=output_documents,
            output_doc_size=output_doc_size,
            use_sharding=False,
            num_servers=1,
            num_loops=num_loops
        )

        return JoinResult(
            output_documents=output_documents,
            output_size_bytes=output_documents * output_doc_size,
            cost=cost,
            join_key=join_key,
            num_loops=num_loops,

            # TD2 Correction format fields
            s1=left_collection.document_count,
            o1=effective_left_docs,
            s2=right_collection.document_count,
            o2=1,  # Typically 1 matching document per loop
            c1_volume_bytes=meta["c1"],
            c2_volume_bytes=meta["c2"],
            num_messages=meta["messages"]
        )

    # ----------------------------------------------------------
    # AUTO-SELECTION
    # ----------------------------------------------------------
    def execute_join(
        self,
        left_collection: Collection,
        right_collection: Collection,
        join_key: str,
        output_keys: List[str],
        left_sharding_key: Optional[str] = None,
        right_sharding_key: Optional[str] = None,
        left_filter_key: Optional[str] = None,
        left_filter_selectivity: Optional[float] = None,
        array_sizes: Optional[Dict[str, int]] = None
    ) -> JoinResult:
        """
        Execute a nested loop join (auto choose with/without sharding)
        """

        if left_sharding_key and right_sharding_key:
            return self.nested_loop_join_with_sharding(
                left_collection=left_collection,
                right_collection=right_collection,
                join_key=join_key,
                output_keys=output_keys,
                left_sharding_key=left_sharding_key,
                right_sharding_key=right_sharding_key,
                left_filter_key=left_filter_key,
                left_filter_selectivity=left_filter_selectivity,
                array_sizes=array_sizes
            )

        else:
            return self.nested_loop_join_without_sharding(
                left_collection=left_collection,
                right_collection=right_collection,
                join_key=join_key,
                output_keys=output_keys,
                left_filter_key=left_filter_key,
                left_filter_selectivity=left_filter_selectivity,
                array_sizes=array_sizes
            )
