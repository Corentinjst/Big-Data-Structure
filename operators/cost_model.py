"""
Cost model for query operations
Calculates time, carbon footprint, and price costs
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from config.constants import (
    BANDWIDTH_SPEED_BYTES_PER_MS,
    COST_PER_GB_TRANSFER,
    CARBON_PER_GB_TRANSFER,
    COST_PER_SERVER_MS,
    CARBON_PER_SERVER_MS,
    INDEX_ACCESS_TIME_MS,
    FULL_SCAN_TIME_PER_DOC_MS,
)

@dataclass
class QueryCost:
    """
    Represents the cost of a query operation
    """
    time_ms: float  # Time in milliseconds
    carbon_gco2: float  # Carbon footprint in gCO2
    price_usd: float  # Price in USD

    # Additional metrics
    data_volume_bytes: int = 0  # Total data transferred
    num_documents: int = 0  # Number of documents processed
    num_servers_involved: int = 0  # Number of servers involved

    def __add__(self, other: 'QueryCost') -> 'QueryCost':
        """Add two query costs together"""
        return QueryCost(
            time_ms=self.time_ms + other.time_ms,
            carbon_gco2=self.carbon_gco2 + other.carbon_gco2,
            price_usd=self.price_usd + other.price_usd,
            data_volume_bytes=self.data_volume_bytes + other.data_volume_bytes,
            num_documents=self.num_documents + other.num_documents,
            num_servers_involved=max(self.num_servers_involved, other.num_servers_involved)
        )

    def __str__(self) -> str:
        """String representation of query cost"""
        return (
            f"QueryCost(\n"
            f"  Communication time: {self.time_ms:.3f} ms ({self.time_ms/1000:.3f} s)\n"
            f"  Carbon: {self.carbon_gco2:.2f} gCO2\n"
            f"  Price: ${self.price_usd:.6f} USD\n"
            f"  Data Volume: {self.data_volume_bytes:,} bytes ({self.data_volume_bytes/1024/1024:.2f} MB)\n"
            f"  Documents accessed: {self.num_documents:,}\n"
            f"  Servers involved: {self.num_servers_involved}\n"
            f")"
        )


class CostModel:
    """
    Cost model for calculating query execution costs
    """

    @staticmethod
    def calculate_communication_cost(
        data_volume_bytes: int,
        num_servers_involved: int = 1000,
        num_documents: int = 0
    ) -> QueryCost:
        """
        Calculate the cost of data communication/transfer

        Args:
            data_volume_bytes: Total volume of data transferred in bytes
            num_servers: Number of servers involved in the operation
            num_documents: Number of documents accessed

        Returns:
            QueryCost object with calculated costs
        """
        # Communication time based on bandwidth
        time_ms = data_volume_bytes / BANDWIDTH_SPEED_BYTES_PER_MS

        # Convert to GB for cost calculation
        data_volume_gb = data_volume_bytes / (1024 ** 3)

        # Calculate costs
        carbon = data_volume_gb * CARBON_PER_GB_TRANSFER
        price = data_volume_gb * COST_PER_GB_TRANSFER

        # Add server processing cost
        server_carbon = time_ms * CARBON_PER_SERVER_MS * num_servers_involved
        server_price = time_ms * COST_PER_SERVER_MS * num_servers_involved

        return QueryCost(
            time_ms=time_ms,
            carbon_gco2=carbon + server_carbon,
            price_usd=price + server_price,
            data_volume_bytes=data_volume_bytes,
            num_documents=num_documents,
            num_servers_involved=num_servers_involved
        )

    @staticmethod
    def calculate_scan_cost(
        num_documents: int,
        doc_size_bytes: int,
        use_index: bool = False,
        num_servers_involved: int = 1
    ) -> QueryCost:
        """
        Calculate the cost of scanning documents

        Args:
            num_documents: Number of documents to scan
            doc_size_bytes: Size of each document in bytes
            use_index: Whether an index is used
            num_servers: Number of servers involved

        Returns:
            QueryCost object with calculated costs
        """
        # Time calculation
        if use_index:
            # Index access time per document
            time_ms = num_documents * INDEX_ACCESS_TIME_MS
        else:
            # Full scan time per document
            time_ms = num_documents * FULL_SCAN_TIME_PER_DOC_MS

        # Data volume
        data_volume_bytes = num_documents * doc_size_bytes

        # Server processing cost
        carbon = time_ms * CARBON_PER_SERVER_MS * num_servers_involved
        price = time_ms * COST_PER_SERVER_MS * num_servers_involved

        return QueryCost(
            time_ms=time_ms,
            carbon_gco2=carbon,
            price_usd=price,
            data_volume_bytes=data_volume_bytes,
            num_documents=num_documents,
            num_servers_involved=num_servers_involved
        )

    @staticmethod
    def calculate_filter_cost(
        total_document_accessed: int,
        doc_size_bytes: int,
        c1: int,
        use_index: bool = False,
        num_servers_involved: int = 1000,
    ) -> QueryCost:
        """
        Calculate the cost of a filter operation

        Args:
            total_document_accessed: Total number of documents to access
            doc_size_bytes: Size of input documents
            c1: C1 volume: #S1 * size(S1) + #O1 * size(O1)
            use_index: Whether an index is used
            num_servers_involved: Number of servers in cluster

        Returns:
            QueryCost object with calculated costs
        """
        # Each server scans its portion
        scan_cost = CostModel.calculate_scan_cost(
            num_documents=total_document_accessed,
            doc_size_bytes=doc_size_bytes,
            use_index=use_index,
            num_servers_involved=num_servers_involved
        )

        # Communication cost for gathering results
        comm_cost = CostModel.calculate_communication_cost(
            data_volume_bytes=c1,
            num_servers_involved=num_servers_involved,
            num_documents=total_document_accessed
        )

        return comm_cost

    @staticmethod
    def calculate_nested_loop_join_cost(
        total_document_accessed_left: int,
        total_document_accessed_right: int,
        doc_size_bytes_left: int,
        doc_size_bytes_right: int,
        c1: int,
        c2: int,
        num_loops: int,
        use_index: bool = False,
        num_servers_s1: int = 1000,  # Server Involved for the left part of the query
        num_servers_s2: int = 1000,  # Server Involved for the right part of the query
    ) -> Tuple[QueryCost, int]:
        """
        Calculate the cost of a nested loop join operation

        Args:
            total_document_accessed_left: Total number of documents accessed on the left part of the query
             total_document_accessed_right: Total number of documents accessed on the right part of the query
            doc_size_bytes: Average size of documents
            c1: C1 volume: #S1 * size(S1) + #O1 * size(O1)
            c2: C2 volume: #S2 * size(S2) + #O2 * size(O2)
            num_loops: Number of loop iterations
            use_index: Whether an index is used
            num_servers_s1: Number of servers in cluster for left part
            num_servers_s2: Number of servers in cluster for right part

        Returns:
            Tuple of (QueryCost object, num_messages)
        """

        # Scan cost for accessing documents for the left part of the query
        scan_cost_1 = CostModel.calculate_scan_cost(
            num_documents=total_document_accessed_left,
            doc_size_bytes=doc_size_bytes_left,
            use_index=use_index,
            num_servers_involved=num_servers_s1
        )

        # Scan cost for accessing documents for the right part of the query

        scan_cost_2_single = CostModel.calculate_scan_cost(
            num_documents=total_document_accessed_right,
            doc_size_bytes=doc_size_bytes_right,
            use_index=use_index,
            num_servers_involved=num_servers_s2
        )

        scan_cost_total = QueryCost(
            time_ms=scan_cost_2_single.time_ms * num_loops,
            carbon_gco2=scan_cost_2_single.carbon_gco2 * num_loops,
            price_usd=scan_cost_2_single.price_usd * num_loops,
            data_volume_bytes=scan_cost_2_single.data_volume_bytes * num_loops,
            num_documents=scan_cost_2_single.num_documents * num_loops,
            num_servers_involved=num_servers_s2
        )

        # Communication cost for C1
        c1_cost = CostModel.calculate_communication_cost(
            data_volume_bytes=c1,
            num_servers_involved=num_servers_s1,
            num_documents=total_document_accessed_left
        )

        # Communication cost for C2 (multiplied by num_loops)
        c2_cost_single = CostModel.calculate_communication_cost(
            data_volume_bytes=c2,
            num_servers_involved=num_servers_s2,
            num_documents=total_document_accessed_right
        )

        c2_cost_total = QueryCost(
            time_ms=c2_cost_single.time_ms * num_loops,
            carbon_gco2=c2_cost_single.carbon_gco2 * num_loops,
            price_usd=c2_cost_single.price_usd * num_loops,
            data_volume_bytes=c2_cost_single.data_volume_bytes * num_loops,
            num_documents=c2_cost_single.num_documents * num_loops,
            num_servers_involved=num_servers_s2
        )

        # Total cost
        total_comm_cost = c1_cost + c2_cost_total

        return total_comm_cost


