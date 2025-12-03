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
    COMPARISON_TIME_MS
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
    num_servers: int = 0  # Number of servers involved

    def __add__(self, other: 'QueryCost') -> 'QueryCost':
        """Add two query costs together"""
        return QueryCost(
            time_ms=self.time_ms + other.time_ms,
            carbon_gco2=self.carbon_gco2 + other.carbon_gco2,
            price_usd=self.price_usd + other.price_usd,
            data_volume_bytes=self.data_volume_bytes + other.data_volume_bytes,
            num_documents=self.num_documents + other.num_documents,
            num_servers=max(self.num_servers, other.num_servers)
        )

    def __str__(self) -> str:
        """String representation of query cost"""
        return (
            f"QueryCost(\n"
            f"  Time: {self.time_ms:.2f} ms ({self.time_ms/1000:.3f} s)\n"
            f"  Carbon: {self.carbon_gco2:.2f} gCO2\n"
            f"  Price: ${self.price_usd:.6f} USD\n"
            f"  Data Volume: {self.data_volume_bytes:,} bytes ({self.data_volume_bytes/1024/1024:.2f} MB)\n"
            f"  Documents: {self.num_documents:,}\n"
            f"  Servers: {self.num_servers}\n"
            f")"
        )


class CostModel:
    """
    Cost model for calculating query execution costs
    """

    @staticmethod
    def calculate_communication_cost(
        data_volume_bytes: int,
        num_servers: int = 1
    ) -> QueryCost:
        """
        Calculate the cost of data communication/transfer

        Args:
            data_volume_bytes: Total volume of data transferred in bytes
            num_servers: Number of servers involved in the operation

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
        server_carbon = time_ms * CARBON_PER_SERVER_MS * num_servers
        server_price = time_ms * COST_PER_SERVER_MS * num_servers

        return QueryCost(
            time_ms=time_ms,
            carbon_gco2=carbon + server_carbon,
            price_usd=price + server_price,
            data_volume_bytes=data_volume_bytes,
            num_servers=num_servers
        )

    @staticmethod
    def calculate_scan_cost(
        num_documents: int,
        doc_size_bytes: int,
        use_index: bool = False,
        num_servers: int = 1
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
        carbon = time_ms * CARBON_PER_SERVER_MS * num_servers
        price = time_ms * COST_PER_SERVER_MS * num_servers

        return QueryCost(
            time_ms=time_ms,
            carbon_gco2=carbon,
            price_usd=price,
            data_volume_bytes=data_volume_bytes,
            num_documents=num_documents,
            num_servers=num_servers
        )

    @staticmethod
    def calculate_filter_cost(
        total_documents: int,
        output_documents: int,
        doc_size_bytes: int,
        output_doc_size_bytes: int,
        use_sharding: bool = True,
        use_index: bool = False,
        num_servers: int = 1000,
        selectivity: Optional[float] = None
    ) -> QueryCost:
        """
        Calculate the cost of a filter operation

        Args:
            total_documents: Total number of documents in collection
            output_documents: Number of documents in result
            doc_size_bytes: Size of input documents
            output_doc_size_bytes: Size of output documents
            use_sharding: Whether sharding is used
            use_index: Whether an index is used
            num_servers: Number of servers in cluster
            selectivity: Filter selectivity (optional)

        Returns:
            QueryCost object with calculated costs
        """
        if selectivity is None:
            selectivity = output_documents / total_documents if total_documents > 0 else 0

        if use_sharding:
            # Documents distributed across servers
            docs_per_server = total_documents / num_servers
            output_per_server = output_documents / num_servers

            # Each server scans its portion
            scan_cost = CostModel.calculate_scan_cost(
                num_documents=int(docs_per_server),
                doc_size_bytes=doc_size_bytes,
                use_index=use_index,
                num_servers=num_servers
            )

            # Communication cost for gathering results
            comm_cost = CostModel.calculate_communication_cost(
                data_volume_bytes=int(output_documents * output_doc_size_bytes),
                num_servers=num_servers
            )

            return scan_cost + comm_cost
        else:
            # All documents on single server or no sharding benefit
            scan_cost = CostModel.calculate_scan_cost(
                num_documents=total_documents,
                doc_size_bytes=doc_size_bytes,
                use_index=use_index,
                num_servers=1
            )

            # Output communication cost
            comm_cost = CostModel.calculate_communication_cost(
                data_volume_bytes=output_documents * output_doc_size_bytes,
                num_servers=1
            )

            return scan_cost + comm_cost

    @staticmethod
    def calculate_nested_loop_join_cost(
        left_documents: int,
        right_documents: int,
        left_doc_size: int,
        right_doc_size: int,
        output_documents: int,
        output_doc_size: int,
        use_sharding: bool = True,
        left_sharded: bool = True,
        right_sharded: bool = True,
        num_servers: int = 1000,
        num_loops: int = None
    ) -> Tuple[QueryCost, Dict[str, int]]:
        
        if num_loops is None:
            num_loops = left_documents

        # ---- NEW: compute C1 and C2 exactly like the TD correction ----
        c1_volume = left_documents * left_doc_size
        c2_volume = right_documents * right_doc_size
        
        # Determine number of messages like the correction table
        if use_sharding and left_sharded and right_sharded:
            num_messages = 1   # co-located join
        else:
            num_messages = num_loops

        # ---- existing cost computation (kept as-is) ----
        total_cost = QueryCost(time_ms=0, carbon_gco2=0, price_usd=0)

        if use_sharding:

            c1_cost = CostModel.calculate_communication_cost(
                data_volume_bytes=c1_volume,
                num_servers=num_servers
            )
            total_cost += c1_cost

            c2_cost = CostModel.calculate_communication_cost(
                data_volume_bytes=c2_volume,
                num_servers=num_servers
            )

            loop_cost = QueryCost(
                time_ms=c2_cost.time_ms * num_loops,
                carbon_gco2=c2_cost.carbon_gco2 * num_loops,
                price_usd=c2_cost.price_usd * num_loops,
                data_volume_bytes=c2_cost.data_volume_bytes * num_loops,
                num_documents=c2_cost.num_documents * num_loops,
                num_servers=num_servers
            )

            total_cost += loop_cost

            output_cost = CostModel.calculate_communication_cost(
                data_volume_bytes=output_documents * output_doc_size,
                num_servers=num_servers
            )
            total_cost += output_cost

        else:
            scan_cost = CostModel.calculate_scan_cost(
                num_documents=left_documents * right_documents,
                doc_size_bytes=(left_doc_size + right_doc_size) // 2,
                use_index=False,
                num_servers=num_servers
            )

            output_cost = CostModel.calculate_communication_cost(
                data_volume_bytes=output_documents * output_doc_size,
                num_servers=num_servers
            )

            total_cost = scan_cost + output_cost

        # ---- NEW: return also C1, C2, loops and #messages ----
        meta = {
            "c1": c1_volume,
            "c2": c2_volume,
            "loops": num_loops,
            "messages": num_messages
        }

        return total_cost, meta

