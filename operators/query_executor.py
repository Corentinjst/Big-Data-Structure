"""
Query executor for running predefined queries (Q1-Q7)
Demonstrates usage of filter and join operators
"""

from typing import Dict, Optional, List, Tuple
from models.schema import Database
from models.statistics import Statistics
from .filter_operator import FilterOperator, FilterResult
from .join_operator import NestedLoopJoinOperator, JoinResult
from .aggregate_operator import AggregateOperator, AggregateResult


class QueryExecutor:
    """
    Executes predefined queries on different database designs
    """

    def __init__(self, database: Database, statistics: Statistics):
        """
        Initialize query executor

        Args:
            database: Database to query
            statistics: Database statistics
        """
        self.database = database
        self.statistics = statistics
        self.filter_op = FilterOperator(statistics)
        self.join_op = NestedLoopJoinOperator(statistics)
        self.aggregate_op = AggregateOperator(statistics)

    def execute_q1(
        self,
        sharding_strategy: Dict[str, str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> FilterResult:
        """
        Q1: The stock of a given ID product in a given warehouse
        SELECT S.quantity, S.location
        FROM Stock S
        WHERE S.IDP = $IDP AND S.IDW = $IDW

        Args:
            sharding_strategy: Dict mapping collection names to sharding keys
            array_sizes: Average array sizes

        Returns:
            FilterResult
        """
        stock_collection = self.database.get_collection("Stock")
        if not stock_collection:
            raise ValueError("Stock collection not found")

        sharding_key = sharding_strategy.get("Stock")
        output_keys = ["quantity", "location"]
        filter_keys = ["IDP","IDW"]

        selectivity = 1 / stock_collection.document_count

        return self.filter_op.filter(
            collection=stock_collection,
            filter_keys=filter_keys,
            output_keys=output_keys,
            sharding_key=sharding_key,
            selectivity=selectivity,
            use_index=True,
            array_sizes=array_sizes
        )

    def execute_q2(
        self,
        brand: str,
        sharding_strategy: Dict[str, str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> FilterResult:
        """
        Q2: Names and prices of product from a given brand (e.g., "Apple")
        SELECT P.name, P.price
        FROM Product P
        WHERE P.brand = $brand

        Args:
            brand: Brand name (e.g., "Apple")
            sharding_strategy: Dict mapping collection names to sharding keys
            array_sizes: Average array sizes

        Returns:
            FilterResult
        """
        product_collection = self.database.get_collection("Product")
        if not product_collection:
            raise ValueError("Product collection not found")

        sharding_key = sharding_strategy.get("Product")
        output_keys = ["name", "price"]

        # For Apple brand: 50 products out of 100,000
        if brand.lower() == "apple":
            selectivity = self.statistics.products_per_brand_apple / self.statistics.num_products
        else:
            # Average selectivity for a brand
            selectivity = 1 / self.statistics.num_brands

        return self.filter_op.filter(
            collection=product_collection,
            filter_keys=["brand"],
            output_keys=output_keys,
            sharding_key=sharding_key,
            selectivity=selectivity,
            use_index=True,
            array_sizes=array_sizes
        )

    def execute_q3(
        self,
        sharding_strategy: Dict[str, str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> FilterResult:
        """
        Q3: Product ID and quantity from order lines ordered at a given date
        SELECT O.IDP, O.quantity
        FROM OrderLine O
        WHERE O.date = $date

        Args:
            sharding_strategy: Dict mapping collection names to sharding keys
            array_sizes: Average array sizes

        Returns:
            FilterResult
        """
        orderline_collection = self.database.get_collection("OrderLine")
        if not orderline_collection:
            raise ValueError("OrderLine collection not found")

        sharding_key = sharding_strategy.get("OrderLine")
        output_keys = ["IDP", "quantity"]

        # Orders balanced over 365 dates
        selectivity = 1 / self.statistics.num_dates

        return self.filter_op.filter(
            collection=orderline_collection,
            filter_keys=["date"],
            output_keys=output_keys,
            sharding_key=sharding_key,
            selectivity=selectivity,
            use_index=False,
            array_sizes=array_sizes
        )

    def execute_q4(
        self,
        sharding_strategy: Dict[str, str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> JoinResult:
        """
        Q4: Stock (list of product names, as well as their quantity) from a given warehouse
        SELECT P.name, S.quantity
        FROM Stock S JOIN Product P ON S.IDP = P.IDP
        WHERE S.IDW = $IDW

        Args:
            sharding_strategy: Dict mapping collection names to sharding keys
            array_sizes: Average array sizes

        Returns:
            JoinResult
        """

        stock_collection = self.database.get_collection("Stock")
        product_collection = self.database.get_collection("Product")

        if not stock_collection or not product_collection:
            raise ValueError("Required collections not found")

        stock_sharding = sharding_strategy.get("Stock")
        product_sharding = sharding_strategy.get("Product")

        left_output_keys = ["quantity"]
        right_output_keys = ["name"]

        left_filter_selectivity = 1 / self.statistics.num_warehouses
        right_filter_selectivity = 1 / product_collection.document_count

        return self.join_op.nested_loop_join(
            left_collection=stock_collection,
            right_collection=product_collection,
            join_key="IDP",
            left_output_keys=left_output_keys,
            right_output_keys =right_output_keys,
            left_sharding_key=stock_sharding,
            right_sharding_key=product_sharding,
            left_filter_keys=["IDW"],
            left_filter_selectivity=left_filter_selectivity,
            right_filter_selectivity=right_filter_selectivity,
            array_sizes=array_sizes
        )

    def execute_q5(
        self,
        brand: str,
        sharding_strategy: Dict[str, str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> JoinResult:
        """
        Q5: Distribution of "Apple" brand products (name & price) in warehouses (IDW & quantity)
        SELECT P.name, P.price, S.IDW, S.quantity
        FROM Product P JOIN Stock S ON P.IDP = S.IDP
        WHERE P.brand = "Apple"

        Args:
            brand: Brand name (e.g., "Apple")
            sharding_strategy: Dict mapping collection names to sharding keys
            array_sizes: Average array sizes

        Returns:
            JoinResult
        """

        # SENS COMME LE TD ET LA CORRECTION (INVERSE DANS LA CORRECTION (le P et le S) MAIS BON CALCUL)

        product_collection = self.database.get_collection("Product")
        stock_collection = self.database.get_collection("Stock")

        if not product_collection or not stock_collection:
            raise ValueError("Required collections not found")

        product_sharding = sharding_strategy.get("Product")
        stock_sharding = sharding_strategy.get("Stock")

        left_output_keys = ["name", "price"]
        right_output_keys = ["IDW", "quantity"]

        # Filter selectivity for Apple brand
        if brand.lower() == "apple":
            left_filter_selectivity = self.statistics.products_per_brand_apple / self.statistics.num_products
        else:
            left_filter_selectivity = 1 / self.statistics.num_brands

        right_filter_selectivity = 1/self.statistics.num_products

        return self.join_op.nested_loop_join(
            left_collection=product_collection,
            right_collection=stock_collection,
            join_key="IDP",
            left_output_keys=left_output_keys,
            right_output_keys =right_output_keys,
            left_sharding_key=product_sharding,
            right_sharding_key=stock_sharding,
            left_filter_keys=["brand"],
            left_filter_selectivity=left_filter_selectivity,
            right_filter_selectivity=right_filter_selectivity,
            array_sizes=array_sizes
        )

    def execute_q6(
        self,
        sharding_strategy: Dict[str, str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> Tuple[AggregateResult, JoinResult]:
        """
        Q6: The 100 most ordered product names and price (sum of quantities)
        SELECT P.name, P.price, OL.NB
        FROM Product P JOIN (
            SELECT O.IDP, SUM(O.quantity) AS NB
            FROM OrderLine O
            GROUP BY O.IDP
        ) OL ON P.IDP = OL.IDP
        ORDER BY OL.NB DESC
        LIMIT 100;
        
        
        Args:
            sharding_strategy: Dict mapping collection names to sharding keys
            array_sizes: Average array sizes
            
        Returns:
            AggregateResult
        """
        
        orderline_collection = self.database.get_collection("OrderLine")
        product_collection = self.database.get_collection("Product")

        if not orderline_collection or not product_collection:
            raise ValueError("Required collections not found")
        
        orderline_sharding = sharding_strategy.get("OrderLine")
        product_sharding = sharding_strategy.get("Product") 

        left_output_keys = ["name","price"]
        # We put all keys
        right_output_keys = ["quantity","IDP","quantity"]
        right_group_by_key = "IDP"
        

        left_filter_selectivity = 1 / self.statistics.num_products
        right_filter_selectivity = self.statistics.num_products / self.statistics.num_order_lines


        limit = 100
        
        # GROUP BY IDP, SUM(quantity)
        return self.aggregate_op.aggregator(
            left_collection=product_collection,
            right_collection=orderline_collection,
            join_key="IDP",
            limit=limit,
            left_output_keys=left_output_keys,
            right_output_keys =right_output_keys,
            left_sharding_key=product_sharding,
            right_sharding_key=orderline_sharding,
            right_group_by_key=right_group_by_key,
            left_filter_selectivity=left_filter_selectivity,
            right_filter_selectivity=right_filter_selectivity,
            array_sizes=array_sizes
        )
        

    def execute_q7(
        self,
        sharding_strategy: Dict[str, str],
        array_sizes: Optional[Dict[str, int]] = None
    ) -> Tuple[AggregateResult, JoinResult]:
        """
        Q7: Name and price of the product most ordered by customer no. 125;
        SELECT P.name, P.price, OL.NB
        FROM Product P JOIN (
            SELECT O.IDP, SUM(O.quantity) AS NB
            FROM OrderLine O
            WHERE O.IDC = $clientId
            GROUP BY O.IDP
        ) OL ON P.IDP = OL.IDP
        ORDER BY OL.NB DESC
        LIMIT 1;
        
        Args:
            sharding_strategy: Dict mapping collection names to sharding keys
            array_sizes: Average array sizes
            
        Returns:
            AggregateResult
        """
        
        
        product_collection = self.database.get_collection("Product")
        orderline_collection = self.database.get_collection("OrderLine")

        if not orderline_collection or not product_collection:
            raise ValueError("Required collections not found")
        
        product_sharding = sharding_strategy.get("Product")
        orderline_sharding = sharding_strategy.get("OrderLine")


        left_output_keys = ["name","price"]
        right_output_keys = ["quantity"]
        right_group_by_key = "IDP"
        right_filter_keys = ["IDC"]

        left_filter_selectivity = 1 / self.statistics.num_products
        right_filter_selectivity = self.statistics.products_per_customer / self.statistics.num_order_lines

        limit = 1

        
        
        
        return self.aggregate_op.aggregator(
            left_collection=product_collection,
            right_collection=orderline_collection,
            join_key="IDP",
            limit=limit,
            left_output_keys=left_output_keys,
            right_output_keys =right_output_keys,
            left_sharding_key=product_sharding,
            right_sharding_key=orderline_sharding,
            right_filter_keys=right_filter_keys,
            right_group_by_key=right_group_by_key,
            left_filter_selectivity=left_filter_selectivity,
            right_filter_selectivity=right_filter_selectivity,
            array_sizes=array_sizes
        )


