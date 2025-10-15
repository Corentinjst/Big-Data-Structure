"""
Calculate sharding distribution statistics
"""

from typing import Dict, Tuple
from models.collection import Collection
from models.statistics import Statistics

class ShardCalculator:
    """Calculate sharding distribution statistics"""
    
    def __init__(self, statistics: Statistics):
        self.stats = statistics
    
    def calculate_distribution(self, 
                              collection: Collection,
                              sharding_key: str,
                              distinct_values: int) -> Dict[str, float]:
        """
        Calculate sharding distribution metrics
        
        Args:
            collection: The collection to analyze
            sharding_key: The key used for sharding
            distinct_values: Number of distinct values for the sharding key
        
        Returns:
            Dictionary with distribution metrics
        """
        num_servers = self.stats.num_servers
        total_docs = collection.document_count
        
        # Average documents per server
        avg_docs_per_server = total_docs / num_servers
        
        # Average distinct values per server
        # Note: This assumes uniform distribution
        avg_distinct_per_server = distinct_values / num_servers
        
        # Check for under-utilization (fewer distinct values than servers)
        servers_with_data = min(distinct_values, num_servers)
        utilization = servers_with_data / num_servers
        
        return {
            'sharding_key': sharding_key,
            'total_documents': total_docs,
            'distinct_values': distinct_values,
            'num_servers': num_servers,
            'avg_docs_per_server': avg_docs_per_server,
            'avg_distinct_per_server': avg_distinct_per_server,
            'servers_with_data': servers_with_data,
            'server_utilization': utilization,
            'skew_warning': utilization < 0.5  # Flag if many servers unused
        }
    
    def compare_sharding_strategies(self, 
                                   collection: Collection,
                                   strategies: Dict[str, int]) -> Dict[str, Dict]:
        """
        Compare multiple sharding strategies
        
        Args:
            collection: The collection to analyze
            strategies: Dict mapping sharding_key -> distinct_values
        
        Returns:
            Dictionary of sharding strategies and their metrics
        """
        results = {}
        
        for shard_key, distinct_vals in strategies.items():
            results[shard_key] = self.calculate_distribution(
                collection,
                shard_key,
                distinct_vals
            )
        
        return results
    
    def recommend_sharding_key(self, 
                              collection: Collection,
                              strategies: Dict[str, int]) -> str:
        """
        Recommend the best sharding key based on distribution
        
        Args:
            collection: The collection to analyze
            strategies: Dict mapping sharding_key -> distinct_values
        
        Returns:
            Recommended sharding key
        """
        results = self.compare_sharding_strategies(collection, strategies)
        
        # Score each strategy
        # Good sharding has:
        # 1. High server utilization
        # 2. Reasonable docs per server
        # 3. Good distinct value distribution
        
        best_key = None
        best_score = -1
        
        for key, metrics in results.items():
            # Penalize low utilization
            utilization_score = metrics['server_utilization']
            
            # Prefer more distinct values per server (but not too many)
            distinct_score = min(metrics['avg_distinct_per_server'] / 10, 1.0)
            
            # Combined score
            score = utilization_score * 0.7 + distinct_score * 0.3
            
            if score > best_score:
                best_score = score
                best_key = key
        
        return best_key