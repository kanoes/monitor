"""
Strategy for processing user count queries.
"""
from typing import Dict, Any
from azure.monitor.query import LogsQueryResult

from .base_strategy import BaseQueryStrategy

class UserCountStrategy(BaseQueryStrategy):
    """Strategy for processing user count queries."""
    
    def __init__(self):
        super().__init__()
        self.query_type = "user_count"
    
    def can_handle(self, query_key: str) -> bool:
        """Check if this strategy can handle user count queries."""
        return "user_count" in query_key
    
    def process(self, query_key: str, data: LogsQueryResult) -> Dict[str, Any]:
        """Process user count query data."""
        row_count = self._get_row_count(data)
        
        return {
            "query_key": query_key,
            "type": "user_count",
            "data": data,
            "metadata": {
                "row_count": row_count,
                "user_count": row_count  # For user count queries, row count equals user count
            }
        }