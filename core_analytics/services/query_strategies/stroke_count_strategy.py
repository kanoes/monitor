"""
Strategy for processing stroke count queries.
"""
from typing import Dict, Any
from azure.monitor.query import LogsQueryResult

from .base_strategy import BaseQueryStrategy

class StrokeCountStrategy(BaseQueryStrategy):
    """Strategy for processing stroke count queries."""
    
    def __init__(self):
        super().__init__()
        self.query_type = "stroke_count"
    
    def can_handle(self, query_key: str) -> bool:
        """Check if this strategy can handle stroke count queries."""
        return "stroke_count" in query_key
    
    def process(self, query_key: str, data: LogsQueryResult) -> Dict[str, Any]:
        """Process stroke count query data."""
        row_count = self._get_row_count(data)
        
        return {
            "query_key": query_key,
            "type": "stroke_count",
            "data": data,
            "metadata": {
                "row_count": row_count,
                "stroke_count": row_count  # For stroke count queries, row count equals stroke count
            }
        }