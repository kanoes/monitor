"""
Base strategy for query processing.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any
from azure.monitor.query import LogsQueryResult

from core_analytics.core.interfaces import IQueryStrategy

class BaseQueryStrategy(IQueryStrategy):
    """Base class for query processing strategies."""
    
    def __init__(self):
        self.query_type = None
    
    @abstractmethod
    def can_handle(self, query_key: str) -> bool:
        """Check if this strategy can handle the given query key."""
        pass
    
    @abstractmethod
    def process(self, query_key: str, data: LogsQueryResult) -> Dict[str, Any]:
        """Process the query data."""
        pass
    
    def _get_row_count(self, data: LogsQueryResult) -> int:
        """Get the number of rows from LogsQueryResult."""
        if not data or not data.tables or not data.tables[0].rows:
            return 0
        return len(data.tables[0].rows)