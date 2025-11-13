"""
Factory for creating query processing strategies.
"""
from typing import List, Optional
import logging

from core_analytics.core.interfaces import IQueryStrategy
from .user_count_strategy import UserCountStrategy
from .stroke_count_strategy import StrokeCountStrategy

class QueryStrategyFactory:
    """Factory for creating and managing query processing strategies."""
    
    def __init__(self):
        self.logger = logging.getLogger("CoreAnalytics")
        self._strategies: List[IQueryStrategy] = [
            UserCountStrategy(),
            StrokeCountStrategy()
        ]
    
    def get_strategy(self, query_key: str) -> Optional[IQueryStrategy]:
        """Get the appropriate strategy for a given query key."""
        for strategy in self._strategies:
            if strategy.can_handle(query_key):
                self.logger.debug(f"Found strategy {strategy.__class__.__name__} for query: {query_key}")
                return strategy
        
        self.logger.warning(f"No strategy found for query: {query_key}")
        return None
    
    def register_strategy(self, strategy: IQueryStrategy) -> None:
        """Register a new strategy."""
        self._strategies.append(strategy)
        self.logger.info(f"Registered new strategy: {strategy.__class__.__name__}")
    
    def get_all_strategies(self) -> List[IQueryStrategy]:
        """Get all registered strategies."""
        return self._strategies.copy()
