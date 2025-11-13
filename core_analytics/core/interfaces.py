"""
Abstract interfaces for Core Analytics application.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime
from azure.monitor.query import LogsQueryResult

class ILogRepository(ABC):
    """Interface for log data repository."""
    
    @abstractmethod
    def fetch_logs(self, query_configs: Dict[str, Any], start_time: datetime, end_time: datetime) -> Dict[str, LogsQueryResult]:
        """Fetch logs based on query configurations."""
        pass
    
    @abstractmethod
    def validate_log_data(self, log_results: Dict[str, LogsQueryResult]) -> bool:
        """Validate fetched log data."""
        pass

class IReportGenerator(ABC):
    """Interface for report generation."""
    
    @abstractmethod
    def generate_report(self, data: LogsQueryResult, filepath: str, report_type: str) -> None:
        """Generate a report from log data."""
        pass

class IStorageService(ABC):
      """Interface for storage operations."""

      @abstractmethod
      def upload_file(self, local_file_path: str, remote_path: str) -> str:
          """Upload a file to remote storage. Returns remote URL/path."""
          pass

      @abstractmethod
      def download_file(self, remote_path: str, local_file_path: str) -> bool:
          """Download a file from remote storage."""
          pass
      
class IQueryStrategy(ABC):
    """Interface for query processing strategies."""
    
    @abstractmethod
    def can_handle(self, query_key: str) -> bool:
        """Check if this strategy can handle the given query key."""
        pass
    
    @abstractmethod
    def process(self, query_key: str, data: LogsQueryResult) -> Dict[str, Any]:
        """Process the query data."""
        pass

class IAnalyticsService(ABC):
    """Interface for analytics business logic."""
    
    @abstractmethod
    def process_analytics_data(self, log_results: Dict[str, LogsQueryResult]) -> Dict[str, Any]:
        """Process analytics data and categorize results."""
        pass
    
    @abstractmethod
    def generate_reports(self, processed_data: Dict[str, Any], output_dir: str, end_time: datetime) -> List[str]:
        """Generate all required reports."""
        pass