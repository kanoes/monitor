"""
Azure Log Repository implementation.
"""
from typing import Dict, Any
from datetime import datetime
import logging

from azure.monitor.query import LogsQueryResult, LogsQueryClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
import os

from core_analytics.core.interfaces import ILogRepository
from core_analytics.core.logging_config import DataFetchError, ValidationError
from core_analytics.config.settings import ConfigurationService
from core_analytics.model.kql_builder import build_kql
import traceback

class AzureLogRepository(ILogRepository):
    """Repository for fetching logs from Azure Monitor."""
    
    def __init__(self, config_service: ConfigurationService):
        self.config_service = config_service
        self.logger = logging.getLogger("CoreAnalytics")
        self._logs_query_client = None
    
    @property
    def logs_query_client(self) -> LogsQueryClient:
        """Lazy initialization of Azure Logs Query Client."""
        if self._logs_query_client is None:
            try:
                credential = DefaultAzureCredential()

                self._logs_query_client = LogsQueryClient(credential)
                self.logger.info("Azure Logs Query Client initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize Azure Logs Query Client: {e}")
                raise DataFetchError(f"Failed to initialize Azure client: {e}")
        
        return self._logs_query_client
    
    def fetch_logs(self, query_configs: Dict[str, Any], start_time: datetime, end_time: datetime) -> Dict[str, LogsQueryResult]:
        """Fetch logs from Azure Monitor based on query configurations."""
        results = {}
        
        self.logger.info(f"Starting log fetch for {len(query_configs)} queries")
        self.logger.info(f"Time range: {start_time} to {end_time}")
        
        for query_key, query_config in query_configs.items():
            try:
                self.logger.debug(f"Processing query: {query_key}")
                
                # Build KQL query
                query = build_kql(
                    query_type=query_config.query_type,
                    contains_keyword=query_config.contains_keyword,
                    startswith_keyword=query_config.startswith_keyword
                )
                
                # Get workspace ID
                workspace_id = self.config_service.get_workspace_id(query_config.workspace)
                
                self.logger.debug(f"Executing query for workspace {query_config.workspace}: {workspace_id}")
                
                # Execute query
                result = self.logs_query_client.query_workspace(
                    workspace_id=workspace_id,
                    query=query,
                    timespan=(start_time, end_time)
                )
                
                results[query_key] = result
                
                if result and result.tables:
                    row_count = len(result.tables[0].rows) if result.tables[0].rows else 0
                    self.logger.info(f"Query {query_key} returned {row_count} rows")
                else:
                    self.logger.warning(f"Query {query_key} returned no data")
                
            except Exception as e:
                self.logger.error(f"Failed to execute query {query_key}: {e}")
                self.logger.error(f"Trace: {traceback.format_exc()}")
                raise DataFetchError(f"Failed to fetch logs for {query_key}: {e}")
        
        self.logger.info(f"Successfully fetched logs for {len(results)} queries")
        return results
    
    def validate_log_data(self, log_results: Dict[str, LogsQueryResult]) -> bool:
        """Validate fetched log data."""
        self.logger.info(f"Validating log data for {len(log_results)} results")
        
        for key, result in log_results.items():
            try:
                if not result:
                    self.logger.error(f"No data returned for query: {key}")
                    raise ValidationError(f"No data returned for query: {key}")
                
                if not hasattr(result, 'tables'):
                    self.logger.error(f"Invalid result structure for query: {key}")
                    raise ValidationError(f"Invalid result structure for query: {key}")
                
                if not result.tables:
                    self.logger.warning(f"No tables in result for query: {key}")
                    continue
                
                # Additional validation can be added here
                self.logger.debug(f"Validation passed for query: {key}")
                
            except Exception as e:
                self.logger.error(f"Validation failed for query {key}: {e}")
                return False
        
        self.logger.info("All log data validation passed")
        return True
