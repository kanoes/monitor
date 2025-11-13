"""
Analytics service containing business logic for processing analytics data.
"""
from typing import Dict, Any, List
from datetime import datetime
import logging

from azure.monitor.query import LogsQueryResult
from core_analytics.core.models import ProcessData

from core_analytics.core.interfaces import IAnalyticsService, ILogRepository
from core_analytics.core.logging_config import CoreAnalyticsException
from core_analytics.config.settings import ConfigurationService
from core_analytics.services.query_strategies.strategy_factory import QueryStrategyFactory

class AnalyticsService(IAnalyticsService):
    """Service for processing analytics data and business logic."""
    
    def __init__(self, 
                 log_repository: ILogRepository, 
                 config_service: ConfigurationService,
                 strategy_factory: QueryStrategyFactory):
        self.log_repository = log_repository
        self.config_service = config_service
        self.strategy_factory = strategy_factory
        self.logger = logging.getLogger("CoreAnalytics")
    
    def fetch_and_process_data(self, start_time: datetime, end_time: datetime) -> ProcessData:
        """Fetch raw data and process it using strategies."""
        self.logger.info("Starting analytics data fetch and processing")
        
        try:
            # Get query configurations
            query_configs = self.config_service.get_enabled_query_configs()
            
            # Fetch raw log data
            self.logger.info(f"Fetching logs for {len(query_configs)} queries in selected group")
            log_results = self.log_repository.fetch_logs(query_configs, start_time, end_time)
            
            # Validate data
            if not self.log_repository.validate_log_data(log_results):
                raise CoreAnalyticsException("Log data validation failed")
            
            # Process data using strategies
            processed_data = self.process_analytics_data(log_results)
            
            self.logger.info("Analytics data fetch and processing completed successfully")
            return processed_data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch and process analytics data: {e}")
            raise
    
    def process_analytics_data(self, log_results: Dict[str, LogsQueryResult]) -> ProcessData:
        """Process analytics data and categorize results using strategies."""
        self.logger.info(f"Processing analytics data for {len(log_results)} queries")
        
        processed_data = ProcessData()
        
        for query_key, data in log_results.items():
            try:
                strategy = self.strategy_factory.get_strategy(query_key)
                
                if strategy:
                    result = strategy.process(query_key, data)
                    
                    if result["type"] == "user_count":
                        processed_data.user_count_results[query_key] = result
                    elif result["type"] == "stroke_count":
                        processed_data.stroke_count_results[query_key] = result
                    else:
                        processed_data.unknown_results[query_key] = result
                        
                    self.logger.debug(f"Processed {query_key} with {result['type']} strategy")
                else:
                    # Fallback for unknown query types
                    processed_data.unknown_results[query_key] = {
                        "query_key": query_key,
                        "type": "unknown",
                        "data": data,
                        "metadata": {"row_count": len(data.tables[0].rows) if data.tables else 0}
                    }
                    self.logger.warning(f"No strategy found for query: {query_key}")
                    
            except Exception as e:
                self.logger.error(f"Failed to process data for query {query_key}: {e}")
                # Continue processing other queries
                continue
        
        self.logger.info(
            f"Processing completed: "
            f"{len(processed_data.user_count_results)} user count, "
            f"{len(processed_data.stroke_count_results)} stroke count, "
            f"{len(processed_data.unknown_results)} unknown queries"
        )
        
        return processed_data
    
    #TODO: remove this method(unless we need to more reports with more complex logic)
    def generate_reports(self, processed_data: Dict[str, Any], output_dir: str, end_time: datetime) -> List[str]:
        """Generate all required reports from processed data."""
        self.logger.info(f"Generating reports to directory: {output_dir}")
        
        try:
            from core_analytics.view.factories.report_factory import ReportFactory
            report_factory = ReportFactory()
            
            generated_files = report_factory.generate_all_reports(
                processed_data, 
                output_dir, 
                end_time
            )
            
            self.logger.info(f"Successfully generated {len(generated_files)} reports")
            return generated_files
            
        except Exception as e:
            self.logger.error(f"Failed to generate reports: {e}")
            raise
