"""
Logging configuration for Core Analytics application.
"""
import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path

class LoggerSetup:
    """Setup structured logging for the application."""
    
    @staticmethod
    def setup_logger(name: str = "CoreAnalytics", level: str = "INFO") -> logging.Logger:
        """Setup and return a configured logger."""
        logger = logging.getLogger(name)
        
        if logger.handlers:
            return logger
        
        logger.setLevel(getattr(logging, level.upper()))
        
        # Create console JSON formatter (simplified for monitoring)
        console_formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )
        
        # Create file JSON formatter (detailed for debugging)
        file_formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s", "module": "%(module)s", "function": "%(funcName)s", "line": %(lineno)d}',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        file_handler = logging.FileHandler(
            log_dir / f"core_analytics_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        return logger

class CoreAnalyticsException(Exception):
    """Base exception for Core Analytics application."""
    pass

class ConfigurationError(CoreAnalyticsException):
    """Exception raised for configuration-related errors."""
    pass

class DataFetchError(CoreAnalyticsException):
    """Exception raised for data fetching errors."""
    pass

class ReportGenerationError(CoreAnalyticsException):
    """Exception raised for report generation errors."""
    pass

class ValidationError(CoreAnalyticsException):
    """Exception raised for data validation errors."""
    pass