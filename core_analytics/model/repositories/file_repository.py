"""
File Repository implementation for file operations.
"""
import os
import shutil
import logging
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from core_analytics.core.logging_config import DataFetchError

class FileRepository:
    """Repository for file system operations."""
    
    def __init__(self):
        self.logger = logging.getLogger("CoreAnalytics")
    
    def delete_directory(self, directory_path: str) -> bool:
        """Delete a directory and all its contents."""
        try:
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
                self.logger.info(f"Successfully deleted directory: {directory_path}")
                return True
            else:
                self.logger.warning(f"Directory does not exist: {directory_path}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to delete directory {directory_path}: {e}")
            return False
    
    def delete_file(self, file_path: str) -> bool:
        """Delete a single file."""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                self.logger.info(f"Successfully deleted file: {file_path}")
                return True
            else:
                self.logger.warning(f"File does not exist: {file_path}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to delete file {file_path}: {e}")
            return False
    
    def get_directory_creation_date(self, directory_path: str) -> Optional[datetime]:
        """Get the creation date of a directory."""
        try:
            if os.path.exists(directory_path):
                timestamp = os.path.getctime(directory_path)
                return datetime.fromtimestamp(timestamp)
            else:
                self.logger.warning(f"Directory does not exist: {directory_path}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to get creation date for {directory_path}: {e}")
            return None
    
    def get_directory_modification_date(self, directory_path: str) -> Optional[datetime]:
        """Get the last modification date of a directory."""
        try:
            if os.path.exists(directory_path):
                timestamp = os.path.getmtime(directory_path)
                return datetime.fromtimestamp(timestamp)
            else:
                self.logger.warning(f"Directory does not exist: {directory_path}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to get modification date for {directory_path}: {e}")
            return None
    
    def list_directories(self, parent_path: str) -> List[str]:
        """List all directories in the given parent path."""
        try:
            if not os.path.exists(parent_path):
                self.logger.warning(f"Parent directory does not exist: {parent_path}")
                return []
            
            directories = []
            for item in os.listdir(parent_path):
                item_path = os.path.join(parent_path, item)
                if os.path.isdir(item_path):
                    directories.append(item_path)
            
            self.logger.debug(f"Found {len(directories)} directories in {parent_path}")
            return directories
        except Exception as e:
            self.logger.error(f"Failed to list directories in {parent_path}: {e}")
            return []
    
    def get_directory_size(self, directory_path: str) -> int:
        """Get the total size of a directory in bytes."""
        try:
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(directory_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total_size += os.path.getsize(filepath)
            return total_size
        except Exception as e:
            self.logger.error(f"Failed to get size for directory {directory_path}: {e}")
            return 0
    
    def directory_exists(self, directory_path: str) -> bool:
        """Check if a directory exists."""
        return os.path.exists(directory_path) and os.path.isdir(directory_path)
    
    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists."""
        return os.path.exists(file_path) and os.path.isfile(file_path)
    
    def create_directory(self, directory_path: str) -> bool:
        """Create a directory (including parent directories if needed)."""
        try:
            os.makedirs(directory_path, exist_ok=True)
            self.logger.info(f"Successfully created directory: {directory_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create directory {directory_path}: {e}")
            return False
