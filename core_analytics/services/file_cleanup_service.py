"""
File Cleanup Service for managing file retention policies.
"""
import logging
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict
from pathlib import Path

from core_analytics.model.repositories.file_repository import FileRepository
from core_analytics.config.settings import ConfigurationService

class FileCleanupService:
    """Service for applying file cleanup business rules."""
    
    def __init__(self, config_service: ConfigurationService, file_repository: FileRepository = None):
        self.config_service = config_service
        self.file_repository = file_repository or FileRepository()
        self.logger = logging.getLogger("CoreAnalytics")
    
    def cleanup_old_output_directories(self, days_threshold: int = 30) -> List[str]:
        """
        Delete output directories older than specified days.
        Business rule: Keep only recent reports within the threshold.
        """
        self.logger.info(f"Starting cleanup of directories older than {days_threshold} days")
        
        app_settings = self.config_service.get_app_settings()
        output_base_dir = app_settings.output_base_dir
        
        if not self.file_repository.directory_exists(output_base_dir):
            self.logger.warning(f"Output directory does not exist: {output_base_dir}")
            return []
        
        # Calculate cutoff date (business logic)
        cutoff_date = datetime.now() - timedelta(days=days_threshold)
        self.logger.info(f"Cutoff date for cleanup: {cutoff_date.strftime('%Y-%m-%d')}")
        
        deleted_folders = []
        candidate_directories = self._get_date_based_directories(output_base_dir)
        
        for directory_info in candidate_directories:
            directory_path = directory_info['path']
            directory_date = directory_info['date']
            
            # Business rule: Delete if older than threshold
            if directory_date < cutoff_date:
                directory_size = self.file_repository.get_directory_size(directory_path)
                
                if self.file_repository.delete_directory(directory_path):
                    deleted_folders.append(directory_path)
                    self.logger.info(f"Deleted old directory: {directory_path} "
                                   f"(created: {directory_date.strftime('%Y-%m-%d')}, "
                                   f"size: {self._format_size(directory_size)})")
                else:
                    self.logger.error(f"Failed to delete directory: {directory_path}")
            else:
                self.logger.debug(f"Keeping recent directory: {directory_path} "
                                f"(created: {directory_date.strftime('%Y-%m-%d')})")
        
        self.logger.info(f"Cleanup completed. Deleted {len(deleted_folders)} directories")
        return deleted_folders
    
    def _get_date_based_directories(self, output_dir: str) -> List[Dict]:
        """
        Get directories that follow date naming pattern (YYYYMMDD).
        Business logic: Only consider directories with date-based names.
        """
        directories = []
        date_pattern = re.compile(r'^\d{8}$')  # YYYYMMDD pattern
        
        for directory_path in self.file_repository.list_directories(output_dir):
            directory_name = Path(directory_path).name
            
            # Business rule: Only process date-named directories
            if date_pattern.match(directory_name):
                try:
                    # Parse date from directory name (business logic)
                    directory_date = datetime.strptime(directory_name, '%Y%m%d')
                    directories.append({
                        'path': directory_path,
                        'name': directory_name,
                        'date': directory_date
                    })
                except ValueError:
                    self.logger.warning(f"Invalid date format in directory name: {directory_name}")
                    continue
            else:
                self.logger.debug(f"Skipping non-date directory: {directory_name}")
        
        return directories
    
    #TODO: this is method made for testing, remove it if not needed
    def get_cleanup_report(self, days_threshold: int = 30) -> Dict:
        """
        Generate a report of what would be cleaned up without actually deleting.
        Business logic: Provide visibility into cleanup decisions.
        """
        self.logger.info(f"Generating cleanup report for directories older than {days_threshold} days")
        
        app_settings = self.config_service.get_app_settings()
        output_base_dir = app_settings.output_base_dir
        
        if not self.file_repository.directory_exists(output_base_dir):
            return {"error": f"Output directory does not exist: {output_base_dir}"}
        
        cutoff_date = datetime.now() - timedelta(days=days_threshold)
        candidate_directories = self._get_date_based_directories(output_base_dir)
        
        to_delete = []
        to_keep = []
        total_size_to_delete = 0
        
        for directory_info in candidate_directories:
            directory_path = directory_info['path']
            directory_date = directory_info['date']
            directory_size = self.file_repository.get_directory_size(directory_path)
            
            directory_summary = {
                'path': directory_path,
                'name': directory_info['name'],
                'date': directory_date.strftime('%Y-%m-%d'),
                'size_bytes': directory_size,
                'size_formatted': self._format_size(directory_size)
            }
            
            if directory_date < cutoff_date:
                to_delete.append(directory_summary)
                total_size_to_delete += directory_size
            else:
                to_keep.append(directory_summary)
        
        return {
            'cutoff_date': cutoff_date.strftime('%Y-%m-%d'),
            'days_threshold': days_threshold,
            'directories_to_delete': to_delete,
            'directories_to_keep': to_keep,
            'total_directories_to_delete': len(to_delete),
            'total_directories_to_keep': len(to_keep),
            'total_size_to_free': self._format_size(total_size_to_delete),
            'total_size_to_free_bytes': total_size_to_delete
        }
    
    def cleanup_empty_directories(self, base_dir: str = None) -> List[str]:
        """
        Remove empty directories in the output folder.
        Business rule: Clean up failed or incomplete report generations.
        """
        if base_dir is None:
            app_settings = self.config_service.get_app_settings()
            base_dir = app_settings.output_base_dir
        
        self.logger.info(f"Cleaning up empty directories in: {base_dir}")
        
        if not self.file_repository.directory_exists(base_dir):
            self.logger.warning(f"Directory does not exist: {base_dir}")
            return []
        
        deleted_directories = []
        
        for directory_path in self.file_repository.list_directories(base_dir):
            try:
                # Business rule: Delete if directory is empty
                if not os.listdir(directory_path):  # Empty directory
                    if self.file_repository.delete_directory(directory_path):
                        deleted_directories.append(directory_path)
                        self.logger.info(f"Deleted empty directory: {directory_path}")
            except Exception as e:
                self.logger.error(f"Error checking directory {directory_path}: {e}")
        
        self.logger.info(f"Removed {len(deleted_directories)} empty directories")
        return deleted_directories
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        import math
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        
        return f"{s} {size_names[i]}"
