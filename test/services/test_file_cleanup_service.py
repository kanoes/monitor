"""
Unit tests for FileCleanupService.
"""
import unittest
import os
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import Mock, patch
from pathlib import Path

from services.file_cleanup_service import FileCleanupService
from model.repositories.file_repository import FileRepository
from config.settings import ConfigurationService

class TestFileCleanupService(unittest.TestCase):
    """Test cases for FileCleanupService."""

    def setUp(self):
        """Test setup."""
        # Mock dependencies
        self.mock_config_service = Mock(spec=ConfigurationService)
        self.mock_file_repository = Mock(spec=FileRepository)
        self.mock_app_settings = Mock()
        
        # Setup default mock behavior
        self.mock_app_settings.output_base_dir = "output"
        self.mock_config_service.get_app_settings.return_value = self.mock_app_settings
        self.mock_file_repository.directory_exists.return_value = True
        
        # Create service with mocked dependencies
        self.service = FileCleanupService(
            config_service=self.mock_config_service,
            file_repository=self.mock_file_repository
        )

    def test_cleanup_old_output_directories_no_base_dir(self):
        """Test cleanup when output base directory doesn't exist."""
        # Mock directory doesn't exist
        self.mock_file_repository.directory_exists.return_value = False
        
        # Run cleanup
        result = self.service.cleanup_old_output_directories(30)
        
        # Should return empty list
        self.assertEqual(result, [])
        self.mock_file_repository.directory_exists.assert_called_once_with("output")

    def test_cleanup_old_output_directories_success(self):
        """Test successful cleanup of old directories."""
        # Setup mock directory structure
        now = datetime.now(ZoneInfo("Asia/Tokyo"))
        old_date = now - timedelta(days=35)  # 35 days old (should be deleted)
        recent_date = now - timedelta(days=15)  # 15 days old (should be kept)
        
        old_dir_path = "output/20240101"
        recent_dir_path = "output/20240220"
        
        # Mock list_directories to return test directories
        self.mock_file_repository.list_directories.return_value = [old_dir_path, recent_dir_path]
        
        # Mock get_directory_size
        self.mock_file_repository.get_directory_size.return_value = 1024
        
        # Mock successful deletion
        self.mock_file_repository.delete_directory.return_value = True
        
        # Mock the _get_date_based_directories method behavior
        with patch.object(self.service, '_get_date_based_directories') as mock_get_dirs:
            mock_get_dirs.return_value = [
                {'path': old_dir_path, 'name': '20240101', 'date': old_date},
                {'path': recent_dir_path, 'name': '20240220', 'date': recent_date}
            ]
            
            # Run cleanup with 30-day threshold
            result = self.service.cleanup_old_output_directories(30)
        
        # Should delete only the old directory
        self.assertEqual(len(result), 1)
        self.assertIn(old_dir_path, result)
        self.mock_file_repository.delete_directory.assert_called_once_with(old_dir_path)

    def test_get_date_based_directories(self):
        """Test getting directories with date-based names."""
        # Setup test directories
        test_dirs = [
            "output/20240101",  # Valid date format
            "output/20240215",  # Valid date format
            "output/stroke_count",  # Invalid - not date format
            "output/temp",  # Invalid - not date format
        ]
        
        self.mock_file_repository.list_directories.return_value = test_dirs
        
        # Call the method
        result = self.service._get_date_based_directories("output")
        
        # Should return only date-formatted directories
        self.assertEqual(len(result), 2)
        
        # Check the structure of returned data
        for dir_info in result:
            self.assertIn('path', dir_info)
            self.assertIn('name', dir_info)
            self.assertIn('date', dir_info)
            self.assertIsInstance(dir_info['date'], datetime)

    def test_get_date_based_directories_invalid_dates(self):
        """Test handling of directories with invalid date formats."""
        # Setup test directories with invalid dates
        test_dirs = [
            "output/20240229",  # Valid leap year date
            "output/20230229",  # Invalid - not a leap year
            "output/20241301",  # Invalid - month 13
            "output/abcd1234",  # Invalid - not numbers
        ]
        
        self.mock_file_repository.list_directories.return_value = test_dirs
        
        # Call the method
        result = self.service._get_date_based_directories("output")
        
        # Should handle invalid dates gracefully
        # Only 20240229 should be valid (2024 is a leap year)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], '20240229')

    def test_get_cleanup_report(self):
        """Test generating cleanup report without actually deleting."""
        # Setup mock data
        now = datetime.now(ZoneInfo("Asia/Tokyo"))
        old_date = now - timedelta(days=35)
        recent_date = now - timedelta(days=15)
        
        old_dir_path = "output/20240101"
        recent_dir_path = "output/20240220"
        
        # Mock directory sizes
        old_size = 2048
        recent_size = 1024
        
        def mock_get_size(path):
            if path == old_dir_path:
                return old_size
            elif path == recent_dir_path:
                return recent_size
            return 0
        
        self.mock_file_repository.get_directory_size.side_effect = mock_get_size
        
        # Mock the _get_date_based_directories method
        with patch.object(self.service, '_get_date_based_directories') as mock_get_dirs:
            mock_get_dirs.return_value = [
                {'path': old_dir_path, 'name': '20240101', 'date': old_date},
                {'path': recent_dir_path, 'name': '20240220', 'date': recent_date}
            ]
            
            # Generate report
            report = self.service.get_cleanup_report(30)
        
        # Verify report structure
        self.assertIn('cutoff_date', report)
        self.assertIn('days_threshold', report)
        self.assertIn('directories_to_delete', report)
        self.assertIn('directories_to_keep', report)
        self.assertIn('total_directories_to_delete', report)
        self.assertIn('total_directories_to_keep', report)
        self.assertIn('total_size_to_free', report)
        
        # Verify report content
        self.assertEqual(report['days_threshold'], 30)
        self.assertEqual(report['total_directories_to_delete'], 1)
        self.assertEqual(report['total_directories_to_keep'], 1)
        self.assertEqual(report['total_size_to_free_bytes'], old_size)

    def test_get_cleanup_report_no_base_dir(self):
        """Test cleanup report when base directory doesn't exist."""
        self.mock_file_repository.directory_exists.return_value = False
        
        # Generate report
        report = self.service.get_cleanup_report(30)
        
        # Should return error
        self.assertIn('error', report)

    def test_cleanup_empty_directories(self):
        """Test cleanup of empty directories."""
        # Setup test directories
        empty_dir1 = "output/empty1"
        empty_dir2 = "output/empty2"
        non_empty_dir = "output/non_empty"
        
        test_dirs = [empty_dir1, empty_dir2, non_empty_dir]
        self.mock_file_repository.list_directories.return_value = test_dirs
        
        # Mock successful deletion for empty directories
        self.mock_file_repository.delete_directory.return_value = True
        
        # Mock os.listdir to simulate empty/non-empty directories
        def mock_listdir(path):
            if path in [empty_dir1, empty_dir2]:
                return []  # Empty directories
            else:
                return ['file1.txt']  # Non-empty directory
        
        with patch('os.listdir', side_effect=mock_listdir):
            result = self.service.cleanup_empty_directories("output")
        
        # Should delete only empty directories
        self.assertEqual(len(result), 2)
        self.assertIn(empty_dir1, result)
        self.assertIn(empty_dir2, result)
        
        # Verify delete_directory was called for empty dirs
        expected_calls = [unittest.mock.call(empty_dir1), unittest.mock.call(empty_dir2)]
        self.mock_file_repository.delete_directory.assert_has_calls(expected_calls)

    def test_cleanup_empty_directories_default_base_dir(self):
        """Test cleanup of empty directories using default base directory."""
        # Don't specify base_dir, should use config default
        self.mock_file_repository.list_directories.return_value = []
        
        with patch('os.listdir', return_value=[]):
            result = self.service.cleanup_empty_directories()
        
        # Should use default output directory from config
        self.mock_file_repository.directory_exists.assert_called_with("output")

    def test_format_size(self):
        """Test file size formatting."""
        # Test various sizes
        test_cases = [
            (0, "0 B"),
            (512, "512.0 B"),
            (1024, "1.0 KB"),
            (1536, "1.5 KB"),
            (1048576, "1.0 MB"),
            (1073741824, "1.0 GB"),
        ]
        
        for size_bytes, expected in test_cases:
            result = self.service._format_size(size_bytes)
            self.assertEqual(result, expected)

    def test_cleanup_failure_handling(self):
        """Test handling of deletion failures."""
        # Setup mock directory
        old_date = datetime.now(ZoneInfo("Asia/Tokyo")) - timedelta(days=35)
        old_dir_path = "output/20240101"
        
        # Mock deletion failure
        self.mock_file_repository.delete_directory.return_value = False
        self.mock_file_repository.get_directory_size.return_value = 1024
        
        with patch.object(self.service, '_get_date_based_directories') as mock_get_dirs:
            mock_get_dirs.return_value = [
                {'path': old_dir_path, 'name': '20240101', 'date': old_date}
            ]
            
            result = self.service.cleanup_old_output_directories(30)
        
        # Should return empty list when deletion fails
        self.assertEqual(len(result), 0)
        self.mock_file_repository.delete_directory.assert_called_once_with(old_dir_path)


def main():
    """Run the tests."""
    unittest.main(verbosity=2)


if __name__ == "__main__":
    main()