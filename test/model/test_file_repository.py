"""
Unit tests for FileRepository.
"""
import unittest
import os
import tempfile
import shutil
from datetime import datetime
from pathlib import Path

from model.repositories.file_repository import FileRepository

class TestFileRepository(unittest.TestCase):
    """Test cases for FileRepository."""

    def setUp(self):
        """Test setup."""
        self.repository = FileRepository()
        # Create a temporary directory for testing
        self.test_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Test cleanup."""
        # Clean up the temporary directory
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_create_directory(self):
        """Test creating a directory."""
        new_dir = os.path.join(self.test_dir, "new_directory")
        
        # Directory should not exist initially
        self.assertFalse(self.repository.directory_exists(new_dir))
        
        # Create directory
        result = self.repository.create_directory(new_dir)
        
        # Verify creation was successful
        self.assertTrue(result)
        self.assertTrue(self.repository.directory_exists(new_dir))
        self.assertTrue(os.path.exists(new_dir))

    def test_delete_directory(self):
        """Test deleting a directory."""
        # Create test directory
        test_dir = os.path.join(self.test_dir, "to_delete")
        os.makedirs(test_dir)
        
        # Add a file to make it non-empty
        test_file = os.path.join(test_dir, "test_file.txt")
        with open(test_file, 'w') as f:
            f.write("test content")
        
        # Verify directory exists
        self.assertTrue(os.path.exists(test_dir))
        
        # Delete directory
        result = self.repository.delete_directory(test_dir)
        
        # Verify deletion was successful
        self.assertTrue(result)
        self.assertFalse(os.path.exists(test_dir))

    def test_delete_nonexistent_directory(self):
        """Test deleting a directory that doesn't exist."""
        nonexistent_dir = os.path.join(self.test_dir, "nonexistent")
        
        # Try to delete non-existent directory
        result = self.repository.delete_directory(nonexistent_dir)
        
        # Should return False but not crash
        self.assertFalse(result)

    def test_delete_file(self):
        """Test deleting a single file."""
        # Create test file
        test_file = os.path.join(self.test_dir, "test_file.txt")
        with open(test_file, 'w') as f:
            f.write("test content")
        
        # Verify file exists
        self.assertTrue(os.path.exists(test_file))
        
        # Delete file
        result = self.repository.delete_file(test_file)
        
        # Verify deletion was successful
        self.assertTrue(result)
        self.assertFalse(os.path.exists(test_file))

    def test_delete_nonexistent_file(self):
        """Test deleting a file that doesn't exist."""
        nonexistent_file = os.path.join(self.test_dir, "nonexistent.txt")
        
        # Try to delete non-existent file
        result = self.repository.delete_file(nonexistent_file)
        
        # Should return False but not crash
        self.assertFalse(result)

    def test_get_directory_creation_date(self):
        """Test getting directory creation date."""
        # Create test directory
        test_dir = os.path.join(self.test_dir, "date_test")
        os.makedirs(test_dir)
        
        # Get creation date
        creation_date = self.repository.get_directory_creation_date(test_dir)
        
        # Verify we got a datetime object
        self.assertIsInstance(creation_date, datetime)
        
        # Should be recent (within last minute)
        now = datetime.now()
        time_diff = (now - creation_date).total_seconds()
        self.assertLess(time_diff, 60)  # Less than 60 seconds ago

    def test_get_directory_creation_date_nonexistent(self):
        """Test getting creation date for non-existent directory."""
        nonexistent_dir = os.path.join(self.test_dir, "nonexistent")
        
        # Should return None for non-existent directory
        result = self.repository.get_directory_creation_date(nonexistent_dir)
        self.assertIsNone(result)

    def test_get_directory_modification_date(self):
        """Test getting directory modification date."""
        # Create test directory
        test_dir = os.path.join(self.test_dir, "mod_test")
        os.makedirs(test_dir)
        
        # Get modification date
        mod_date = self.repository.get_directory_modification_date(test_dir)
        
        # Verify we got a datetime object
        self.assertIsInstance(mod_date, datetime)

    def test_list_directories(self):
        """Test listing directories."""
        # Create test subdirectories
        sub_dirs = ["dir1", "dir2", "dir3"]
        for dir_name in sub_dirs:
            os.makedirs(os.path.join(self.test_dir, dir_name))
        
        # Create a file (should not be included in directory list)
        test_file = os.path.join(self.test_dir, "test_file.txt")
        with open(test_file, 'w') as f:
            f.write("test")
        
        # List directories
        directories = self.repository.list_directories(self.test_dir)
        
        # Verify we got the right number of directories
        self.assertEqual(len(directories), 3)
        
        # Verify all expected directories are present
        directory_names = [os.path.basename(d) for d in directories]
        for expected_dir in sub_dirs:
            self.assertIn(expected_dir, directory_names)

    def test_list_directories_empty(self):
        """Test listing directories in empty folder."""
        empty_dir = os.path.join(self.test_dir, "empty")
        os.makedirs(empty_dir)
        
        # List directories
        directories = self.repository.list_directories(empty_dir)
        
        # Should return empty list
        self.assertEqual(len(directories), 0)

    def test_list_directories_nonexistent(self):
        """Test listing directories in non-existent folder."""
        nonexistent_dir = os.path.join(self.test_dir, "nonexistent")
        
        # List directories
        directories = self.repository.list_directories(nonexistent_dir)
        
        # Should return empty list
        self.assertEqual(len(directories), 0)

    def test_get_directory_size(self):
        """Test getting directory size."""
        # Create test directory with files
        test_dir = os.path.join(self.test_dir, "size_test")
        os.makedirs(test_dir)
        
        # Create files with known sizes
        file1 = os.path.join(test_dir, "file1.txt")
        file2 = os.path.join(test_dir, "file2.txt")
        
        content1 = "Hello World"  # 11 bytes
        content2 = "Test Content"  # 12 bytes
        
        with open(file1, 'w') as f:
            f.write(content1)
        with open(file2, 'w') as f:
            f.write(content2)
        
        # Get directory size
        total_size = self.repository.get_directory_size(test_dir)
        
        # Should be at least the size of our content (may be slightly larger due to filesystem overhead)
        expected_min_size = len(content1) + len(content2)
        self.assertGreaterEqual(total_size, expected_min_size)

    def test_get_directory_size_empty(self):
        """Test getting size of empty directory."""
        empty_dir = os.path.join(self.test_dir, "empty")
        os.makedirs(empty_dir)
        
        # Get size
        size = self.repository.get_directory_size(empty_dir)
        
        # Should be 0
        self.assertEqual(size, 0)

    def test_directory_exists(self):
        """Test checking if directory exists."""
        # Create test directory
        test_dir = os.path.join(self.test_dir, "exists_test")
        os.makedirs(test_dir)
        
        # Test existing directory
        self.assertTrue(self.repository.directory_exists(test_dir))
        
        # Test non-existent directory
        nonexistent = os.path.join(self.test_dir, "nonexistent")
        self.assertFalse(self.repository.directory_exists(nonexistent))

    def test_file_exists(self):
        """Test checking if file exists."""
        # Create test file
        test_file = os.path.join(self.test_dir, "exists_test.txt")
        with open(test_file, 'w') as f:
            f.write("test")
        
        # Test existing file
        self.assertTrue(self.repository.file_exists(test_file))
        
        # Test non-existent file
        nonexistent = os.path.join(self.test_dir, "nonexistent.txt")
        self.assertFalse(self.repository.file_exists(nonexistent))

    def test_directory_vs_file_exists(self):
        """Test that directory_exists and file_exists distinguish correctly."""
        # Create directory
        test_dir = os.path.join(self.test_dir, "test_dir")
        os.makedirs(test_dir)
        
        # Create file
        test_file = os.path.join(self.test_dir, "test_file.txt")
        with open(test_file, 'w') as f:
            f.write("test")
        
        # Directory should only be recognized as directory
        self.assertTrue(self.repository.directory_exists(test_dir))
        self.assertFalse(self.repository.file_exists(test_dir))
        
        # File should only be recognized as file
        self.assertTrue(self.repository.file_exists(test_file))
        self.assertFalse(self.repository.directory_exists(test_file))


def main():
    """Run the tests."""
    unittest.main(verbosity=2)


if __name__ == "__main__":
    main()