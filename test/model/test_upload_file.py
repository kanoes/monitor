"""
Test uploading Excel files to Azure Blob Storage.
"""
import unittest
import os
import tempfile
from datetime import datetime
import pandas as pd

from model.repositories.azure_blob_repository import AzureBlobRepository
from config.settings import ConfigurationService

class TestUploadExcelFile(unittest.TestCase):
    """Test uploading Excel files to blob storage."""

    def setUp(self):
        """Test setup."""
        self.config_service = ConfigurationService()
        self.repository = AzureBlobRepository(self.config_service)

    def create_test_excel_file(self) -> str:
        """Create a test Excel file and return its path."""
        # Create sample data
        data = {
            'Name': ['Alice', 'Bob', 'Charlie'],
            'Age': [25, 30, 35],
            'Department': ['Engineering', 'Sales', 'Marketing']
        }
        df = pd.DataFrame(data)
        
        # Create temporary Excel file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
            temp_path = temp_file.name
            
        # Write Excel file
        with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Test Data', index=False)
            
        return temp_path

    @unittest.skipUnless(
        os.environ.get("AZURE_BLOB_CONNECTION_STRING"), 
        "AZURE_BLOB_CONNECTION_STRING not set"
    )
    def test_upload_excel_file_to_blob(self):
        """Test uploading an actual Excel file to Azure Blob Storage."""
        # Create test Excel file
        excel_file_path = self.create_test_excel_file()
        
        try:
            # Generate unique remote path with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            remote_path = f"test/excel_upload_test_{timestamp}.xlsx"
            
            # Upload file
            result_url = self.repository.upload_file(excel_file_path, remote_path)
            
            # Verify upload was successful
            self.assertIsInstance(result_url, str)
            self.assertTrue(result_url.startswith("https://"))
            self.assertIn("core-analytics", result_url)  # container name
            self.assertIn(remote_path, result_url)
            
            print(f"Successfully uploaded Excel file to: {result_url}")
            
        finally:
            # Clean up local test file
            if os.path.exists(excel_file_path):
                os.unlink(excel_file_path)

    @unittest.skipUnless(
        os.environ.get("AZURE_BLOB_CONNECTION_STRING"), 
        "AZURE_BLOB_CONNECTION_STRING not set"
    )
    def test_upload_and_download_excel_file(self):
        """Test uploading and then downloading an Excel file."""
        # Create test Excel file
        original_file_path = self.create_test_excel_file()
        
        try:
            # Generate unique remote path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            remote_path = f"test/roundtrip_test_{timestamp}.xlsx"
            
            # Upload file
            result_url = self.repository.upload_file(original_file_path, remote_path)
            self.assertIsInstance(result_url, str)
            
            # Download the file to a different location
            download_path = original_file_path.replace('.xlsx', '_downloaded.xlsx')
            download_result = self.repository.download_file(remote_path, download_path)
            self.assertTrue(download_result)
            
            # Verify both files exist
            self.assertTrue(os.path.exists(original_file_path))
            self.assertTrue(os.path.exists(download_path))
            
            # Compare file sizes (basic verification)
            original_size = os.path.getsize(original_file_path)
            downloaded_size = os.path.getsize(download_path)
            self.assertEqual(original_size, downloaded_size)
            
            print(f"Successfully uploaded and downloaded Excel file")
            print(f"Original size: {original_size} bytes")
            print(f"Downloaded size: {downloaded_size} bytes")
            
        finally:
            # Clean up local files
            for path in [original_file_path, download_path]:
                if os.path.exists(path):
                    os.unlink(path)


def main():
    """Run the tests."""
    if not os.environ.get("AZURE_BLOB_CONNECTION_STRING"):
        print("Skipping integration tests - AZURE_BLOB_CONNECTION_STRING not set")
        print("To run these tests, set the environment variable:")
        print("export AZURE_BLOB_CONNECTION_STRING='your_connection_string'")
        return
    
    print("Running Excel file upload tests...")
    unittest.main(verbosity=2)


if __name__ == "__main__":
    main()