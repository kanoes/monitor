"""
Unit tests for AzureBlobRepository.
"""
import unittest
import os
import tempfile
from unittest.mock import Mock, patch

from model.repositories.azure_blob_repository import AzureBlobRepository
from config.settings import ConfigurationService
from core.logging_config import DataFetchError

class TestAzureBlobRepository(unittest.TestCase):
    """Test cases for AzureBlobRepository."""

    def setUp(self):
        """Test setup."""
        self.config_service = Mock(spec=ConfigurationService)
        self.mock_app_settings = Mock()
        self.mock_app_settings.blob_container_name = "test-container"
        self.config_service.get_app_settings.return_value = self.mock_app_settings
        
        self.repository = AzureBlobRepository(self.config_service)

    @patch.dict(os.environ, {"AZURE_BLOB_CONNECTION_STRING": "test_connection_string"})
    @patch("model.repositories.azure_blob_repository.BlobServiceClient")
    def test_blob_service_client_initialization(self, mock_blob_service_client):
        """Test blob service client initialization with connection string."""
        mock_client = Mock()
        mock_blob_service_client.from_connection_string.return_value = mock_client
        
        client = self.repository.blob_service_client
        
        mock_blob_service_client.from_connection_string.assert_called_once_with("test_connection_string")
        self.assertEqual(client, mock_client)

    @patch("model.repositories.azure_blob_repository.BlobServiceClient")
    def test_upload_file_success(self, mock_blob_service_client):
        """Test successful file upload."""
        # Create temporary test file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as temp_file:
            temp_file.write("test content")
            temp_file_path = temp_file.name
        
        try:
            mock_service_client = Mock()
            mock_blob_client = Mock()
            mock_blob_client.url = "https://test.blob.core.windows.net/test-container/remote_file.txt"
            mock_service_client.get_blob_client.return_value = mock_blob_client
            mock_blob_service_client.from_connection_string.return_value = mock_service_client
            
            with patch.dict(os.environ, {"AZURE_BLOB_CONNECTION_STRING": "test_connection"}):
                result_url = self.repository.upload_file(temp_file_path, "remote_file.txt")
                
                mock_blob_client.upload_blob.assert_called_once()
                self.assertEqual(result_url, mock_blob_client.url)
        
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    @patch("model.repositories.azure_blob_repository.BlobServiceClient")
    def test_upload_file_not_found(self, mock_blob_service_client):
        """Test upload with non-existent file."""
        with patch.dict(os.environ, {"AZURE_BLOB_CONNECTION_STRING": "test_connection"}):
            with self.assertRaises(DataFetchError) as context:
                self.repository.upload_file("non_existent_file.txt", "remote_file.txt")
            
            self.assertIn("Local file not found", str(context.exception))

    @patch("model.repositories.azure_blob_repository.BlobServiceClient")
    def test_download_file_success(self, mock_blob_service_client):
        """Test successful file download."""
        mock_service_client = Mock()
        mock_blob_client = Mock()
        mock_download_stream = Mock()
        mock_download_stream.readall.return_value = b"downloaded content"
        mock_blob_client.download_blob.return_value = mock_download_stream
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service_client.from_connection_string.return_value = mock_service_client
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
        
        try:
            with patch.dict(os.environ, {"AZURE_BLOB_CONNECTION_STRING": "test_connection"}):
                result = self.repository.download_file("remote_file.txt", temp_file_path)
                
                mock_blob_client.download_blob.assert_called_once()
                self.assertTrue(result)
        
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)


def main():
    """Run the tests."""
    unittest.main(verbosity=2)


if __name__ == "__main__":
    main()