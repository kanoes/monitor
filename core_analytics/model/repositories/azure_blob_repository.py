"""
Azure Blob Repository implementation.
"""
from typing import Optional
import logging
import os

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

from core_analytics.core.interfaces import IStorageService
from core_analytics.core.logging_config import DataFetchError
from core_analytics.config.settings import ConfigurationService

class AzureBlobRepository(IStorageService):
    """Repository for Azure Blob Storage operations."""
    
    def __init__(self, config_service: ConfigurationService):
        self.config_service = config_service
        self.logger = logging.getLogger("CoreAnalytics")
        self._blob_service_client = None
    
    @property
    def blob_service_client(self) -> BlobServiceClient:
        """Lazy initialization of Azure Blob Service Client."""
        if self._blob_service_client is None:
            try:
                connection_string = os.environ.get("AZURE_BLOB_CONNECTION_STRING")
                if connection_string:
                    self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                    self.logger.info("Azure Blob Service Client initialized with connection string")
                else:
                    credential = DefaultAzureCredential()
                    
                    account_url = f"https://{os.environ['AZURE_STORAGE_ACCOUNT_NAME']}.blob.core.windows.net"
                    self._blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
                    self.logger.info("Azure Blob Service Client initialized with credentials")
                    
            except Exception as e:
                self.logger.error(f"Failed to initialize Azure Blob Service Client: {e}")
                raise DataFetchError(f"Failed to initialize Azure Blob client: {e}")
        
        return self._blob_service_client
    
    def get_blob_client(self, filename: str) -> BlobClient:
        """Get Azure Blob client for specific file."""
        try:
            app_settings = self.config_service.get_app_settings()
            return self.blob_service_client.get_blob_client(
                container=app_settings.blob_container_name, 
                blob=filename
            )
        except Exception as e:
            self.logger.error(f"Failed to get blob client for {filename}: {e}")
            raise DataFetchError(f"Failed to get blob client: {e}")
    
    def get_container_client(self) -> ContainerClient:
        """Get Azure Blob container client."""
        try:
            app_settings = self.config_service.get_app_settings()
            return self.blob_service_client.get_container_client(app_settings.blob_container_name)
        except Exception as e:
            self.logger.error(f"Failed to get container client: {e}")
            raise DataFetchError(f"Failed to get container client: {e}")
    
    def upload_file(self, local_file_path: str, remote_path: str) -> str:
        """Upload a file to Azure Blob Storage."""
        try:
            self.logger.info(f"Uploading file {local_file_path} to {remote_path}")
            
            blob_client = self.get_blob_client(remote_path)
            
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            
            self.logger.info(f"Successfully uploaded {local_file_path} to {remote_path}")
            return blob_client.url
            
        except FileNotFoundError:
            self.logger.error(f"Local file not found: {local_file_path}")
            raise DataFetchError(f"Local file not found: {local_file_path}")
        except Exception as e:
            self.logger.error(f"Failed to upload {local_file_path}: {e}")
            raise DataFetchError(f"Failed to upload file: {e}")
    
    def download_file(self, remote_path: str, local_file_path: str) -> bool:
        """Download a file from Azure Blob Storage."""
        try:
            self.logger.info(f"Downloading {remote_path} to {local_file_path}")
            
            blob_client = self.get_blob_client(remote_path)
            
            with open(local_file_path, "wb") as download_file:
                download_stream = blob_client.download_blob()
                download_file.write(download_stream.readall())
            
            self.logger.info(f"Successfully downloaded {remote_path} to {local_file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download {remote_path}: {e}")
            raise DataFetchError(f"Failed to download file: {e}")
