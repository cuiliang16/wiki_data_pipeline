import os
import json
from typing import Dict, Any, List, Optional
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import AzureError

class AzureManager:
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.connection_string = config.get('connection_string')
        self.container_name = config.get('container_name', 'wikipedia-data')
        self.batch_size = config.get('batch_size', 1000)
        
        if not self.connection_string:
            raise ValueError("Azure connection string is required")
        
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        
        # Ensure container exists
        self._ensure_container_exists()
    
    def _ensure_container_exists(self):
        """Ensure the Azure container exists."""
        try:
            self.container_client.get_container_properties()
        except AzureError:
            # Container doesn't exist, create it
            self.blob_service_client.create_container(self.container_name)
            self.logger.info(f"Created Azure container: {self.container_name}")
    
    def upload_article(self, article_data: Dict[str, Any], file_index: int, batch_num: int) -> bool:
        """Upload a single article to Azure Blob Storage."""
        try:
            # Format filename and directory
            file_pattern = self.config.get('file_pattern', 'article_{index:06d}.json')
            dir_pattern = self.config.get('directory_pattern', 'batch_{batch_num:03d}')
            
            filename = file_pattern.format(index=file_index)
            directory = dir_pattern.format(batch_num=batch_num)
            
            # Create blob path
            blob_path = f"{directory}/{filename}"
            
            # Convert article data to JSON
            json_data = json.dumps(article_data, ensure_ascii=False, indent=2)
            
            # Upload to Azure
            blob_client = self.container_client.get_blob_client(blob_path)
            blob_client.upload_blob(json_data, overwrite=True)
            
            self.logger.debug(f"Uploaded article: {article_data.get('title', 'Unknown')} to {blob_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload article {article_data.get('title', 'Unknown')}: {str(e)}")
            return False
    
    def upload_batch(self, articles: List[Dict[str, Any]], batch_num: int, start_index: int) -> bool:
        """Upload a batch of articles to Azure Blob Storage."""
        try:
            success_count = 0
            total_count = len(articles)
            
            for i, article in enumerate(articles):
                file_index = start_index + i
                if self.upload_article(article, file_index, batch_num):
                    success_count += 1
            
            self.logger.log_batch_upload(batch_num, success_count)
            self.logger.info(f"Batch {batch_num}: {success_count}/{total_count} articles uploaded successfully")
            
            return success_count == total_count
            
        except Exception as e:
            self.logger.error(f"Failed to upload batch {batch_num}: {str(e)}")
            return False
    
    def test_connection(self) -> bool:
        """Test the connection to Azure Blob Storage."""
        try:
            # Try to get container properties
            self.container_client.get_container_properties()
            self.logger.info("Azure Blob Storage connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"Azure Blob Storage connection test failed: {str(e)}")
            return False
    
    def get_container_info(self) -> Dict[str, Any]:
        """Get information about the Azure container."""
        try:
            properties = self.container_client.get_container_properties()
            blobs = list(self.container_client.list_blobs())
            
            return {
                'container_name': self.container_name,
                'blob_count': len(blobs),
                'last_modified': properties.last_modified.isoformat() if properties.last_modified else None,
                'etag': properties.etag
            }
        except Exception as e:
            self.logger.error(f"Failed to get container info: {str(e)}")
            return {}
    
    def list_blobs(self, prefix: Optional[str] = None) -> List[str]:
        """List blobs in the container with optional prefix."""
        try:
            blobs = self.container_client.list_blobs(name_starts_with=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            self.logger.error(f"Failed to list blobs: {str(e)}")
            return []
    
    def delete_blob(self, blob_name: str) -> bool:
        """Delete a specific blob."""
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            self.logger.info(f"Deleted blob: {blob_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete blob {blob_name}: {str(e)}")
            return False
    
    def download_blob(self, blob_name: str) -> Optional[Dict[str, Any]]:
        """Download and parse a blob as JSON."""
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob()
            content = blob_data.readall().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            self.logger.error(f"Failed to download blob {blob_name}: {str(e)}")
            return None 