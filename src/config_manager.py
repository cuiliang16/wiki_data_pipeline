import os
import yaml
from dotenv import load_dotenv
from typing import Dict, Any, Optional
import logging

class ConfigManager:
    def __init__(self, config_path: str = "config/config.yaml", env_path: str = "config/.env"):
        self.config_path = config_path
        self.env_path = env_path
        self.config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file and environment variables."""
        try:
            # Load environment variables
            if os.path.exists(self.env_path):
                load_dotenv(self.env_path)
            
            # Load YAML configuration
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as file:
                    self.config = yaml.safe_load(file)
            else:
                raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
            # Override Azure connection string from environment if available
            azure_conn_str = os.getenv('AZURE_CONNECTION_STRING')
            if azure_conn_str:
                self.config['azure']['connection_string'] = azure_conn_str
            
            self._validate_config()
            
        except Exception as e:
            logging.error(f"Failed to load configuration: {e}")
            raise
    
    def _validate_config(self):
        """Validate required configuration parameters."""
        required_sections = ['azure', 'naming', 'api', 'logging', 'processing']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate Azure configuration
        if not self.config['azure'].get('connection_string'):
            raise ValueError("Azure connection string is required")
        
        if not self.config['azure'].get('container_name'):
            raise ValueError("Azure container name is required")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key (supports dot notation)."""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_azure_config(self) -> Dict[str, Any]:
        """Get Azure configuration."""
        return self.config.get('azure', {})
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration."""
        return self.config.get('api', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.config.get('logging', {})
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration."""
        return self.config.get('processing', {})
    
    def get_naming_config(self) -> Dict[str, Any]:
        """Get naming configuration."""
        return self.config.get('naming', {}) 