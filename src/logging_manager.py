import logging
import logging.handlers
import os
from typing import Dict, Any

class LoggingManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = None
        self.error_logger = None
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging with rotating file handlers."""
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Main logger setup
        self.logger = logging.getLogger('wiki_pipeline')
        self.logger.setLevel(getattr(logging, self.config.get('level', 'INFO')))
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # Main log file handler (rotating)
        main_log_path = self.config.get('main_log', 'logs/wiki_pipeline.log')
        main_handler = logging.handlers.RotatingFileHandler(
            main_log_path,
            maxBytes=self.config.get('max_file_size', 10485760),  # 10MB
            backupCount=self.config.get('backup_count', 5)
        )
        main_handler.setLevel(logging.INFO)
        main_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        main_handler.setFormatter(main_formatter)
        self.logger.addHandler(main_handler)
        
        # Error logger setup
        self.error_logger = logging.getLogger('wiki_pipeline_errors')
        self.error_logger.setLevel(logging.ERROR)
        self.error_logger.handlers.clear()
        
        # Error log file handler (rotating)
        error_log_path = self.config.get('error_log', 'logs/errors.log')
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_path,
            maxBytes=self.config.get('max_file_size', 10485760),  # 10MB
            backupCount=self.config.get('backup_count', 5)
        )
        error_handler.setLevel(logging.ERROR)
        error_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        error_handler.setFormatter(error_formatter)
        self.error_logger.addHandler(error_handler)
    
    def info(self, message: str):
        """Log info message."""
        self.logger.info(message)
    
    def error(self, message: str):
        """Log error message to both main and error logs."""
        self.logger.error(message)
        self.error_logger.error(message)
    
    def warning(self, message: str):
        """Log warning message."""
        self.logger.warning(message)
    
    def debug(self, message: str):
        """Log debug message."""
        self.logger.debug(message)
    
    def log_progress(self, current: int, total: int, message: str = ""):
        """Log progress information."""
        percentage = (current / total * 100) if total > 0 else 0
        progress_message = f"Progress: {current}/{total} ({percentage:.1f}%) {message}"
        self.info(progress_message)
    
    def log_api_call(self, url: str, status_code: int, response_time: float):
        """Log API call information."""
        self.info(f"API Call: {url} - Status: {status_code} - Time: {response_time:.2f}s")
    
    def log_rate_limit(self, delay: float):
        """Log rate limiting information."""
        self.warning(f"Rate limited, waiting {delay:.1f} seconds")
    
    def log_retry(self, attempt: int, max_attempts: int, error: str):
        """Log retry attempt information."""
        self.warning(f"Retry attempt {attempt}/{max_attempts}: {error}")
    
    def log_batch_upload(self, batch_num: int, file_count: int):
        """Log batch upload information."""
        self.info(f"Uploaded batch {batch_num} with {file_count} files")
    
    def log_article_processed(self, title: str, success: bool):
        """Log article processing result."""
        if success:
            self.debug(f"Processed article: {title}")
        else:
            self.error(f"Failed to process article: {title}") 