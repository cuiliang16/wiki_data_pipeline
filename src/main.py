import sys
import os
import time
from typing import Dict, Any, List, Optional

# Add src directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config_manager import ConfigManager
from logging_manager import LoggingManager
from queue_manager import QueueManager
from mediawiki_client import MediaWikiClient
from azure_manager import AzureManager
from data_processor import DataProcessor
from checkpoint_manager import CheckpointManager

class WikipediaDataPipeline:
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the Wikipedia data pipeline."""
        self.config_manager = ConfigManager(config_path)
        self.logger = LoggingManager(self.config_manager.get_logging_config())
        self.queue_manager = QueueManager(
            enable_persistence=self.config_manager.get('processing.queue_persistence', True)
        )
        self.mediawiki_client = MediaWikiClient(
            self.config_manager.get_api_config(), 
            self.logger
        )
        self.azure_manager = AzureManager(
            self.config_manager.get_azure_config(), 
            self.logger
        )
        self.data_processor = DataProcessor()
        self.checkpoint_manager = CheckpointManager()
        
        self.batch_size = self.config_manager.get('processing.batch_size', 1000)
        self.checkpoint_interval = self.config_manager.get('processing.checkpoint_interval', 100)
    
    def initialize(self) -> bool:
        """Initialize all components and test connections."""
        try:
            self.logger.info("Initializing Wikipedia Data Pipeline...")
            
            # Test MediaWiki API connection
            self.logger.info("Testing MediaWiki API connection...")
            if not self.mediawiki_client.test_connection():
                self.logger.error("Failed to connect to MediaWiki API")
                return False
            
            # Test Azure Blob Storage connection
            self.logger.info("Testing Azure Blob Storage connection...")
            if not self.azure_manager.test_connection():
                self.logger.error("Failed to connect to Azure Blob Storage")
                return False
            
            self.logger.info("All connections successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Initialization failed: {str(e)}")
            return False
    
    def populate_queue(self, limit: Optional[int] = None) -> int:
        """Populate the queue with Wikipedia articles."""
        try:
            self.logger.info("Fetching Wikipedia articles...")
            
            # Check if we can resume from checkpoint
            if self.checkpoint_manager.can_resume():
                resume_info = self.checkpoint_manager.get_resume_info()
                self.logger.info(f"Resuming from checkpoint: {resume_info}")
                return resume_info['total_processed']
            
            # Get all articles from MediaWiki API
            articles = self.mediawiki_client.get_all_articles(limit)
            self.logger.info(f"Found {len(articles)} articles")
            
            # Add articles to queue
            self.queue_manager.add_articles(articles)
            
            return len(articles)
            
        except Exception as e:
            self.logger.error(f"Failed to populate queue: {str(e)}")
            return 0
    
    def process_articles(self) -> Dict[str, int]:
        """Process articles from the queue."""
        try:
            self.logger.info("Starting article processing...")
            self.checkpoint_manager.start_processing()
            
            processed_count = 0
            failed_count = 0
            current_batch = []
            batch_number = self.checkpoint_manager.get_last_batch_number()
            start_index = self.checkpoint_manager.get_last_processed_index()
            
            while not self.queue_manager.is_empty():
                # Get next article
                article = self.queue_manager.get_next_article()
                if not article:
                    break
                
                try:
                    # Fetch article content
                    title = article.get('title', '')
                    self.logger.debug(f"Processing article: {title}")
                    
                    article_content = self.mediawiki_client.get_article_html(title)
                    
                    if article_content:
                        # Process article data
                        processed_article = self.data_processor.process_article(article_content)
                        
                        if self.data_processor.validate_article(processed_article):
                            current_batch.append(processed_article)
                            self.queue_manager.mark_processed(title)
                            processed_count += 1
                            self.logger.log_article_processed(title, True)
                        else:
                            self.queue_manager.mark_failed(title, "Invalid article data")
                            failed_count += 1
                            self.logger.log_article_processed(title, False)
                    else:
                        # Article not found or is redirect
                        self.queue_manager.mark_failed(title, "Article not found or is redirect")
                        failed_count += 1
                        self.logger.log_article_processed(title, False)
                
                except Exception as e:
                    title = article.get('title', 'Unknown')
                    self.queue_manager.mark_failed(title, str(e))
                    failed_count += 1
                    self.logger.error(f"Failed to process article {title}: {str(e)}")
                
                # Upload batch when full
                if len(current_batch) >= self.batch_size:
                    batch_number += 1
                    self._upload_batch(current_batch, batch_number, start_index)
                    start_index += len(current_batch)
                    current_batch = []
                    
                    # Update checkpoint
                    self.checkpoint_manager.update_progress(
                        start_index, batch_number, processed_count, failed_count
                    )
                    
                    # Log progress
                    queue_status = self.queue_manager.get_queue_status()
                    self.logger.log_progress(
                        processed_count, 
                        processed_count + queue_status['queue_size'] + failed_count,
                        f"Queue: {queue_status['queue_size']}, Failed: {failed_count}"
                    )
            
            # Upload remaining articles in final batch
            if current_batch:
                batch_number += 1
                self._upload_batch(current_batch, batch_number, start_index)
            
            # Final checkpoint update
            self.checkpoint_manager.update_progress(
                start_index + len(current_batch), batch_number, processed_count, failed_count
            )
            
            self.checkpoint_manager.finish_processing()
            
            return {
                'processed': processed_count,
                'failed': failed_count,
                'batches': batch_number
            }
            
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            return {'processed': 0, 'failed': 0, 'batches': 0}
    
    def _upload_batch(self, articles: List[Dict[str, Any]], batch_number: int, start_index: int):
        """Upload a batch of articles to Azure Blob Storage."""
        try:
            self.logger.info(f"Uploading batch {batch_number} with {len(articles)} articles...")
            
            success = self.azure_manager.upload_batch(articles, batch_number, start_index)
            
            if success:
                self.logger.info(f"Successfully uploaded batch {batch_number}")
            else:
                self.logger.error(f"Failed to upload batch {batch_number}")
                
        except Exception as e:
            self.logger.error(f"Error uploading batch {batch_number}: {str(e)}")
    
    def run(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete Wikipedia data pipeline."""
        try:
            self.logger.info("Starting Wikipedia Data Pipeline")
            
            # Initialize components
            if not self.initialize():
                return {'success': False, 'error': 'Initialization failed'}
            
            # Populate queue
            total_articles = self.populate_queue(limit)
            if total_articles == 0:
                return {'success': False, 'error': 'No articles found'}
            
            self.logger.info(f"Processing {total_articles} articles")
            
            # Process articles
            results = self.process_articles()
            
            # Create summary
            summary = {
                'success': True,
                'total_articles': total_articles,
                'processed': results['processed'],
                'failed': results['failed'],
                'batches': results['batches'],
                'checkpoint_summary': self.checkpoint_manager.get_checkpoint_summary()
            }
            
            self.logger.info(f"Pipeline completed successfully: {summary}")
            return summary
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return {'success': False, 'error': str(e)}

def main():
    """Main entry point for the Wikipedia data pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Wikipedia Data Pipeline')
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    parser.add_argument('--limit', type=int, help='Limit number of articles to process')
    parser.add_argument('--reset', action='store_true', help='Reset checkpoint and start fresh')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = WikipediaDataPipeline(args.config)
    
    # Reset checkpoint if requested
    if args.reset:
        pipeline.checkpoint_manager.reset_checkpoint()
        pipeline.queue_manager.reset_all()
        print("Checkpoint and queue reset")
    
    # Run pipeline
    results = pipeline.run(args.limit)
    
    if results['success']:
        print(f"Pipeline completed successfully!")
        print(f"Total articles: {results['total_articles']}")
        print(f"Processed: {results['processed']}")
        print(f"Failed: {results['failed']}")
        print(f"Batches: {results['batches']}")
    else:
        print(f"Pipeline failed: {results['error']}")
        sys.exit(1)

if __name__ == "__main__":
    main() 