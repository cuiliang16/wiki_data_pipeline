#!/usr/bin/env python3
"""
Test script for Wikipedia Data Pipeline
"""

import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from config_manager import ConfigManager
from logging_manager import LoggingManager
from queue_manager import QueueManager
from mediawiki_client import MediaWikiClient
from data_processor import DataProcessor
from checkpoint_manager import CheckpointManager

def test_config_manager():
    """Test configuration manager."""
    print("Testing ConfigManager...")
    try:
        config_manager = ConfigManager()
        print("✓ ConfigManager initialized successfully")
        
        # Test getting configuration values
        azure_config = config_manager.get_azure_config()
        api_config = config_manager.get_api_config()
        logging_config = config_manager.get_logging_config()
        
        print(f"✓ Azure config loaded: {len(azure_config)} items")
        print(f"✓ API config loaded: {len(api_config)} items")
        print(f"✓ Logging config loaded: {len(logging_config)} items")
        
        return True
    except Exception as e:
        print(f"✗ ConfigManager test failed: {e}")
        return False

def test_logging_manager():
    """Test logging manager."""
    print("\nTesting LoggingManager...")
    try:
        config_manager = ConfigManager()
        logging_config = config_manager.get_logging_config()
        logger = LoggingManager(logging_config)
        
        logger.info("Test info message")
        logger.warning("Test warning message")
        logger.error("Test error message")
        
        print("✓ LoggingManager initialized and messages logged")
        return True
    except Exception as e:
        print(f"✗ LoggingManager test failed: {e}")
        return False

def test_queue_manager():
    """Test queue manager."""
    print("\nTesting QueueManager...")
    try:
        queue_manager = QueueManager()
        
        # Test adding articles
        test_articles = [
            {'title': 'Test Article 1', 'pageid': '1'},
            {'title': 'Test Article 2', 'pageid': '2'}
        ]
        queue_manager.add_articles(test_articles)
        
        # Test getting articles
        article = queue_manager.get_next_article()
        if article:
            queue_manager.mark_processed(article['title'])
            print(f"✓ Processed article: {article['title']}")
        
        status = queue_manager.get_queue_status()
        print(f"✓ Queue status: {status}")
        
        return True
    except Exception as e:
        print(f"✗ QueueManager test failed: {e}")
        return False

def test_mediawiki_client():
    """Test MediaWiki client."""
    print("\nTesting MediaWikiClient...")
    try:
        config_manager = ConfigManager()
        api_config = config_manager.get_api_config()
        logging_config = config_manager.get_logging_config()
        logger = LoggingManager(logging_config)
        
        client = MediaWikiClient(api_config, logger)
        
        # Test connection
        if client.test_connection():
            print("✓ MediaWiki API connection successful")
        else:
            print("✗ MediaWiki API connection failed")
            return False
        
        # Test getting a small number of articles
        articles = client.get_all_articles(limit=5)
        print(f"✓ Retrieved {len(articles)} test articles")
        
        return True
    except Exception as e:
        print(f"✗ MediaWikiClient test failed: {e}")
        return False

def test_data_processor():
    """Test data processor."""
    print("\nTesting DataProcessor...")
    try:
        processor = DataProcessor()
        
        # Test article processing
        test_article = {
            'title': 'Test Article',
            'url': 'https://en.wikipedia.org/wiki/Test_Article',
            'content': 'This is test content',
            'categories': ['Test Category'],
            'author': None,
            'pageid': '123'
        }
        
        processed = processor.process_article(test_article)
        
        if processor.validate_article(processed):
            print("✓ Article processed and validated successfully")
        else:
            print("✗ Article validation failed")
            return False
        
        return True
    except Exception as e:
        print(f"✗ DataProcessor test failed: {e}")
        return False

def test_checkpoint_manager():
    """Test checkpoint manager."""
    print("\nTesting CheckpointManager...")
    try:
        checkpoint_manager = CheckpointManager()
        
        # Test checkpoint operations
        checkpoint_manager.start_processing()
        checkpoint_manager.update_progress(100, 1, 100, 0)
        
        summary = checkpoint_manager.get_checkpoint_summary()
        print(f"✓ Checkpoint summary: {summary}")
        
        return True
    except Exception as e:
        print(f"✗ CheckpointManager test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("Wikipedia Data Pipeline - Component Tests")
    print("=" * 50)
    
    tests = [
        test_config_manager,
        test_logging_manager,
        test_queue_manager,
        test_mediawiki_client,
        test_data_processor,
        test_checkpoint_manager
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed! Pipeline is ready to use.")
        return True
    else:
        print("✗ Some tests failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 