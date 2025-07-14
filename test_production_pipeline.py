#!/usr/bin/env python3
"""
Test script for Production Wikipedia Data Pipeline
Tests the mwclient + wikitextparser implementation
"""

import sys
import os
import json
from typing import Dict, Any

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from production_mediawiki_client import ProductionMediaWikiClient
from production_data_processor import ProductionDataProcessor
from logging_manager import LoggingManager

def test_production_client():
    """Test the production MediaWiki client."""
    print("Testing Production MediaWiki Client...")
    
    # Create test config
    config = {
        'base_url': 'https://en.wikipedia.org/w/api.php',
        'user_agent': 'WikipediaDataPipeline/1.0',
        'max_retries': 3,
        'initial_delay': 1,
        'max_delay': 60
    }
    
    # Create logger
    logger = LoggingManager({
        'level': 'INFO',
        'main_log': 'logs/test_production.log',
        'error_log': 'logs/test_production_errors.log'
    })
    
    try:
        # Initialize client
        client = ProductionMediaWikiClient(config, logger)
        
        # Test connection
        print("Testing connection...")
        if client.test_connection():
            print("✓ Connection successful")
        else:
            print("✗ Connection failed")
            return False
        
        # Test site info
        print("Getting site info...")
        site_info = client.get_site_info()
        print(f"✓ Site: {site_info.get('sitename', 'Unknown')}")
        
        # Test available libraries
        libraries = client.get_available_libraries()
        print(f"✓ Available libraries: {libraries}")
        
        # Test article retrieval
        test_articles = [
            "Python (programming language)",
            "Wikipedia",
            "Artificial intelligence"
        ]
        
        for title in test_articles:
            print(f"\nTesting article: {title}")
            article = client.get_article_content(title)
            
            if article:
                print(f"✓ Article retrieved successfully")
                print(f"  - Title: {article.get('title')}")
                print(f"  - URL: {article.get('url')}")
                print(f"  - Content length: {len(article.get('content', ''))}")
                print(f"  - Parsed content length: {len(article.get('parsed_content', ''))}")
                print(f"  - Plain text length: {len(article.get('plain_text', ''))}")
                print(f"  - Categories: {len(article.get('categories', []))}")
                print(f"  - Source: {article.get('source')}")
                
                # Test data processor
                processor = ProductionDataProcessor()
                processed = processor.process_article(article)
                
                if processor.validate_article(processed):
                    print(f"  ✓ Article processed successfully")
                    print(f"  - Metadata: {processed.get('metadata', {})}")
                else:
                    print(f"  ✗ Article processing failed")
            else:
                print(f"✗ Failed to retrieve article: {title}")
        
        # Test article enumeration (limited)
        print("\nTesting article enumeration (limited to 10)...")
        articles = client.get_all_articles(limit=10)
        print(f"✓ Retrieved {len(articles)} articles")
        
        for i, article in enumerate(articles[:3]):
            print(f"  {i+1}. {article.get('title')} (ID: {article.get('pageid')})")
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {str(e)}")
        return False

def test_production_processor():
    """Test the production data processor."""
    print("\nTesting Production Data Processor...")
    
    processor = ProductionDataProcessor()
    
    # Test data
    test_article = {
        'title': 'Test Article',
        'url': 'https://en.wikipedia.org/wiki/Test_Article',
        'content': 'This is [[wikitext]] content with **bold** and *italic*.',
        'parsed_content': 'This is wikitext content with bold and italic.',
        'plain_text': 'This is wikitext content with bold and italic.',
        'categories': ['Test', 'Example'],
        'author': None,
        'pageid': '12345',
        'source': 'mwclient',
        'metadata': {
            'pageid': '12345',
            'namespace': 0,
            'revision': '67890',
            'length': 50,
            'touched': '2024-01-01T00:00:00Z'
        }
    }
    
    try:
        # Test processing
        processed = processor.process_article(test_article)
        
        if processor.validate_article(processed):
            print("✓ Article processing successful")
            print(f"  - Title: {processed.get('title')}")
            print(f"  - Content length: {len(processed.get('content', ''))}")
            print(f"  - Categories: {processed.get('categories')}")
            print(f"  - Metadata: {processed.get('metadata', {})}")
        else:
            print("✗ Article processing failed validation")
            return False
        
        # Test JSON formatting
        json_data = processor.format_for_storage(test_article)
        print("✓ JSON formatting successful")
        print(f"  - JSON length: {len(json_data)} characters")
        
        # Test batch processing
        batch = [test_article, test_article]
        processed_batch = processor.batch_process(batch)
        print(f"✓ Batch processing successful: {len(processed_batch)} articles")
        
        # Test summary creation
        summary = processor.create_summary(processed_batch)
        print("✓ Summary creation successful")
        print(f"  - Total articles: {summary.get('total_articles')}")
        print(f"  - Total content length: {summary.get('total_content_length')}")
        print(f"  - Average content length: {summary.get('average_content_length')}")
        
        return True
        
    except Exception as e:
        print(f"✗ Processor test failed: {str(e)}")
        return False

def test_integration():
    """Test integration between client and processor."""
    print("\nTesting Integration...")
    
    # Create test config
    config = {
        'base_url': 'https://en.wikipedia.org/w/api.php',
        'user_agent': 'WikipediaDataPipeline/1.0',
        'max_retries': 3,
        'initial_delay': 1,
        'max_delay': 60
    }
    
    logger = LoggingManager({
        'level': 'INFO',
        'main_log': 'logs/test_integration.log',
        'error_log': 'logs/test_integration_errors.log'
    })
    
    try:
        # Initialize components
        client = ProductionMediaWikiClient(config, logger)
        processor = ProductionDataProcessor()
        
        # Test with a real article
        title = "Python (programming language)"
        print(f"Testing integration with article: {title}")
        
        # Get article
        article = client.get_article_content(title)
        
        if article:
            # Process article
            processed = processor.process_article(article)
            
            if processor.validate_article(processed):
                print("✓ Integration test successful")
                print(f"  - Original content length: {len(article.get('content', ''))}")
                print(f"  - Processed content length: {len(processed.get('content', ''))}")
                print(f"  - Categories: {len(processed.get('categories', []))}")
                print(f"  - Source: {processed.get('source')}")
                
                # Test JSON output
                json_output = processor.format_for_storage(processed)
                print(f"  - JSON output length: {len(json_output)} characters")
                
                # Save sample output
                with open('test_output_production.json', 'w', encoding='utf-8') as f:
                    f.write(json_output)
                print("  - Sample output saved to test_output_production.json")
                
                return True
            else:
                print("✗ Integration test failed - article validation failed")
                return False
        else:
            print("✗ Integration test failed - could not retrieve article")
            return False
            
    except Exception as e:
        print(f"✗ Integration test failed: {str(e)}")
        return False

def main():
    """Run all production tests."""
    print("Production Wikipedia Data Pipeline Tests")
    print("=" * 50)
    
    tests = [
        ("Production MediaWiki Client", test_production_client),
        ("Production Data Processor", test_production_processor),
        ("Integration Test", test_integration)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}")
        print("-" * 30)
        
        try:
            success = test_func()
            results.append((test_name, success))
            
            if success:
                print(f"✓ {test_name} PASSED")
            else:
                print(f"✗ {test_name} FAILED")
                
        except Exception as e:
            print(f"✗ {test_name} ERROR: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "PASSED" if success else "FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Production pipeline is ready.")
        return 0
    else:
        print("❌ Some tests failed. Please check the logs.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 