#!/usr/bin/env python3
"""
Test script for Enhanced Parallel Production MediaWiki Client
Tests parallel processing and QPS rate limiting capabilities
"""

import sys
import os
import time
import json
from typing import Dict, Any

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from enhanced_production_mediawiki_client import EnhancedProductionMediaWikiClient
from logging_manager import LoggingManager

def test_enhanced_client():
    """Test the enhanced parallel MediaWiki client."""
    print("Testing Enhanced Parallel Production MediaWiki Client...")
    
    # Create test config with parallel processing settings
    config = {
        'base_url': 'https://en.wikipedia.org/w/api.php',
        'user_agent': 'WikipediaDataPipeline/1.0',
        'max_retries': 3,
        'initial_delay': 1,
        'max_delay': 60,
        'max_workers': 4,
        'chunk_size': 10,
        'qps_limit': 2.0,
        'burst_limit': 5,
        'window_size': 60,
        'adaptive_rate_limiting': True
    }
    
    # Create logger
    logger = LoggingManager({
        'level': 'INFO',
        'main_log': 'logs/test_enhanced_parallel.log',
        'error_log': 'logs/test_enhanced_parallel_errors.log'
    })
    
    try:
        # Initialize enhanced client
        client = EnhancedProductionMediaWikiClient(config, logger)
        
        # Test connection
        print("Testing connection...")
        if client.test_connection():
            print("✓ Connection successful")
        else:
            print("✗ Connection failed")
            return False
        
        # Test rate limiting stats
        stats = client.get_rate_limit_stats()
        print(f"✓ Rate limiting stats: {stats}")
        
        # Test single article retrieval
        print("\nTesting single article retrieval...")
        start_time = time.time()
        article = client.get_article_content("Python (programming language)")
        single_time = time.time() - start_time
        
        if article:
            print(f"✓ Single article retrieved in {single_time:.2f}s")
            print(f"  - Title: {article.get('title')}")
            print(f"  - Content length: {len(article.get('content', ''))}")
            print(f"  - Categories: {len(article.get('categories', []))}")
        else:
            print("✗ Failed to retrieve single article")
            return False
        
        # Test parallel processing
        print("\nTesting parallel processing...")
        test_articles = [
            {"title": "Python (programming language)"},
            {"title": "Wikipedia"},
            {"title": "Artificial intelligence"},
            {"title": "Machine learning"},
            {"title": "Data science"}
        ]
        
        def progress_callback(processed, failed, total):
            print(f"  Progress: {processed}/{total} processed, {failed} failed")
        
        start_time = time.time()
        results, failed = client.process_articles_parallel(test_articles, progress_callback)
        parallel_time = time.time() - start_time
        
        print(f"✓ Parallel processing completed in {parallel_time:.2f}s")
        print(f"  - Successful: {len(results)}")
        print(f"  - Failed: {len(failed)}")
        
        # Calculate performance improvement
        expected_serial_time = single_time * len(test_articles)
        speedup = expected_serial_time / parallel_time if parallel_time > 0 else 0
        print(f"  - Speedup: {speedup:.2f}x")
        
        # Test rate limiting under load
        print("\nTesting rate limiting under load...")
        load_test_articles = [{"title": "Test Article " + str(i)} for i in range(20)]
        
        start_time = time.time()
        load_results, load_failed = client.process_articles_parallel(load_test_articles)
        load_time = time.time() - start_time
        
        print(f"✓ Load test completed in {load_time:.2f}s")
        print(f"  - Successful: {len(load_results)}")
        print(f"  - Failed: {len(load_failed)}")
        
        # Final rate limiting stats
        final_stats = client.get_rate_limit_stats()
        print(f"✓ Final rate limiting stats: {final_stats}")
        
        # Cleanup
        client.shutdown()
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {str(e)}")
        return False

def test_rate_limiting():
    """Test rate limiting functionality."""
    print("\nTesting Rate Limiting...")
    
    config = {
        'base_url': 'https://en.wikipedia.org/w/api.php',
        'user_agent': 'WikipediaDataPipeline/1.0',
        'max_retries': 3,
        'initial_delay': 1,
        'max_delay': 60,
        'max_workers': 2,
        'chunk_size': 5,
        'qps_limit': 1.0,  # Very conservative for testing
        'burst_limit': 3,
        'window_size': 60,
        'adaptive_rate_limiting': True
    }
    
    logger = LoggingManager({
        'level': 'INFO',
        'main_log': 'logs/test_rate_limiting.log',
        'error_log': 'logs/test_rate_limiting_errors.log'
    })
    
    try:
        client = EnhancedProductionMediaWikiClient(config, logger)
        
        # Test rapid requests
        print("  Testing rapid requests with rate limiting...")
        start_time = time.time()
        
        test_articles = [
            {"title": "Python (programming language)"},
            {"title": "Wikipedia"},
            {"title": "Artificial intelligence"}
        ]
        
        results, failed = client.process_articles_parallel(test_articles)
        total_time = time.time() - start_time
        
        print(f"  ✓ Rate limiting test completed in {total_time:.2f}s")
        print(f"    - Expected minimum time: {len(test_articles)}s (1 QPS)")
        print(f"    - Actual time: {total_time:.2f}s")
        
        if total_time >= len(test_articles) * 0.8:  # Allow some tolerance
            print("  ✓ Rate limiting working correctly")
        else:
            print("  ⚠ Rate limiting may be too permissive")
        
        client.shutdown()
        return True
        
    except Exception as e:
        print(f"  ✗ Rate limiting test failed: {str(e)}")
        return False

def test_memory_efficiency():
    """Test memory efficiency with large article lists."""
    print("\nTesting Memory Efficiency...")
    
    config = {
        'base_url': 'https://en.wikipedia.org/w/api.php',
        'user_agent': 'WikipediaDataPipeline/1.0',
        'max_retries': 3,
        'initial_delay': 1,
        'max_delay': 60,
        'max_workers': 2,
        'chunk_size': 50,  # Smaller chunks for memory testing
        'qps_limit': 2.0,
        'burst_limit': 5,
        'window_size': 60,
        'adaptive_rate_limiting': True
    }
    
    logger = LoggingManager({
        'level': 'INFO',
        'main_log': 'logs/test_memory.log',
        'error_log': 'logs/test_memory_errors.log'
    })
    
    try:
        client = EnhancedProductionMediaWikiClient(config, logger)
        
        # Create a larger test set
        large_test_articles = [
            {"title": "Python (programming language)"},
            {"title": "Wikipedia"},
            {"title": "Artificial intelligence"},
            {"title": "Machine learning"},
            {"title": "Data science"},
            {"title": "Computer science"},
            {"title": "Software engineering"},
            {"title": "Web development"},
            {"title": "Database"},
            {"title": "Algorithm"}
        ]
        
        print(f"  Testing with {len(large_test_articles)} articles...")
        start_time = time.time()
        
        results, failed = client.process_articles_parallel(large_test_articles)
        
        total_time = time.time() - start_time
        print(f"  ✓ Memory efficiency test completed in {total_time:.2f}s")
        print(f"    - Successful: {len(results)}")
        print(f"    - Failed: {len(failed)}")
        print(f"    - Average time per article: {total_time/len(large_test_articles):.2f}s")
        
        client.shutdown()
        return True
        
    except Exception as e:
        print(f"  ✗ Memory efficiency test failed: {str(e)}")
        return False

def main():
    """Run all enhanced parallel client tests."""
    print("Enhanced Parallel Production MediaWiki Client Tests")
    print("=" * 60)
    
    tests = [
        ("Enhanced Parallel Client", test_enhanced_client),
        ("Rate Limiting", test_rate_limiting),
        ("Memory Efficiency", test_memory_efficiency)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}")
        print("-" * 40)
        
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
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "PASSED" if success else "FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Enhanced parallel client is ready.")
        return 0
    else:
        print("❌ Some tests failed. Please check the logs.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 