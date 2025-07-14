import requests
import time
import urllib.parse
import threading
import queue
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Dict, Any, List, Optional, Literal, Callable
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

try:
    import mwclient
    import wikitextparser
    MWCLIENT_AVAILABLE = True
    WIKITEXTPARSER_AVAILABLE = True
except ImportError as e:
    MWCLIENT_AVAILABLE = False
    WIKITEXTPARSER_AVAILABLE = False
    print(f"Warning: Required libraries not available: {e}")

@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    requests_per_second: float = 2.0  # Default: 2 requests per second
    burst_limit: int = 5  # Allow burst of 5 requests
    window_size: int = 60  # 60-second sliding window
    adaptive_enabled: bool = True  # Enable adaptive rate limiting

class AdaptiveRateLimiter:
    """Adaptive rate limiter with QPS control."""
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.request_times = []
        self.lock = threading.Lock()
        self.last_429_time = None
        self.consecutive_429s = 0
        self.current_qps = config.requests_per_second
        
    def should_wait(self) -> float:
        """Determine if we should wait and for how long."""
        with self.lock:
            now = time.time()
            
            # Clean old requests outside the window
            self.request_times = [t for t in self.request_times 
                                if now - t < self.config.window_size]
            
            # Check if we're within burst limit
            if len(self.request_times) >= self.config.burst_limit:
                # Wait until we can make another request
                wait_time = self.request_times[0] + (1.0 / self.current_qps) - now
                return max(0, wait_time)
            
            # Check QPS limit
            if len(self.request_times) > 0:
                time_since_first = now - self.request_times[0]
                if time_since_first < 1.0 / self.current_qps:
                    return (1.0 / self.current_qps) - time_since_first
            
            return 0.0
    
    def record_request(self, status_code: int = 200):
        """Record a request and adjust rate if needed."""
        with self.lock:
            now = time.time()
            self.request_times.append(now)
            
            if self.config.adaptive_enabled:
                self._adjust_rate(status_code, now)
    
    def _adjust_rate(self, status_code: int, timestamp: float):
        """Adaptively adjust rate based on response status."""
        if status_code == 429:
            self.consecutive_429s += 1
            self.last_429_time = timestamp
            
            # Reduce rate on consecutive 429s
            if self.consecutive_429s >= 3:
                self.current_qps = max(0.5, self.current_qps * 0.8)
                self.logger.warning(f"Reducing QPS to {self.current_qps} due to 429s")
        else:
            # Gradually increase rate if no 429s
            if self.consecutive_429s > 0:
                self.consecutive_429s = max(0, self.consecutive_429s - 1)
            
            # Increase rate if no 429s for a while
            if (self.last_429_time is None or 
                timestamp - self.last_429_time > 300):  # 5 minutes
                self.current_qps = min(self.config.requests_per_second, 
                                     self.current_qps * 1.1)

class EnhancedProductionMediaWikiClient:
    """Enhanced MediaWiki client with parallel processing and QPS rate limiting."""
    
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.base_url = config.get('base_url', 'https://en.wikipedia.org/w/api.php')
        self.user_agent = config.get('user_agent', 'WikipediaDataPipeline/1.0')
        self.max_retries = config.get('max_retries', 3)
        self.initial_delay = config.get('initial_delay', 1)
        self.max_delay = config.get('max_delay', 60)
        
        # Parallel processing configuration
        self.max_workers = config.get('max_workers', 4)
        self.chunk_size = config.get('chunk_size', 100)
        
        # Rate limiting configuration
        rate_limit_config = RateLimitConfig(
            requests_per_second=config.get('qps_limit', 2.0),
            burst_limit=config.get('burst_limit', 5),
            window_size=config.get('window_size', 60),
            adaptive_enabled=config.get('adaptive_rate_limiting', True)
        )
        self.rate_limiter = AdaptiveRateLimiter(rate_limit_config)
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Initialize mwclient site
        self.mwclient_site = None
        if MWCLIENT_AVAILABLE:
            try:
                self.mwclient_site = mwclient.Site('en.wikipedia.org')
                self.logger.info("mwclient library initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize mwclient: {e}")
                raise
        else:
            raise ImportError("mwclient is required for production environment")
        
        # Initialize requests session for fallback
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent
        })
        
        # Thread-safe request counter
        self.request_counter = 0
        self.request_lock = threading.Lock()
    
    def _make_request_with_rate_limit(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make API request with rate limiting."""
        for attempt in range(self.max_retries + 1):
            try:
                # Apply rate limiting
                wait_time = self.rate_limiter.should_wait()
                if wait_time > 0:
                    time.sleep(wait_time)
                
                start_time = time.time()
                response = self.session.get(self.base_url, params=params, timeout=30)
                response_time = time.time() - start_time
                
                # Record request for rate limiting
                self.rate_limiter.record_request(response.status_code)
                
                self.logger.log_api_call(f"{self.base_url}?{urllib.parse.urlencode(params)}", 
                                       response.status_code, response_time)
                
                if response.status_code == 429:
                    # Rate limited - exponential backoff
                    delay = min(self.initial_delay * (2 ** attempt), self.max_delay)
                    self.logger.log_rate_limit(delay)
                    time.sleep(delay)
                    continue
                
                if response.status_code == 200:
                    return response.json()
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    self.logger.log_retry(attempt + 1, self.max_retries, error_msg)
                    
                    if attempt < self.max_retries:
                        delay = min(self.initial_delay * (2 ** attempt), self.max_delay)
                        time.sleep(delay)
                    else:
                        self.logger.error(f"Failed after {self.max_retries} attempts: {error_msg}")
                        return None
                        
            except requests.exceptions.RequestException as e:
                error_msg = f"Request exception: {str(e)}"
                self.logger.log_retry(attempt + 1, self.max_retries, error_msg)
                
                if attempt < self.max_retries:
                    delay = min(self.initial_delay * (2 ** attempt), self.max_delay)
                    time.sleep(delay)
                else:
                    self.logger.error(f"Failed after {self.max_retries} attempts: {error_msg}")
                    return None
        
        return None
    
    def get_article_content(self, title: str) -> Optional[Dict[str, Any]]:
        """Get article content using mwclient + wikitextparser (production method)."""
        try:
            # Get page using mwclient
            page = self.mwclient_site.pages[title]
            if not page.exists:
                self.logger.warning(f"Page does not exist: {title}")
                return None
            
            # Check if it's a redirect
            if page.redirect:
                self.logger.debug(f"Page is a redirect: {title}")
                return None
            
            # Get wikitext content
            wikitext = page.text()
            
            # Parse wikitext using wikitextparser
            parsed_content = wikitext
            plain_text = wikitext
            
            if WIKITEXTPARSER_AVAILABLE:
                try:
                    parser = wikitextparser.parse(wikitext)
                    # Extract plain text from wikitext
                    plain_text = parser.plain_text()
                    # Get parsed content (cleaned wikitext)
                    parsed_content = str(parser)
                except Exception as e:
                    self.logger.warning(f"Failed to parse wikitext for {title}: {e}")
            
            # Get categories using mwclient
            categories = []
            try:
                for cat in page.categories():
                    cat_name = cat.name
                    if cat_name.startswith('Category:'):
                        cat_name = cat_name.replace('Category:', '')
                    categories.append(cat_name)
            except Exception as e:
                self.logger.warning(f"Failed to get categories for {title}: {e}")
            
            # Get additional metadata
            page_info = {
                'pageid': page.pageid,
                'namespace': page.namespace,
                'revision': page.revision,
                'length': len(wikitext),
                'touched': self._format_timestamp(page.touched) if page.touched else None
            }
            
            # Construct Wikipedia URL
            url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(title)}"
            
            return {
                'title': title,
                'url': url,
                'content': wikitext,  # Original wikitext
                'parsed_content': parsed_content,  # Parsed wikitext
                'plain_text': plain_text,  # Plain text
                'categories': categories,
                'author': None,
                'pageid': page.pageid,
                'format': 'wikitext',
                'source': 'mwclient',
                'metadata': page_info
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get article {title} with mwclient: {e}")
            # Fallback to basic API if mwclient fails
            return self._get_article_fallback(title)
    
    def _format_timestamp(self, timestamp) -> Optional[str]:
        """Format timestamp to ISO format."""
        try:
            if hasattr(timestamp, 'isoformat'):
                return timestamp.isoformat()
            elif hasattr(timestamp, 'strftime'):
                return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                return str(timestamp)
        except Exception:
            return None
    
    def _get_article_fallback(self, title: str) -> Optional[Dict[str, Any]]:
        """Fallback method using basic MediaWiki API."""
        self.logger.warning(f"Using fallback API for article: {title}")
        
        content_params = {
            'action': 'query',
            'titles': title,
            'prop': 'revisions|categories',
            'rvprop': 'content',
            'cllimit': 500,
            'format': 'json'
        }
        
        response = self._make_request_with_rate_limit(content_params)
        if not response:
            return None
        
        query = response.get('query', {})
        pages = query.get('pages', {})
        
        if not pages:
            return None
        
        page_data = list(pages.values())[0]
        
        # Check if page is a redirect
        if 'missing' in page_data:
            return None
        
        # Get content
        revisions = page_data.get('revisions', [])
        if not revisions:
            return None
        
        content = revisions[0].get('*', '')
        
        # Check if it's a redirect
        if content.startswith('#REDIRECT') or content.startswith('#redirect'):
            return None
        
        # Parse wikitext if parser available
        parsed_content = content
        plain_text = content
        if WIKITEXTPARSER_AVAILABLE:
            try:
                parser = wikitextparser.parse(content)
                plain_text = parser.plain_text()
                parsed_content = str(parser)
            except Exception as e:
                self.logger.warning(f"Failed to parse wikitext for {title}: {e}")
        
        # Get categories
        categories = []
        for category in page_data.get('categories', []):
            cat_title = category.get('title', '')
            if cat_title.startswith('Category:'):
                cat_name = cat_title.replace('Category:', '')
                categories.append(cat_name)
        
        # Construct Wikipedia URL
        url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(title)}"
        
        return {
            'title': title,
            'url': url,
            'content': content,
            'parsed_content': parsed_content,
            'plain_text': plain_text,
            'categories': categories,
            'author': None,
            'pageid': page_data.get('pageid', ''),
            'format': 'wikitext',
            'source': 'fallback_api',
            'metadata': {
                'pageid': page_data.get('pageid'),
                'length': len(content)
            }
        }
    
    def process_articles_parallel(self, articles: List[Dict[str, Any]], 
                                callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Process articles in parallel with rate limiting."""
        results = []
        failed_articles = []
        
        # Split articles into chunks for parallel processing
        chunks = [articles[i:i + self.chunk_size] 
                 for i in range(0, len(articles), self.chunk_size)]
        
        self.logger.info(f"Processing {len(articles)} articles in {len(chunks)} chunks with {self.max_workers} workers")
        
        # Process chunks in parallel
        futures = []
        for chunk in chunks:
            future = self.executor.submit(self._process_article_chunk, chunk)
            futures.append(future)
        
        # Collect results
        for i, future in enumerate(futures):
            try:
                chunk_results, chunk_failed = future.result()
                results.extend(chunk_results)
                failed_articles.extend(chunk_failed)
                
                if callback:
                    callback(len(results), len(failed_articles), len(articles))
                    
            except Exception as e:
                self.logger.error(f"Failed to process chunk {i}: {e}")
                failed_articles.extend([{'title': 'Unknown', 'error': str(e)}])
        
        self.logger.info(f"Parallel processing completed: {len(results)} successful, {len(failed_articles)} failed")
        return results, failed_articles
    
    def _process_article_chunk(self, articles: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process a chunk of articles."""
        results = []
        failed = []
        
        for article in articles:
            try:
                title = article.get('title', '')
                content = self.get_article_content(title)
                
                if content:
                    results.append(content)
                else:
                    failed.append({'title': title, 'error': 'Failed to retrieve content'})
                    
            except Exception as e:
                failed.append({'title': article.get('title', 'Unknown'), 'error': str(e)})
        
        return results, failed
    
    def get_all_articles_streaming(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all Wikipedia articles using streaming approach to avoid memory issues."""
        articles = []
        
        try:
            # Use mwclient to get all pages with streaming
            for page in self.mwclient_site.allpages(namespace=0):  # Main namespace
                article = {
                    'title': page.name,
                    'pageid': page.pageid,
                    'ns': page.namespace
                }
                articles.append(article)
                
                if limit and len(articles) >= limit:
                    break
                
                # Log progress every 1000 articles
                if len(articles) % 1000 == 0:
                    self.logger.info(f"Retrieved {len(articles)} articles so far")
        
        except Exception as e:
            self.logger.error(f"Failed to get articles with mwclient: {e}")
            # Fallback to basic API
            self.logger.info("Falling back to basic API for article enumeration")
            return self._get_all_articles_fallback(limit)
        
        return articles
    
    def get_all_articles(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all Wikipedia articles using mwclient."""
        return self.get_all_articles_streaming(limit)
    
    def _get_all_articles_fallback(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fallback method for getting all articles using basic API."""
        articles = []
        gapcontinue = None
        
        while True:
            params = {
                'action': 'query',
                'generator': 'allpages',
                'gapnamespace': 0,  # Main namespace
                'gaplimit': 500,
                'format': 'json'
            }
            
            if gapcontinue:
                params['gapcontinue'] = gapcontinue
            
            response = self._make_request_with_rate_limit(params)
            if not response:
                break
            
            query = response.get('query', {})
            pages = query.get('pages', {})
            
            for page_id, page_data in pages.items():
                if page_id == '-1':  # Skip special pages
                    continue
                
                article = {
                    'title': page_data.get('title', ''),
                    'pageid': page_data.get('pageid', ''),
                    'ns': page_data.get('ns', 0)
                }
                articles.append(article)
                
                if limit and len(articles) >= limit:
                    return articles
            
            # Check for continuation
            if 'continue' in response:
                gapcontinue = response['continue'].get('gapcontinue')
                if not gapcontinue:
                    break
            else:
                break
        
        return articles
    
    def test_connection(self) -> bool:
        """Test the connection to MediaWiki using mwclient."""
        try:
            # Test basic site info using API call
            params = {
                'action': 'query',
                'meta': 'siteinfo',
                'format': 'json'
            }
            
            response = self._make_request_with_rate_limit(params)
            if response:
                site_info = response.get('query', {}).get('general', {})
                self.logger.info(f"Connected to {site_info.get('sitename', 'Unknown site')}")
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to connect with mwclient: {e}")
            return False
    
    def get_site_info(self) -> Dict[str, Any]:
        """Get detailed site information."""
        try:
            params = {
                'action': 'query',
                'meta': 'siteinfo',
                'format': 'json'
            }
            
            response = self._make_request_with_rate_limit(params)
            if response:
                site_info = response.get('query', {}).get('general', {})
                return {
                    'sitename': site_info.get('sitename'),
                    'base': site_info.get('base'),
                    'generator': site_info.get('generator'),
                    'phpversion': site_info.get('phpversion'),
                    'time': site_info.get('time')
                }
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"Failed to get site info: {e}")
            return {}
    
    def get_available_libraries(self) -> Dict[str, bool]:
        """Get information about available libraries."""
        return {
            'mwclient': MWCLIENT_AVAILABLE,
            'wikitextparser': WIKITEXTPARSER_AVAILABLE
        }
    
    def get_rate_limit_stats(self) -> Dict[str, Any]:
        """Get current rate limiting statistics."""
        return {
            'current_qps': self.rate_limiter.current_qps,
            'consecutive_429s': self.rate_limiter.consecutive_429s,
            'requests_in_window': len(self.rate_limiter.request_times),
            'max_workers': self.max_workers,
            'chunk_size': self.chunk_size
        }
    
    def shutdown(self):
        """Shutdown the client and cleanup resources."""
        if self.executor:
            self.executor.shutdown(wait=True) 