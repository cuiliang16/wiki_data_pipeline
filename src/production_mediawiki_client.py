import requests
import time
import urllib.parse
from typing import Dict, Any, List, Optional, Literal
import logging

try:
    import mwclient
    import wikitextparser
    MWCLIENT_AVAILABLE = True
    WIKITEXTPARSER_AVAILABLE = True
except ImportError as e:
    MWCLIENT_AVAILABLE = False
    WIKITEXTPARSER_AVAILABLE = False
    print(f"Warning: Required libraries not available: {e}")

class ProductionMediaWikiClient:
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.base_url = config.get('base_url', 'https://en.wikipedia.org/w/api.php')
        self.user_agent = config.get('user_agent', 'WikipediaDataPipeline/1.0')
        self.max_retries = config.get('max_retries', 3)
        self.initial_delay = config.get('initial_delay', 1)
        self.max_delay = config.get('max_delay', 60)
        
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
    
    def _make_request(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make API request with retry logic and rate limiting (fallback method)."""
        for attempt in range(self.max_retries + 1):
            try:
                start_time = time.time()
                response = self.session.get(self.base_url, params=params, timeout=30)
                response_time = time.time() - start_time
                
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
        
        response = self._make_request(content_params)
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
    
    def get_all_articles(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all Wikipedia articles using mwclient."""
        articles = []
        
        try:
            # Use mwclient to get all pages
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
            
            response = self._make_request(params)
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
            
            response = self._make_request(params)
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
            
            response = self._make_request(params)
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