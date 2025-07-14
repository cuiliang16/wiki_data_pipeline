import requests
import time
import urllib.parse
from typing import Dict, Any, List, Optional
import logging

class MediaWikiClient:
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger
        self.base_url = config.get('base_url', 'https://en.wikipedia.org/w/api.php')
        self.user_agent = config.get('user_agent', 'WikipediaDataPipeline/1.0')
        self.max_retries = config.get('max_retries', 3)
        self.initial_delay = config.get('initial_delay', 1)
        self.max_delay = config.get('max_delay', 60)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent
        })
    
    def _make_request(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make API request with retry logic and rate limiting."""
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
    
    def get_all_articles(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all Wikipedia articles using the generator API."""
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
    
    def get_article_content(self, title: str) -> Optional[Dict[str, Any]]:
        """Get full article content including categories."""
        # First, get the article content
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
            'categories': categories,
            'author': None,  # Wikipedia doesn't have traditional authors
            'pageid': page_data.get('pageid', '')
        }
    
    def get_article_html(self, title: str) -> Optional[Dict[str, Any]]:
        """Get article content in HTML format."""
        # Get HTML content using the parse action
        html_params = {
            'action': 'parse',
            'page': title,
            'prop': 'text|categories',
            'format': 'json'
        }
        
        response = self._make_request(html_params)
        if not response:
            return None
        
        parse = response.get('parse', {})
        if not parse:
            return None
        
        # Check if it's a redirect
        if 'redirects' in parse:
            return None
        
        html_content = parse.get('text', {}).get('*', '')
        categories = []
        
        for category in parse.get('categories', []):
            cat_name = category.get('*', '')
            if cat_name:
                categories.append(cat_name)
        
        # Construct Wikipedia URL
        url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(title)}"
        
        return {
            'title': title,
            'url': url,
            'content': html_content,
            'categories': categories,
            'author': None,
            'pageid': parse.get('pageid', '')
        }
    
    def test_connection(self) -> bool:
        """Test the connection to MediaWiki API."""
        params = {
            'action': 'query',
            'meta': 'siteinfo',
            'format': 'json'
        }
        
        response = self._make_request(params)
        return response is not None 