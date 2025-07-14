import json
from typing import Dict, Any, List, Optional

class DataProcessor:
    def __init__(self):
        pass
    
    def process_article(self, article_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process article data into the required JSON format."""
        try:
            # Extract required fields
            title = article_data.get('title', '')
            url = article_data.get('url', '')
            content = article_data.get('content', '')
            categories = article_data.get('categories', [])
            author = article_data.get('author', None)  # Always None for Wikipedia
            pageid = article_data.get('pageid', '')
            
            # Create the processed article structure
            processed_article = {
                'title': title,
                'url': url,
                'content': content,
                'categories': categories,
                'author': author,
                'pageid': pageid,
                'metadata': {
                    'content_length': len(content),
                    'category_count': len(categories),
                    'processing_timestamp': self._get_current_timestamp()
                }
            }
            
            return processed_article
            
        except Exception as e:
            # Return a minimal structure if processing fails
            return {
                'title': article_data.get('title', 'Unknown'),
                'url': article_data.get('url', ''),
                'content': '',
                'categories': [],
                'author': None,
                'pageid': article_data.get('pageid', ''),
                'error': str(e)
            }
    
    def validate_article(self, article_data: Dict[str, Any]) -> bool:
        """Validate that article data has required fields."""
        required_fields = ['title', 'url', 'content']
        
        for field in required_fields:
            if not article_data.get(field):
                return False
        
        return True
    
    def clean_content(self, content: str) -> str:
        """Clean and normalize article content."""
        if not content:
            return ''
        
        # Remove excessive whitespace
        content = ' '.join(content.split())
        
        return content
    
    def extract_categories(self, categories: List[str]) -> List[str]:
        """Extract and clean category names."""
        cleaned_categories = []
        
        for category in categories:
            if category:
                # Remove 'Category:' prefix if present
                if category.startswith('Category:'):
                    category = category[9:]
                
                # Clean category name
                category = category.strip()
                if category:
                    cleaned_categories.append(category)
        
        return cleaned_categories
    
    def format_for_storage(self, article_data: Dict[str, Any]) -> str:
        """Format article data as JSON string for storage."""
        try:
            # Process the article
            processed_article = self.process_article(article_data)
            
            # Convert to JSON with proper formatting
            json_data = json.dumps(processed_article, ensure_ascii=False, indent=2)
            
            return json_data
            
        except Exception as e:
            # Return error JSON if formatting fails
            error_data = {
                'title': article_data.get('title', 'Unknown'),
                'error': f'Failed to format article: {str(e)}',
                'original_data': article_data
            }
            return json.dumps(error_data, ensure_ascii=False, indent=2)
    
    def batch_process(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of articles."""
        processed_articles = []
        
        for article in articles:
            processed_article = self.process_article(article)
            if self.validate_article(processed_article):
                processed_articles.append(processed_article)
        
        return processed_articles
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def create_summary(self, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a summary of processed articles."""
        total_articles = len(articles)
        total_content_length = sum(len(article.get('content', '')) for article in articles)
        total_categories = sum(len(article.get('categories', [])) for article in articles)
        
        # Count unique categories
        all_categories = []
        for article in articles:
            all_categories.extend(article.get('categories', []))
        unique_categories = len(set(all_categories))
        
        return {
            'total_articles': total_articles,
            'total_content_length': total_content_length,
            'average_content_length': total_content_length / total_articles if total_articles > 0 else 0,
            'total_categories': total_categories,
            'unique_categories': unique_categories,
            'average_categories_per_article': total_categories / total_articles if total_articles > 0 else 0
        } 