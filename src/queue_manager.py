import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

class QueueManager:
    def __init__(self, queue_file: str = "queue/article_queue.json", enable_persistence: bool = True):
        self.queue_file = queue_file
        self.enable_persistence = enable_persistence
        self.queue: List[Dict[str, Any]] = []
        self.processed: List[str] = []
        self.failed: List[str] = []
        
        # Create queue directory if it doesn't exist
        os.makedirs(os.path.dirname(queue_file), exist_ok=True)
        
        self._load_queue()
    
    def _load_queue(self):
        """Load queue from JSON file if it exists."""
        if os.path.exists(self.queue_file):
            try:
                with open(self.queue_file, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    self.queue = data.get('queue', [])
                    self.processed = data.get('processed', [])
                    self.failed = data.get('failed', [])
            except Exception as e:
                print(f"Failed to load queue from {self.queue_file}: {e}")
                self.queue = []
                self.processed = []
                self.failed = []
    
    def _save_queue(self):
        """Save queue to JSON file."""
        if not self.enable_persistence:
            return
        
        try:
            data = {
                'queue': self.queue,
                'processed': self.processed,
                'failed': self.failed,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.queue_file, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Failed to save queue to {self.queue_file}: {e}")
    
    def add_articles(self, articles: List[Dict[str, Any]]):
        """Add articles to the queue."""
        for article in articles:
            if article not in self.queue and article.get('title') not in self.processed:
                self.queue.append(article)
        
        self._save_queue()
    
    def add_article(self, article: Dict[str, Any]):
        """Add a single article to the queue."""
        if article not in self.queue and article.get('title') not in self.processed:
            self.queue.append(article)
            self._save_queue()
    
    def get_next_article(self) -> Optional[Dict[str, Any]]:
        """Get the next article from the queue."""
        if self.queue:
            article = self.queue.pop(0)
            self._save_queue()
            return article
        return None
    
    def mark_processed(self, title: str):
        """Mark an article as processed."""
        if title not in self.processed:
            self.processed.append(title)
            self._save_queue()
    
    def mark_failed(self, title: str, error: str = ""):
        """Mark an article as failed."""
        if title not in self.failed:
            self.failed.append(title)
            self._save_queue()
    
    def get_queue_size(self) -> int:
        """Get the current queue size."""
        return len(self.queue)
    
    def get_processed_count(self) -> int:
        """Get the number of processed articles."""
        return len(self.processed)
    
    def get_failed_count(self) -> int:
        """Get the number of failed articles."""
        return len(self.failed)
    
    def get_total_count(self) -> int:
        """Get the total number of articles (queue + processed + failed)."""
        return len(self.queue) + len(self.processed) + len(self.failed)
    
    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return len(self.queue) == 0
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status."""
        return {
            'queue_size': self.get_queue_size(),
            'processed_count': self.get_processed_count(),
            'failed_count': self.get_failed_count(),
            'total_count': self.get_total_count(),
            'is_empty': self.is_empty()
        }
    
    def clear_queue(self):
        """Clear the queue (for testing or reset)."""
        self.queue = []
        self._save_queue()
    
    def reset_all(self):
        """Reset all queues (for testing or complete restart)."""
        self.queue = []
        self.processed = []
        self.failed = []
        self._save_queue() 