import json
import os
from typing import Dict, Any, Optional
from datetime import datetime

class CheckpointManager:
    def __init__(self, checkpoint_file: str = "checkpoints/progress.json"):
        self.checkpoint_file = checkpoint_file
        self.checkpoint_data = {
            'last_processed_index': 0,
            'last_batch_number': 0,
            'total_processed': 0,
            'total_failed': 0,
            'last_updated': None,
            'start_time': None,
            'status': 'idle'
        }
        
        # Create checkpoints directory if it doesn't exist
        os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
        
        self._load_checkpoint()
    
    def _load_checkpoint(self):
        """Load checkpoint from JSON file if it exists."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    self.checkpoint_data.update(data)
            except Exception as e:
                print(f"Failed to load checkpoint from {self.checkpoint_file}: {e}")
                # Keep default values
        else:
            # Initialize with start time
            self.checkpoint_data['start_time'] = datetime.now().isoformat()
            self._save_checkpoint()
    
    def _save_checkpoint(self):
        """Save checkpoint to JSON file."""
        try:
            self.checkpoint_data['last_updated'] = datetime.now().isoformat()
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as file:
                json.dump(self.checkpoint_data, file, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Failed to save checkpoint to {self.checkpoint_file}: {e}")
    
    def start_processing(self):
        """Mark the start of processing."""
        self.checkpoint_data['status'] = 'processing'
        self.checkpoint_data['start_time'] = datetime.now().isoformat()
        self._save_checkpoint()
    
    def finish_processing(self):
        """Mark the end of processing."""
        self.checkpoint_data['status'] = 'completed'
        self._save_checkpoint()
    
    def update_progress(self, processed_index: int, batch_number: int, total_processed: int, total_failed: int):
        """Update processing progress."""
        self.checkpoint_data['last_processed_index'] = processed_index
        self.checkpoint_data['last_batch_number'] = batch_number
        self.checkpoint_data['total_processed'] = total_processed
        self.checkpoint_data['total_failed'] = total_failed
        self._save_checkpoint()
    
    def get_last_processed_index(self) -> int:
        """Get the last processed article index."""
        return self.checkpoint_data.get('last_processed_index', 0)
    
    def get_last_batch_number(self) -> int:
        """Get the last processed batch number."""
        return self.checkpoint_data.get('last_batch_number', 0)
    
    def get_total_processed(self) -> int:
        """Get the total number of processed articles."""
        return self.checkpoint_data.get('total_processed', 0)
    
    def get_total_failed(self) -> int:
        """Get the total number of failed articles."""
        return self.checkpoint_data.get('total_failed', 0)
    
    def get_status(self) -> str:
        """Get the current processing status."""
        return self.checkpoint_data.get('status', 'idle')
    
    def get_start_time(self) -> Optional[str]:
        """Get the processing start time."""
        return self.checkpoint_data.get('start_time')
    
    def get_last_updated(self) -> Optional[str]:
        """Get the last update time."""
        return self.checkpoint_data.get('last_updated')
    
    def can_resume(self) -> bool:
        """Check if processing can be resumed."""
        return self.get_status() == 'processing' and self.get_total_processed() > 0
    
    def get_resume_info(self) -> Dict[str, Any]:
        """Get information needed to resume processing."""
        return {
            'last_processed_index': self.get_last_processed_index(),
            'last_batch_number': self.get_last_batch_number(),
            'total_processed': self.get_total_processed(),
            'total_failed': self.get_total_failed(),
            'can_resume': self.can_resume()
        }
    
    def reset_checkpoint(self):
        """Reset checkpoint to initial state."""
        self.checkpoint_data = {
            'last_processed_index': 0,
            'last_batch_number': 0,
            'total_processed': 0,
            'total_failed': 0,
            'last_updated': datetime.now().isoformat(),
            'start_time': datetime.now().isoformat(),
            'status': 'idle'
        }
        self._save_checkpoint()
    
    def get_processing_time(self) -> Optional[float]:
        """Get the total processing time in seconds."""
        start_time = self.get_start_time()
        if not start_time:
            return None
        
        try:
            start_dt = datetime.fromisoformat(start_time)
            if self.get_status() == 'completed':
                end_dt = datetime.fromisoformat(self.get_last_updated())
            else:
                end_dt = datetime.now()
            
            return (end_dt - start_dt).total_seconds()
        except Exception:
            return None
    
    def get_progress_percentage(self, total_articles: int) -> float:
        """Get the progress percentage."""
        if total_articles == 0:
            return 0.0
        
        processed = self.get_total_processed()
        return (processed / total_articles) * 100
    
    def get_checkpoint_summary(self) -> Dict[str, Any]:
        """Get a summary of the checkpoint data."""
        processing_time = self.get_processing_time()
        
        return {
            'status': self.get_status(),
            'last_processed_index': self.get_last_processed_index(),
            'last_batch_number': self.get_last_batch_number(),
            'total_processed': self.get_total_processed(),
            'total_failed': self.get_total_failed(),
            'start_time': self.get_start_time(),
            'last_updated': self.get_last_updated(),
            'processing_time_seconds': processing_time,
            'can_resume': self.can_resume()
        } 