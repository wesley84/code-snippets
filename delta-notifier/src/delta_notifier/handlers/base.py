from abc import ABC, abstractmethod
from typing import List, Dict

class NotificationHandler(ABC):
    """Base class for notification handlers"""
    
    @abstractmethod
    def handle_change(self, changes: List[Dict[str, str]]):
        """Handle table changes
        
        Args:
            changes: List of dictionaries containing change details
        """
        pass 