from .base import NotificationHandler
from typing import List, Dict
import requests
import json

class SlackNotificationHandler(NotificationHandler):
    """Sends notifications to Slack"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        
    def handle_change(self, changes: List[Dict[str, str]]):
        """Send change notification to Slack"""
        message = self._format_message(changes)
        self._send_to_slack(message)
        
    def _format_message(self, changes: List[Dict[str, str]]) -> str:
        """Format changes into Slack message"""
        message = "🔔 *Delta Table Changes Detected*\n\n"
        
        for change in changes:
            message += f"• *{change['operation_type']}*\n"
            message += f"  - Version: {change['version']}\n"
            message += f"  - Time: {change['timestamp']}\n"
            message += f"  - User: {change['user']}\n"
            
            # Add operation-specific details
            if 'schema' in change:
                message += f"  - Schema: {change['schema']}\n"
            if 'changes' in change:
                message += f"  - Changes: {change['changes']}\n"
            if 'partition_details' in change:
                message += f"  - Partition Details: {change['partition_details']}\n"
            
            message += "\n"
            
        return message
        
    def _send_to_slack(self, message: str):
        """Send message to Slack webhook"""
        payload = {"text": message}
        response = requests.post(
            self.webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status() 