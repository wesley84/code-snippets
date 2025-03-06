from .base import NotificationHandler
from pyspark.sql import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class EmailNotificationHandler(NotificationHandler):
    """Sends notifications via email"""
    
    def __init__(self, smtp_config: dict, recipients: list):
        self.smtp_config = smtp_config
        self.recipients = recipients
        
    def handle_change(self, changes: DataFrame):
        """Send change notification via email"""
        
        # Format message
        changes_list = changes.collect()
        message = self._format_message(changes_list)
        
        # Send email
        self._send_email(message)
        
    def _format_message(self, changes) -> str:
        """Format changes into email message"""
        message = "Delta Table Changes Detected\n\n"
        
        for change in changes:
            message += f"Version {change.version}:\n"
            message += f"Operation: {change.operation}\n"
            message += f"Timestamp: {change.timestamp}\n"
            message += f"User: {change.userName}\n"
            if change.operationParameters:
                message += f"Details: {change.operationParameters}\n"
            message += "\n"
            
        return message
        
    def _send_email(self, message: str):
        """Send email via SMTP"""
        msg = MIMEMultipart()
        msg["Subject"] = "Delta Table Change Notification"
        msg["From"] = self.smtp_config["sender"]
        msg["To"] = ", ".join(self.recipients)
        
        msg.attach(MIMEText(message, "plain"))
        
        with smtplib.SMTP(self.smtp_config["host"], self.smtp_config["port"]) as server:
            if self.smtp_config.get("use_tls"):
                server.starttls()
            if self.smtp_config.get("username"):
                server.login(
                    self.smtp_config["username"],
                    self.smtp_config["password"]
                )
            server.send_message(msg) 