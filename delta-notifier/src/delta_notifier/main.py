import argparse
import json
import logging
from pyspark.sql import SparkSession
from delta_notifier.table_monitor import DeltaTableMonitor
from delta_notifier.handlers.slack import SlackNotificationHandler
from delta_notifier.handlers.email import EmailNotificationHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Monitor Delta table changes')
    parser.add_argument('--table_path', required=True, help='Full path to Delta table')
    parser.add_argument('--operations_to_monitor', type=str, help='JSON array of operations to monitor')
    parser.add_argument('--notification_config', type=str, help='JSON notification configuration')
    
    args = parser.parse_args()
    
    # Parse JSON configs
    operations = json.loads(args.operations_to_monitor) if args.operations_to_monitor else None
    notification_config = json.loads(args.notification_config) if args.notification_config else {}
    
    # Create monitor
    monitor = DeltaTableMonitor(
        spark=spark,
        table_path=args.table_path,
        operations_to_monitor=operations
    )
    
    # Add notification handlers
    if 'slack' in notification_config:
        slack_config = notification_config['slack']
        monitor.add_notification_handler(
            SlackNotificationHandler(webhook_url=slack_config['webhook_url'])
        )
        
    if 'email' in notification_config:
        email_config = notification_config['email']
        monitor.add_notification_handler(
            EmailNotificationHandler(
                smtp_config=email_config['smtp'],
                recipients=email_config['recipients']
            )
        )
    
    # Start monitoring
    logger.info(f"Starting monitoring for table: {args.table_path}")
    monitor.monitor_table_changes()

if __name__ == "__main__":
    main() 