import argparse
import json
import logging
from pyspark.sql import SparkSession
from delta_notifier.table_monitor import DeltaTableMonitor
from delta_notifier.handlers.slack import SlackNotificationHandler
from delta_notifier.handlers.email import EmailNotificationHandler
from delta_notifier.handlers.delta import DeltaHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder.getOrCreate()
    
    parser = argparse.ArgumentParser(description='Monitor Delta table changes')
    parser.add_argument('--table_path', help='Full path to Delta table (optional)')
    parser.add_argument('--catalog', help='Catalog to monitor (if no table_path)')
    parser.add_argument('--schema', help='Schema to monitor (if no table_path)')
    parser.add_argument('--days_lookback', type=int, default=2, help='Number of days to look back for changes')
    parser.add_argument('--operations_to_monitor', type=str, help='JSON array of operations to monitor')
    parser.add_argument('--notification_config', type=str, help='JSON notification configuration')
    parser.add_argument('--output_table', default='adb_cb6l9m_workspace.main.table_changes', 
                       help='Delta table to store change events')
    
    args = parser.parse_args()
    
    # Parse JSON configs
    operations = json.loads(args.operations_to_monitor) if args.operations_to_monitor else None
    notification_config = json.loads(args.notification_config) if args.notification_config else {}
    
    # Create monitor
    monitor = DeltaTableMonitor(
        spark=spark,
        table_path=args.table_path,
        catalog=args.catalog,
        schema=args.schema,
        operations_to_monitor=operations,
        days_lookback=args.days_lookback
    )
    
    # Add Delta handler as default
    monitor.add_notification_handler(
        DeltaHandler(spark, args.output_table)
    )
    
    # Add optional notification handlers
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
    monitor.monitor_table_changes()

if __name__ == "__main__":
    main() 