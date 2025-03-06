from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class DeltaTableMonitor:
    """Monitors Delta table events and triggers notifications"""
    
    # Define operations to monitor
    MONITORED_OPERATIONS = {
        'CREATE': 'Table Creation',
        'ALTER': 'Table Alteration',
        'ADD PARTITION': 'Partition Added',
        'DROP PARTITION': 'Partition Dropped',
        'MERGE': 'Table Merge',
        'UPDATE': 'Data Update',
        'DELETE': 'Data Deletion'
    }
    
    def __init__(self, spark: SparkSession, table_path: str, operations_to_monitor: List[str] = None):
        self.spark = spark
        self.table_path = table_path
        self.notification_handlers = []
        self.operations_to_monitor = operations_to_monitor or list(self.MONITORED_OPERATIONS.keys())
        
    def add_notification_handler(self, handler):
        """Add a notification handler"""
        self.notification_handlers.append(handler)
        
    def monitor_table_changes(self, interval_seconds: int = 60):
        """Monitor table for specific changes using Delta table history"""
        from time import sleep
        
        # Get initial version
        last_version = self._get_current_version()
        logger.info(f"Starting monitoring for table {self.table_path}")
        logger.info(f"Monitoring operations: {', '.join(self.operations_to_monitor)}")
        
        while True:
            try:
                current_version = self._get_current_version()
                
                if current_version > last_version:
                    # Get relevant changes since last check
                    changes = self._get_filtered_changes(last_version, current_version)
                    
                    if not changes.isEmpty():
                        self._notify_changes(changes)
                    last_version = current_version
                    
                sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error monitoring table: {str(e)}")
                raise
                
    def _get_current_version(self) -> int:
        """Get current version of Delta table"""
        return self.spark.sql(f"DESCRIBE HISTORY {self.table_path}") \
            .select("version").first()[0]
            
    def _get_filtered_changes(self, from_version: int, to_version: int):
        """Get filtered changes between versions"""
        history = self.spark.sql(f"""
            DESCRIBE HISTORY {self.table_path} 
            WHERE version > {from_version} 
            AND version <= {to_version}
            ORDER BY version
        """)
        
        # Filter for monitored operations
        filtered = history.filter(
            col("operation").isin(self.operations_to_monitor)
        )
        
        return filtered
    
    def _get_operation_details(self, change) -> Dict[str, str]:
        """Extract detailed information about the operation"""
        details = {
            'operation_type': self.MONITORED_OPERATIONS.get(change.operation, change.operation),
            'timestamp': str(change.timestamp),
            'user': change.userName,
            'version': str(change.version)
        }
        
        # Add operation-specific details
        if change.operationParameters:
            params = change.operationParameters
            if change.operation == 'CREATE':
                details['schema'] = params.get('schema', 'N/A')
                details['properties'] = params.get('properties', 'N/A')
            elif change.operation == 'ALTER':
                details['changes'] = params.get('alterations', 'N/A')
            elif 'PARTITION' in change.operation:
                details['partition_details'] = params.get('partition_values', 'N/A')
                
        return details
        
    def _notify_changes(self, changes):
        """Notify all handlers of changes with detailed information"""
        changes_list = changes.collect()
        detailed_changes = [self._get_operation_details(change) for change in changes_list]
        
        for handler in self.notification_handlers:
            handler.handle_change(detailed_changes) 