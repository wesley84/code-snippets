from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, expr
import logging
from typing import List, Dict
import json

logger = logging.getLogger(__name__)

class DeltaTableMonitor:
    """Monitors Delta table events using system tables"""
    
    MONITORED_OPERATIONS = {
        'CREATE_TABLE': 'Table Creation',
        'ALTER_TABLE': 'Table Alteration',
        'CREATE_TABLE_AS_SELECT': 'Table Creation from Query',
        'MODIFY_TABLE_PROPERTIES': 'Table Properties Modified',
        'ADD_COLUMNS': 'Columns Added',
        'DROP_COLUMNS': 'Columns Dropped',
        'UPDATE_COLUMNS': 'Columns Updated'
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
        """Monitor table changes using system.access.audit table"""
        from time import sleep
        
        logger.info(f"Starting monitoring for table {self.table_path}")
        logger.info(f"Monitoring operations: {', '.join(self.operations_to_monitor)}")
        
        # Get initial timestamp
        last_check = current_timestamp()
        
        while True:
            try:
                # Query system audit logs
                changes = self._get_audit_events(last_check)
                
                if changes.count() > 0:
                    self._notify_changes(changes)
                
                last_check = current_timestamp()
                sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error monitoring table: {str(e)}")
                raise
    
    def _get_audit_events(self, since_timestamp):
        """Get relevant audit events from system tables"""
        return (
            self.spark.table("system.access.audit")
            .filter(
                (col("timestamp") > since_timestamp) &
                (col("actionName").isin(self.operations_to_monitor)) &
                (col("fullName") == self.table_path)
            )
            .select(
                "timestamp",
                "actionName",
                "userName",
                "requestParams",
                "response"
            )
        )
    
    def _get_operation_details(self, event) -> Dict[str, str]:
        """Extract detailed information from audit event"""
        details = {
            'operation_type': self.MONITORED_OPERATIONS.get(event.actionName, event.actionName),
            'timestamp': str(event.timestamp),
            'user': event.userName,
        }
        
        # Parse request parameters
        if event.requestParams:
            try:
                params = json.loads(event.requestParams)
                if event.actionName == 'CREATE_TABLE':
                    details['schema'] = params.get('schema')
                    details['properties'] = str(params.get('properties', {}))
                elif event.actionName == 'ALTER_TABLE':
                    details['changes'] = str(params.get('changes', []))
                elif 'COLUMNS' in event.actionName:
                    details['column_details'] = str(params.get('columns', []))
            except:
                details['raw_params'] = event.requestParams
                
        return details
        
    def _notify_changes(self, changes):
        """Notify all handlers of changes with detailed information"""
        changes_list = changes.collect()
        detailed_changes = [self._get_operation_details(event) for event in changes_list]
        
        for handler in self.notification_handlers:
            handler.handle_change(detailed_changes) 