from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, expr, current_date, date_sub
import logging
from typing import List, Dict, Optional
import json

logger = logging.getLogger(__name__)

class DeltaTableMonitor:
    """Monitors Delta table events using system tables"""
    
    MONITORED_OPERATIONS = {
        'CREATE': 'Table Creation',
        'ALTER': 'Table Modification',
        'DROP': 'Table Deletion'
    }
    
    def __init__(self, spark: SparkSession, table_path: Optional[str] = None, 
                 catalog: Optional[str] = None, schema: Optional[str] = None,
                 operations_to_monitor: List[str] = None,
                 days_lookback: int = 2):
        self.spark = spark
        self.table_path = table_path
        self.catalog = catalog or self._get_current_catalog()
        self.schema = schema or self._get_current_schema()
        self.notification_handlers = []
        self.operations_to_monitor = operations_to_monitor or list(self.MONITORED_OPERATIONS.keys())
        self.days_lookback = days_lookback
        
    def _get_current_catalog(self) -> str:
        """Get current catalog name"""
        return self.spark.sql("SELECT current_catalog()").collect()[0][0]
        
    def _get_current_schema(self) -> str:
        """Get current schema name"""
        return self.spark.sql("SELECT current_database()").collect()[0][0]
        
    def add_notification_handler(self, handler):
        """Add a notification handler"""
        self.notification_handlers.append(handler)
        
    def _get_audit_events(self, since_timestamp):
        """Get relevant table events from information schema"""
        base_query = f"""
        SELECT DISTINCT
            table_catalog as catalog,
            table_schema as schema,
            table_name,
            created as timestamp,
            CASE 
                WHEN created >= date_sub(current_date(), {self.days_lookback}) THEN 'CREATE'
                ELSE 'ALTER'
            END as operation_type,
            table_type
        FROM system.information_schema.tables
        WHERE created >= date_sub(current_date(), {self.days_lookback})
        """
        
        if self.table_path:
            catalog, schema, table = self.table_path.split(".")
            base_query += f"""
            AND table_catalog = '{catalog}'
            AND table_schema = '{schema}'
            AND table_name = '{table}'
            """
        else:
            base_query += f"""
            AND table_catalog = '{self.catalog}'
            AND table_schema = '{self.schema}'
            """
            
        return self.spark.sql(base_query)
    
    def _get_operation_details(self, event) -> Dict[str, str]:
        """Extract detailed information from table event"""
        table_path = f"{event.catalog}.{event.schema}.{event.table_name}"
        
        details = {
            'operation_type': self.MONITORED_OPERATIONS.get(event.operation_type, event.operation_type),
            'timestamp': str(event.timestamp),
            'table': table_path,
            'table_type': event.table_type
        }
        
        # Get additional table details if available
        try:
            table_details = self.spark.sql(f"DESCRIBE DETAIL {table_path}").collect()[0]
            details.update({
                'format': table_details.format,
                'location': table_details.location,
                'partitioning': str(table_details.partitioning),
                'properties': str(table_details.properties)
            })
        except Exception as e:
            logger.warning(f"Could not get detailed information for {table_path}: {str(e)}")
            
        return details
        
    def monitor_table_changes(self, interval_seconds: int = 60):
        """Monitor table changes using information schema"""
        from time import sleep
        
        if self.table_path:
            logger.info(f"Starting monitoring for specific table: {self.table_path}")
        else:
            logger.info(f"Starting monitoring for all tables in {self.catalog}.{self.schema}")
            
        logger.info(f"Looking back {self.days_lookback} days for changes")
        
        while True:
            try:
                # Query information schema
                changes = self._get_audit_events(self.days_lookback)
                
                if changes.count() > 0:
                    self._notify_changes(changes)
                
                sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error monitoring tables: {str(e)}")
                raise
    
    def _notify_changes(self, changes):
        """Notify all handlers of changes with detailed information"""
        changes_list = changes.collect()
        detailed_changes = [self._get_operation_details(event) for event in changes_list]
        
        for handler in self.notification_handlers:
            handler.handle_change(detailed_changes) 