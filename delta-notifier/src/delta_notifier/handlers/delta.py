from .base import NotificationHandler
from typing import List, Dict
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import datetime

class DeltaHandler(NotificationHandler):
    """Writes table change events to a Delta table"""
    
    def __init__(self, spark: SparkSession, output_table: str):
        """
        Args:
            spark: SparkSession
            output_table: Full path to output Delta table (catalog.schema.table)
        """
        self.spark = spark
        self.output_table = output_table
        self._ensure_table_exists()
    
    def _get_schema(self) -> StructType:
        """Define schema for the audit table"""
        return StructType([
            StructField("operation_type", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("table_type", StringType(), True),
            StructField("format", StringType(), True),
            StructField("location", StringType(), True),
            StructField("partitioning", StringType(), True),
            StructField("properties", StringType(), True),
            StructField("recorded_at", TimestampType(), False)
        ])
    
    def _ensure_table_exists(self):
        """Create the output table if it doesn't exist"""
        try:
            self.spark.sql(f"SELECT 1 FROM {self.output_table} LIMIT 1")
        except:
            # Table doesn't exist, create it
            empty_df = self.spark.createDataFrame([], self._get_schema())
            empty_df.write.format("delta").saveAsTable(self.output_table)
            
            # Add table properties
            self.spark.sql(f"""
                ALTER TABLE {self.output_table}
                SET TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true',
                    'delta.enableChangeDataFeed' = 'true'
                )
            """)
    
    def handle_change(self, changes: List[Dict[str, str]]):
        """Write changes to Delta table"""
        if not changes:
            return
            
        # Convert changes to rows
        rows = []
        current_time = datetime.datetime.now()
        
        for change in changes:
            row = {
                "operation_type": change["operation_type"],
                "table_name": change["table"],
                "event_timestamp": datetime.datetime.fromisoformat(change["timestamp"].replace('Z', '+00:00')),
                "table_type": change.get("table_type"),
                "format": change.get("format"),
                "location": change.get("location"),
                "partitioning": change.get("partitioning"),
                "properties": change.get("properties"),
                "recorded_at": current_time
            }
            rows.append(row)
        
        # Create DataFrame and write to Delta table
        df = self.spark.createDataFrame(rows, self._get_schema())
        
        # Write using merge to handle duplicates
        df.createOrReplaceTempView("new_changes")
        
        self.spark.sql(f"""
            MERGE INTO {self.output_table} target
            USING new_changes source
            ON target.table_name = source.table_name
            AND target.event_timestamp = source.event_timestamp
            AND target.operation_type = source.operation_type
            WHEN NOT MATCHED THEN INSERT *
        """) 