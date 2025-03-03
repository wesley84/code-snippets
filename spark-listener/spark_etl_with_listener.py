from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import time

def track_stage_time(func):
    """Decorator to track execution time of stages
    
    This decorator wraps ETL stage functions to measure and print their execution time.
    It helps monitor the performance of each processing stage.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Stage {func.__name__} completed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

class ETLStageTracker:
    """Tracks Spark stage execution using the Spark status tracker API
    
    This class provides utilities to monitor Spark stage execution status
    and wait for stage completion before proceeding.
    """
    def __init__(self, spark):
        self.spark = spark
        self.tracker = spark.sparkContext.statusTracker()
    
    def wait_for_stage(self, stage_id):
        """Blocks until a specific Spark stage completes
        
        Args:
            stage_id: ID of the Spark stage to monitor
        """
        # Wait for stage to be registered
        while not self.tracker.getStageInfo(stage_id):
            time.sleep(0.1)
        
        # Wait for stage to complete
        while any(s.stageId == stage_id for s in self.tracker.getActiveStages()):
            time.sleep(0.1)
            
    def get_current_stages(self):
        """Get currently active stage IDs"""
        return self.tracker.getActiveStageIds()

@track_stage_time
def read_stage(spark, stage_tracker):
    """First ETL stage: Data ingestion
    
    Reads CSV data from a specified path with schema inference.
    Repartitions the data into 10 partitions for better parallelism.
    """
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/databricks-datasets/iot-stream/data-user/") \
        .repartition(10)
    
    # Force action to get stages
    df.cache()
    df.count()
    
    # Wait for all active stages
    for stage_id in stage_tracker.get_current_stages():
        print(f"Waiting for read stage {stage_id}")
        stage_tracker.wait_for_stage(stage_id)
        
    return df

@track_stage_time
def transform_stage(df, stage_tracker):
    """Second ETL stage: Data transformation
    
    Filters records where age > 30 and caches the result.
    Forces evaluation by counting records to ensure caching occurs.
    """
    filtered_df = df.filter(col("age") > 30).cache()
    
    # Force evaluation and get stages
    count = filtered_df.count()
    print(f"Number of records after filtering: {count}")
    
    # Wait for all active stages
    for stage_id in stage_tracker.get_current_stages():
        print(f"Waiting for transform stage {stage_id}")
        stage_tracker.wait_for_stage(stage_id)
        
    return filtered_df

@track_stage_time
def write_stage(df, stage_tracker):
    """Final ETL stage: Data persistence
    
    Writes the transformed data to a Delta table, overwriting existing data.
    """
    df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("adb_cb6l9m_workspace.`demo-dataset`.tracker_users_listener")
        
    # Wait for all active stages
    for stage_id in stage_tracker.get_current_stages():
        print(f"Waiting for write stage {stage_id}")
        stage_tracker.wait_for_stage(stage_id)

def run_etl():
    """Main ETL orchestration function
    
    Executes the ETL pipeline in stages:
    1. Read data from CSV
    2. Transform and filter the data
    3. Write results to Delta table
    
    Includes error handling but spark.stop() is currently commented out
    """
    
    # Initialize stage tracker
    stage_tracker = ETLStageTracker(spark)
    
    try:
        # Execute stages
        print("Starting read stage...")
        df = read_stage(spark, stage_tracker)
        
        print("Starting transform stage...")
        filtered_df = transform_stage(df, stage_tracker)
        
        print("Starting write stage...")
        write_stage(filtered_df, stage_tracker)
        
        print("ETL pipeline completed successfully")
        
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        raise
    #finally:
        # Clean up
        #spark.stop()

if __name__ == "__main__":
    run_etl() 