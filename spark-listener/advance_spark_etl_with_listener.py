from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import threading
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
        self.stage_times = {}  # Store stage timing information
    
    def wait_for_stage(self, stage_id):
        """Blocks until a specific Spark stage completes
        
        Args:
            stage_id: ID of the Spark stage to monitor
        """
        stage_start = time.time()
        
        # Wait for stage to be registered
        while not self.tracker.getStageInfo(stage_id):
            time.sleep(0.1)
        
        # Get initial metrics
        stage_info = self.tracker.getStageInfo(stage_id)
        total_tasks = stage_info.numTasks
        print(f"Stage {stage_id} started with {total_tasks} tasks")
        
        # Wait for stage to complete while monitoring progress
        last_progress = 0
        while True:
            stage_info = self.tracker.getStageInfo(stage_id)
            if not stage_info:
                continue
                
            completed_tasks = stage_info.numCompletedTasks
            if completed_tasks > last_progress:
                elapsed = time.time() - stage_start
                print(f"Stage {stage_id}: {completed_tasks}/{total_tasks} tasks completed in {elapsed:.2f}s")
                last_progress = completed_tasks
            
            if not any(s == stage_id for s in self.tracker.getActiveStageIds()):
                break
            time.sleep(0.1)
        
        # Record final timing
        stage_end = time.time()
        duration = stage_end - stage_start
        
        # Store stage metrics
        self.stage_times[stage_id] = {
            'start_time': stage_start,
            'end_time': stage_end,
            'duration': duration,
            'total_tasks': total_tasks,
            'completed_tasks': stage_info.numCompletedTasks,
            'failed_tasks': stage_info.numFailedTasks
        }
        
        print(f"Stage {stage_id} completed in {duration:.2f} seconds")
            
    def get_current_stages(self):
        """Get currently active stage IDs"""
        return self.tracker.getActiveStageIds()
    
    def print_stage_summary(self):
        """Print summary of all stage timings"""
        print("\nStage Execution Summary:")
        print("-" * 60)
        total_duration = 0
        
        for stage_id, metrics in sorted(self.stage_times.items()):
            print(f"Stage {stage_id}:")
            print(f"  Duration: {metrics['duration']:.2f}s")
            print(f"  Tasks: {metrics['completed_tasks']}/{metrics['total_tasks']} completed")
            if metrics['failed_tasks'] > 0:
                print(f"  Failed Tasks: {metrics['failed_tasks']}")
            print(f"  Start Time: {datetime.fromtimestamp(metrics['start_time']).strftime('%H:%M:%S')}")
            print(f"  End Time: {datetime.fromtimestamp(metrics['end_time']).strftime('%H:%M:%S')}")
            print()
            total_duration += metrics['duration']
            
        print(f"Total Stage Execution Time: {total_duration:.2f} seconds")
        print("-" * 60)

@track_stage_time
def read_stage(spark, stage_tracker):
    """First ETL stage: Data ingestion
    
    Reads CSV data from a specified path with schema inference.
    Repartitions the data into 10 partitions for better parallelism.
    """
    def async_read():
        return spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("/databricks-datasets/iot-stream/data-user/") \
            .repartition(10)
    
    # Start read operation in background
    df = async_read()
    
    # Start action in background thread
    def trigger_action():
        df.cache()
        count = df.count()
        print(f"Read {count} records")
    
    action_thread = threading.Thread(target=trigger_action)
    action_thread.start()
    
    # Monitor stages while action executes
    while action_thread.is_alive():
        active_stages = stage_tracker.tracker.getActiveStageIds()
        if active_stages:
            for stage in active_stages:
                print(f"Monitoring read stage {stage}")
                stage_tracker.wait_for_stage(stage)
        time.sleep(0.1)
    
    action_thread.join()
    return df

@track_stage_time
def transform_stage(df, stage_tracker):
    """Second ETL stage: Data transformation
    
    Filters records where age > 30 and caches the result.
    Forces evaluation by counting records to ensure caching occurs.
    """
    def async_transform():
        return df.filter(col("age") > 30).cache()
    
    # Start transform in background
    filtered_df = async_transform()
    
    def trigger_action():
        count = filtered_df.count()
        print(f"Transformed {count} records")
    
    action_thread = threading.Thread(target=trigger_action)
    action_thread.start()
    
    # Monitor stages while transform executes
    while action_thread.is_alive():
        active_stages = stage_tracker.tracker.getActiveStageIds()
        if active_stages:
            for stage in active_stages:
                print(f"Monitoring transform stage {stage}")
                stage_tracker.wait_for_stage(stage)
        time.sleep(0.1)
    
    action_thread.join()
    return filtered_df

@track_stage_time
def write_stage(df, stage_tracker):
    """Final ETL stage: Data persistence
    
    Writes the transformed data to a Delta table, overwriting existing data.
    """
    def async_write():
        df.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable("adb_cb6l9m_workspace.`demo-dataset`.tracker_users_listener")
    
    # Start write in background
    write_thread = threading.Thread(target=async_write)
    write_thread.start()
    
    # Monitor stages while write executes
    while write_thread.is_alive():
        active_stages = stage_tracker.tracker.getActiveStageIds()
        if active_stages:
            for stage in active_stages:
                print(f"Monitoring write stage {stage}")
                stage_tracker.wait_for_stage(stage)
        time.sleep(0.1)
    
    write_thread.join()

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
        
        # Print final timing summary
        stage_tracker.print_stage_summary()
        print("ETL pipeline completed successfully")
        
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        raise
    #finally:
        # Clean up
        #spark.stop()

if __name__ == "__main__":
    run_etl() 