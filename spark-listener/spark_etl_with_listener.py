from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import time

def track_stage_time(func):
    """Decorator to track execution time of stages"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Stage {func.__name__} completed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

class ETLStageTracker:
    def __init__(self, spark):
        self.spark = spark
        self.tracker = spark.sparkContext.statusTracker()
    
    def wait_for_stage(self, stage_id):
        while not self.tracker.getStageInfo(stage_id):
            time.sleep(0.1)
        
        while any(s.stageId == stage_id for s in self.tracker.getActiveStages()):
            time.sleep(0.1)

@track_stage_time
def read_stage(spark):
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/databricks-datasets/iot-stream/data-user/") \
        .repartition(10)
    return df

@track_stage_time
def transform_stage(df):
    filtered_df = df.filter(col("age") > 30).cache()
    # Force evaluation
    count = filtered_df.count()
    print(f"Number of records after filtering: {count}")
    return filtered_df

@track_stage_time
def write_stage(df):
    df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("adb_swxxef_workspace.`td-test`.tracker_users_listener")

def run_etl():
    # Create Spark session with some configurations for better performance
    
    try:
        # Execute stages
        df = read_stage(spark)
        filtered_df = transform_stage(df)
        write_stage(filtered_df)
        
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        raise
    #finally:
        # Clean up
        #spark.stop()

if __name__ == "__main__":
    run_etl() 