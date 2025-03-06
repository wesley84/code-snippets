import os
import requests
import zipfile
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataFileLoader:
    """Handles downloading, unzipping and loading data files to Unity Catalog"""
    
    def __init__(self, spark: SparkSession, volume_path: str):
        """
        Args:
            spark: Active SparkSession
            volume_path: Unity Catalog volume path (e.g., 'catalog.schema.volume')
        """
        self.spark = spark
        self.volume_path = volume_path
        # Get dbutils from spark
        self.dbutils = self._get_dbutils()
        
    def _get_dbutils(self):
        """Get dbutils from Databricks runtime"""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            # If running in Databricks, dbutils is already available
            try:
                from databricks.sdk.runtime import dbutils
                return dbutils
            except ImportError:
                raise ImportError("Could not import dbutils. Make sure you're running in a Databricks environment.")
        
    def download_file(self, url: str, local_path: str) -> str:
        """Downloads file from URL to local path
        
        Args:
            url: URL to download from
            local_path: Directory or file path to save to
            
        Returns:
            Path to the downloaded file
        """
        try:
            print(f"Downloading file from {url}")
            response = requests.get(url, stream=True, verify=True)
            response.raise_for_status()
            
            # If local_path is a directory, append the filename from URL
            if os.path.isdir(local_path) or local_path.endswith('/'):
                filename = os.path.basename(urlparse(url).path)
                if not filename:  # Handle URLs without filename
                    filename = 'downloaded_file.zip'
                file_path = os.path.join(local_path, filename)
            else:
                file_path = local_path
                # Ensure directory exists
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Download the file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            print(f"File downloaded to {file_path}")
            return file_path
            
        except requests.exceptions.RequestException as e:
            print(f"Error downloading file: {str(e)}")
            print(f"URL attempted: {url}")
            raise
        except OSError as e:
            print(f"Error saving file: {str(e)}")
            print(f"Attempted path: {file_path}")
            raise
    
    def unzip_file(self, zip_path: str, extract_path: str) -> str:
        """Unzips file and returns path to CSV
        
        Args:
            zip_path: Path to zip file
            extract_path: Path to extract contents
            
        Returns:
            Path to extracted CSV file
        """
        filename = os.path.basename(zip_path).replace('.zip', '')
        extract_path_new = os.path.join(extract_path, filename)
        logger.info(f"Extracting {filename} {extract_path} {extract_path_new}")
        if not os.path.exists(extract_path_new):
            os.makedirs(extract_path_new, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path_new)
            
        # Find CSV file in extracted contents
        csv_files = []
        for root, _, files in os.walk(extract_path_new):
            csv_files.extend(
                os.path.join(root, file)
                for file in files
                if file.endswith('.csv')
            )
            
        if not csv_files:
            raise ValueError("No CSV file found in zip archive")
            
        logger.info(f"Found CSV file: {csv_files[0]}")
        return csv_files[0]
    
    def load_to_volume(self, file_path: str, volume_dir: str) -> str:
        """Copies file to Unity Catalog volume
        
        Args:
            file_path: Local path to file
            volume_dir: Subdirectory in volume (e.g., 'raw')
            
        Returns:
            Full path in volume including filename
        """
        # Get the source filename
        file_name = os.path.basename(file_path)
        
        # Ensure volume directory exists
        volume_file_path = f"{self.volume_path}/{volume_dir}/{file_name}"
        
        # Create directory if it doesn't exist
        volume_dir_path = f"/Volumes/{self.volume_path}/{volume_dir}/{file_name}"
        if not self.dbutils.fs.ls(volume_dir_path):
            self.dbutils.fs.mkdirs(volume_dir_path)
        
        # Copy file to volume with filename
        self.dbutils.fs.cp(f"file:{file_path}", f"/Volumes/{self.volume_path}/{volume_dir}/{file_name}")
        logger.info(f"File copied to volume: {volume_file_path}")
        return volume_file_path
    
    def create_delta_table(self, 
                          catalog_name: str,
                          volume_path: str,
                          table_name: str,
                          schema_name: str = None,
                          options: dict = None) -> None:
        """Creates Delta table from CSV in volume using Spark"""
        logger.info(f"Creating Delta table {table_name}")
        filename = os.path.basename(volume_path).replace('.csv', '')
        table_name = filename
        read_options = {
            "header": "true",
            "inferSchema": "true"
        }
        if options:
            read_options.update(options)
            
        # Read the CSV file
        df = self.spark.read.options(**read_options) \
            .csv(f"/Volumes/{volume_path}")
        
        # Add source_file column
        from pyspark.sql.functions import lit, input_file_name
        df = df.withColumn("source_file", col("_metadata.file_name")) 
        
        if schema_name:
            table_name = f"{catalog_name}.{schema_name}.`{table_name}`"
            
        # Check if table exists
        if self.spark._jsparkSession.catalog().tableExists(table_name):
            logger.info(f"Table {table_name} already exists, overwriting")
            
        df.write.format("delta").option("overwriteSchema", "true") \
            .mode("overwrite") \
            .saveAsTable(table_name)
            
        logger.info(f"Delta table {table_name} created successfully") 