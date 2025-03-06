import tempfile
import argparse
import json
from data_loader.file_loader import DataFileLoader

def executor(catalog_name: str = None,
         data_urls: list = None,  # Changed from data_url to data_urls
         volume_path: str = None, 
         table_name: str = None, 
         schema_name: str = None):
    """Main entry point for the data loader
    
    Args:
        catalog_name: Target catalog name
        data_urls: List of URLs of zip files containing CSV data
        volume_path: Unity Catalog volume path
        table_name: Base name for the target Delta tables
        schema_name: Schema name for the target tables
    """
    # Initialize loader with Spark session
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    loader = DataFileLoader(spark, volume_path)

    # Create temp directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        for idx, data_url in enumerate(data_urls):
            try:
                # Generate unique table name for each file
                current_table = f"{table_name}_{idx}" if len(data_urls) > 1 else table_name
                
                # Download zip file
                zip_path = loader.download_file(data_url, temp_dir)
                
                # Extract CSV
                csv_path = loader.unzip_file(zip_path, temp_dir)
                
                # Load to volume with unique subfolder
                volume_file_path = loader.load_to_volume(csv_path, f"raw")
                
                # Create Delta table
                loader.create_delta_table(
                    catalog_name,
                    volume_file_path,
                    current_table,
                    schema_name,
                    options={"header": "true"}
                )
                print(f"Successfully processed file {idx + 1} of {len(data_urls)}")
                
            except Exception as e:
                print(f"Error processing file {data_url}: {str(e)}")
                raise

def entrypoint():
    """Command line entrypoint"""
    parser = argparse.ArgumentParser(description='Data Loader')
    parser.add_argument('--catalog_name', help='Target Delta catalog name')
    parser.add_argument('--data_url', help='JSON array of URLs to download')
    parser.add_argument('--volume_path', help='Unity Catalog volume path')
    parser.add_argument('--table_name', help='Base name for target Delta tables')
    parser.add_argument('--schema_name', help='Target schema name')
    
    args = parser.parse_args()
    list_of_items = args.data_url.split(',')
    
    executor(
        catalog_name=args.catalog_name,
        data_urls=list_of_items,
        volume_path=args.volume_path,
        table_name=args.table_name,
        schema_name=args.schema_name
    )

if __name__ == "__main__":
    entrypoint() 