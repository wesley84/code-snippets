# Data Loader Project

A robust data loading utility for Databricks that downloads, processes, and loads data into Delta tables in Unity Catalog.

## Overview

This project provides an automated solution for:
- Downloading data files from URLs
- Extracting CSV files from zip archives
- Loading data into Unity Catalog volumes
- Creating Delta tables with proper schema inference
- Handling multiple files in parallel
- Tracking source file lineage

## Project Structure 
data-loader/
├── src/
│ └── data_loader/
│ ├── init.py
│ ├── file_loader.py # Core data loading functionality
│ └── main.py # Entry point and CLI handling
├── databricks.yml # Databricks job and resource configuration
├── pyproject.toml # Python package configuration
├── README.md # Project documentation
└── .gitignore # Git ignore rules

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.10+
- Poetry for dependency management
- Databricks CLI configured with workspace access

## Configuration

### Environment Setup

1. Configure Databricks authentication:
bash
~/.databrickscfg
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = your-personal-access-token
2. Install dependencies:
bash
pip install databricks-connect poetry
poetry install

### Job Configuration

Configure the job in `databricks.yml`:
yaml
variables:
data_url:
description: "The URL of the data to load"
default: "https://example.com/data.zip"
catalog_name:
default: "main"
schema_name:
default: "default"
volume_name:
default: "raw_data"

## Usage

### 1. Build the Package
bash
cd data-loader
poetry build

### 2. Deploy to Databricks
bash
databricks bundle deploy -t dev

### 3. Run the Job

#### Using CLI:
bash
databricks bundle run -t dev data-loader-job \
--parameters="data_url=https://example.com/data.zip"

## Features

### Multiple File Support
- Processes multiple files in sequence
- Creates separate tables for each file
- Maintains source file tracking

### Error Handling
- Robust download retry logic
- Detailed error messages
- Proper cleanup of temporary files

### Data Organization
- Structured volume paths
- Automatic table naming
- Source file lineage tracking

## Development

### Running Tests
bash
poetry run pytest


## Monitoring

Monitor job progress in the Databricks UI:
1. Go to Jobs tab
2. Find "Data Loader Job"
3. Check run status and logs

## Troubleshooting

Common issues and solutions:

1. **Connection Issues**
   - Verify Databricks token
   - Check URL accessibility
   - Confirm network access

2. **Permission Errors**
   - Verify Unity Catalog access
   - Check volume permissions
   - Confirm table create privileges

3. **Data Issues**
   - Validate ZIP file format
   - Check CSV structure
   - Verify schema compatibility

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.