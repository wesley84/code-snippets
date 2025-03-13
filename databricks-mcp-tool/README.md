# Databricks MCP Tool

## Overview
This tool provides a server interface for interacting with Databricks Unity Catalog through the Model Control Plane (MCP) protocol. It allows AI assistants and other applications to query Databricks metadata and explore data assets programmatically.

## Features
- **Unity Catalog Exploration**: Query catalogs, schemas, and tables in your Databricks workspace
- **MCP Protocol Support**: Implements the Model Control Plane protocol for AI assistant integration
- **Multiple Transport Options**: Supports both stdio and Server-Sent Events (SSE) transports
- **Environment Variable Configuration**: Easily configure with environment variables or direct parameters

## Recent Updates
- Separated the monolithic `unity_catalog_server.py` into modular components
- Added a standalone `run_server.py` script for easier execution
- Improved error handling and credential management
- Enhanced documentation and usage examples

## Installation
```bash
# Clone the repository
git clone https://github.com/yourusername/databricks-mcp-tool.git
cd databricks-mcp-tool

# Install dependencies
pip install -r requirements.txt

# Optional: Install in development mode
pip install -e .
```

## Configuration
Configure your Databricks credentials using one of these methods:

1. **Environment Variables**:
   ```bash
   export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_ACCESS_TOKEN="your-access-token"
   ```

2. **`.env` File**:
   Create a `.env` file in the project root:
   ```
   DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
   DATABRICKS_ACCESS_TOKEN=your-access-token
   ```

3. **Direct Parameters**:
   Pass credentials directly when calling functions or running the server.

## Usage

### Running the Server
```bash
# Using the run_server.py script (recommended)
python run_server.py --transport sse --port 8765

# Or using the module directly (if installed)
python -m mcp_databricks_tool --transport sse --port 8765
```

### Available Tools
The server exposes the following tools:

1. **list_catalogs**: Lists all catalogs in Unity Catalog
2. **list_schemas**: Lists all schemas in a specified catalog
3. **list_tables**: Lists all tables in a specified schema

### Example Queries
When integrated with an AI assistant, you can ask questions like:
- "What catalogs are available in my Databricks workspace?"
- "Show me all schemas in the 'samples' catalog"
- "List all tables in the 'main' schema of the 'adb_cb6l9m_workspace' catalog"

## Project Structure
- `mcp_databricks_tool/`: Main package directory
  - `unity_catalog_server.py`: Core server implementation
  - `__main__.py`: Entry point for running as a module
- `run_server.py`: Standalone script for running the server
- `requirements.txt`: Project dependencies

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## License
[Specify your license here]
