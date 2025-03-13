import anyio
import click
import mcp.types as types
from mcp.server.lowlevel import Server
from databricks.sdk import WorkspaceClient
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
env_path = Path('.env')
if env_path.exists():
    load_dotenv(env_path)

# Get default credentials from environment variables
DEFAULT_WORKSPACE_URL = os.getenv('DATABRICKS_WORKSPACE_URL')
DEFAULT_ACCESS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN')

async def list_catalogs(
    workspace_url: str = None,
    access_token: str = None,
) -> list[types.TextContent]:
    """List all catalogs in Unity Catalog"""
    # Use provided credentials or fall back to environment variables
    workspace_url = workspace_url or DEFAULT_WORKSPACE_URL
    access_token = access_token or DEFAULT_ACCESS_TOKEN

    if not workspace_url:
        raise ValueError("Workspace URL not provided and DATABRICKS_WORKSPACE_URL environment variable not set")
    if not access_token:
        raise ValueError("Access token not provided and DATABRICKS_ACCESS_TOKEN environment variable not set")

    w = WorkspaceClient(
        host=workspace_url,
        token=access_token
    )
    catalogs = w.catalogs.list()
    catalog_list = [c.name for c in catalogs]
    return [types.TextContent(type="text", text=str(catalog_list))]

async def list_schemas(
    workspace_url: str = None,
    access_token: str = None,
    catalog_name: str = None,
) -> list[types.TextContent]:
    """List all schemas in a catalog"""
    # Use provided credentials or fall back to environment variables
    workspace_url = workspace_url or DEFAULT_WORKSPACE_URL
    access_token = access_token or DEFAULT_ACCESS_TOKEN

    if not workspace_url:
        raise ValueError("Workspace URL not provided and DATABRICKS_WORKSPACE_URL environment variable not set")
    if not access_token:
        raise ValueError("Access token not provided and DATABRICKS_ACCESS_TOKEN environment variable not set")
    if not catalog_name:
        raise ValueError("Catalog name is required")

    w = WorkspaceClient(
        host=workspace_url,
        token=access_token
    )
    schemas = w.schemas.list(catalog_name=catalog_name)
    schema_list = [s.name for s in schemas]
    return [types.TextContent(type="text", text=str(schema_list))]

async def list_tables(
    workspace_url: str = None,
    access_token: str = None,
    catalog_name: str = None,
    schema_name: str = None,
) -> list[types.TextContent]:
    """List all tables in a schema"""
    # Use provided credentials or fall back to environment variables
    workspace_url = workspace_url or DEFAULT_WORKSPACE_URL
    access_token = access_token or DEFAULT_ACCESS_TOKEN

    if not workspace_url:
        raise ValueError("Workspace URL not provided and DATABRICKS_WORKSPACE_URL environment variable not set")
    if not access_token:
        raise ValueError("Access token not provided and DATABRICKS_ACCESS_TOKEN environment variable not set")
    if not catalog_name or not schema_name:
        raise ValueError("Both catalog_name and schema_name are required")

    w = WorkspaceClient(
        host=workspace_url,
        token=access_token
    )
    tables = w.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    table_list = [t.name for t in tables]
    return [types.TextContent(type="text", text=str(table_list))]

@click.command()
@click.option("--port", default=8000, help="Port to listen on for SSE")
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
    help="Transport type",
)
def main(port: int, transport: str) -> int:
    app = Server("unity-catalog-server")

    @app.call_tool()
    async def unity_catalog_tool(
        name: str, arguments: dict
    ) -> list[types.TextContent]:
        if name not in ["list_catalogs", "list_schemas", "list_tables"]:
            raise ValueError(f"Unknown tool: {name}")

        # Get credentials from arguments or environment variables
        workspace_url = arguments.get("workspace_url", DEFAULT_WORKSPACE_URL)
        access_token = arguments.get("access_token", DEFAULT_ACCESS_TOKEN)

        if name == "list_catalogs":
            return await list_catalogs(workspace_url, access_token)
        elif name == "list_schemas":
            if "catalog_name" not in arguments:
                raise ValueError("Missing required argument 'catalog_name'")
            return await list_schemas(
                workspace_url,
                access_token,
                arguments["catalog_name"]
            )
        else:  # list_tables
            if "catalog_name" not in arguments or "schema_name" not in arguments:
                raise ValueError("Missing required arguments 'catalog_name' and/or 'schema_name'")
            return await list_tables(
                workspace_url,
                access_token,
                arguments["catalog_name"],
                arguments["schema_name"]
            )

    @app.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="list_catalogs",
                description="Lists all catalogs in Unity Catalog",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "workspace_url": {
                            "type": "string",
                            "description": "Databricks workspace URL (optional if set in environment)",
                        },
                        "access_token": {
                            "type": "string",
                            "description": "Databricks access token (optional if set in environment)",
                        }
                    },
                },
            ),
            types.Tool(
                name="list_schemas",
                description="Lists all schemas in a catalog",
                inputSchema={
                    "type": "object",
                    "required": ["catalog_name"],
                    "properties": {
                        "workspace_url": {
                            "type": "string",
                            "description": "Databricks workspace URL (optional if set in environment)",
                        },
                        "access_token": {
                            "type": "string",
                            "description": "Databricks access token (optional if set in environment)",
                        },
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the catalog",
                        }
                    },
                },
            ),
            types.Tool(
                name="list_tables",
                description="Lists all tables in a schema",
                inputSchema={
                    "type": "object",
                    "required": ["catalog_name", "schema_name"],
                    "properties": {
                        "workspace_url": {
                            "type": "string",
                            "description": "Databricks workspace URL (optional if set in environment)",
                        },
                        "access_token": {
                            "type": "string",
                            "description": "Databricks access token (optional if set in environment)",
                        },
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the catalog",
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Name of the schema",
                        }
                    },
                },
            )
        ]

    if transport == "sse":
        from mcp.server.sse import SseServerTransport
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route

        sse = SseServerTransport("/messages/")

        async def handle_sse(request):
            async with sse.connect_sse(
                request.scope, request.receive, request._send
            ) as streams:
                await app.run(
                    streams[0], streams[1], app.create_initialization_options()
                )

        starlette_app = Starlette(
            debug=True,
            routes=[
                Route("/sse", endpoint=handle_sse),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )

        import uvicorn
        uvicorn.run(starlette_app, host="0.0.0.0", port=port)
    else:
        from mcp.server.stdio import stdio_server

        async def arun():
            async with stdio_server() as streams:
                await app.run(
                    streams[0], streams[1], app.create_initialization_options()
                )

        anyio.run(arun)

    return 0