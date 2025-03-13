import pytest
from unittest.mock import Mock, patch
import mcp.types as types
from unity_catalog_server import list_catalogs
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo

@pytest.mark.asyncio
async def test_list_catalogs_success():
    # Mock data
    mock_catalogs = [
        CatalogInfo(name="catalog1", comment="Test catalog 1"),
        CatalogInfo(name="catalog2", comment="Test catalog 2")
    ]
    
    # Mock the WorkspaceClient
    with patch('unity_catalog_server.WorkspaceClient') as mock_client:
        # Configure the mock
        mock_instance = Mock()
        mock_instance.catalogs.list.return_value = mock_catalogs
        mock_client.return_value = mock_instance
        
        # Test data
        workspace_url = "https://test-workspace.databricks.com"
        access_token = "test-token"
        
        # Call the function
        result = await list_catalogs(workspace_url, access_token)
        
        # Assertions
        assert isinstance(result, list)
        assert len(result) == 1  # We return a single TextContent object
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "catalog1" in result[0].text
        assert "catalog2" in result[0].text
        
        # Verify WorkspaceClient was called correctly
        mock_client.assert_called_once_with(
            host=workspace_url,
            token=access_token
        )
        mock_instance.catalogs.list.assert_called_once()

@pytest.mark.asyncio
async def test_list_catalogs_empty():
    # Mock the WorkspaceClient to return empty list
    with patch('unity_catalog_server.WorkspaceClient') as mock_client:
        mock_instance = Mock()
        mock_instance.catalogs.list.return_value = []
        mock_client.return_value = mock_instance
        
        result = await list_catalogs("test-url", "test-token")
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text == "[]"

@pytest.mark.asyncio
async def test_list_catalogs_error():
    # Mock the WorkspaceClient to raise an exception
    with patch('unity_catalog_server.WorkspaceClient') as mock_client:
        mock_instance = Mock()
        mock_instance.catalogs.list.side_effect = Exception("Test error")
        mock_client.return_value = mock_instance
        
        with pytest.raises(Exception) as exc_info:
            await list_catalogs("test-url", "test-token")
        
        assert str(exc_info.value) == "Test error" 