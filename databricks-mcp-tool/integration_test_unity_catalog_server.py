import os
import pytest
import logging
import mcp.types as types
from unity_catalog_server import list_catalogs
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
env_path = Path('.env')
if env_path.exists():
    load_dotenv(env_path)

# Integration test configuration
TEST_WORKSPACE_URL = os.getenv('DATABRICKS_WORKSPACE_URL')
TEST_ACCESS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN')

# Set up logging
logger = logging.getLogger(__name__)

def skip_if_no_credentials():
    """Skip test if credentials are not properly configured"""
    if not TEST_WORKSPACE_URL:
        pytest.skip("DATABRICKS_WORKSPACE_URL environment variable not set")
    if not TEST_ACCESS_TOKEN:
        pytest.skip("DATABRICKS_ACCESS_TOKEN environment variable not set")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_catalogs_integration(caplog):
    """Integration test for list_catalogs function"""
    caplog.set_level(logging.INFO)
    skip_if_no_credentials()
    
    try:
        # Call the function with actual credentials
        result = await list_catalogs(TEST_WORKSPACE_URL, TEST_ACCESS_TOKEN)
        
        # Basic validation of the response
        assert isinstance(result, list), "Result should be a list"
        assert len(result) == 1, "Should return exactly one TextContent object"
        assert isinstance(result[0], types.TextContent), "Result should contain TextContent"
        assert result[0].type == "text", "TextContent type should be 'text'"
        
        # Parse the catalog list from the text (it's returned as a string representation of a list)
        catalog_list = eval(result[0].text)  # Safe since we know it's a list of strings
        assert isinstance(catalog_list, list), "Catalog list should be a list"
        
        # Log the catalogs found (helpful for debugging)
        logger.info(f"Found catalogs: {catalog_list}")
        print(f"Found catalogs (print): {catalog_list}")  # Keep print for comparison
        
        # Additional validations
        for catalog in catalog_list:
            assert isinstance(catalog, str), f"Catalog name '{catalog}' should be a string"
            assert len(catalog) > 0, "Catalog name should not be empty"
            logger.info(f"Validated catalog: {catalog}")
            
    except Exception as e:
        pytest.fail(f"Integration test failed: {str(e)}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_catalogs_invalid_token():
    """Test behavior with invalid access token"""
    skip_if_no_credentials()
    
    invalid_token = "invalid-token"
    with pytest.raises(Exception) as exc_info:
        await list_catalogs(TEST_WORKSPACE_URL, invalid_token)
    assert "error" in str(exc_info.value).lower()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_catalogs_invalid_url():
    """Test behavior with invalid workspace URL"""
    skip_if_no_credentials()
    
    invalid_url = "https://invalid-workspace.databricks.com"
    with pytest.raises(Exception) as exc_info:
        await list_catalogs(invalid_url, TEST_ACCESS_TOKEN)
    assert "error" in str(exc_info.value).lower()

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 