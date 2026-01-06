"""
pytest fixtures for HubSpot handler tests.

Provides reusable test fixtures for:
- MindsDB API connection
- HubSpot database connection (session-scoped)
- Knowledge Base creation (session-scoped)
- AI Agent setup (session-scoped)
- Test data setup and teardown
"""

import os
import time
import logging
from typing import Any, Dict, Optional

import pytest
import requests


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


# Configuration from environment variables
MINDSDB_API_URL = os.getenv("MINDSDB_API_URL", "http://localhost:47334")
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
HUBSPOT_DB_NAME = os.getenv("HUBSPOT_DB_NAME", "hubspot_ds")

OLLAMA_API_BASE = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "tinyllama")


def execute_sql_via_mindsdb(sql: str, timeout: int = 300) -> Dict[str, Any]:
    """
    Execute a SQL query against MindsDB and return the JSON response.

    Args:
        sql: SQL query string to execute
        timeout: Request timeout in seconds

    Returns:
        Response JSON dict containing 'data' or 'error' keys

    Raises:
        Exception: If API request fails or MindsDB returns an error
    """
    logger.debug("Executing SQL via MindsDB: %s", sql.strip()[:200])
    resp = requests.post(
        f"{MINDSDB_API_URL}/api/sql/query",
        json={"query": sql},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise Exception(
            f"MindsDB API request failed with status {resp.status_code}: {resp.text}"
        )
    data = resp.json()
    if data.get("type") == "error":
        raise Exception(f"MindsDB returned error: {data}")
    return data


def wait_for_model_status(
    model_name: str, expected_status: str = "complete", max_wait: int = 120
) -> bool:
    """
    Wait for a model to reach expected status.

    Args:
        model_name: Name of the model to check
        expected_status: Expected status (default: 'complete')
        max_wait: Maximum seconds to wait

    Returns:
        True if model reached expected status, False otherwise
    """
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            result = execute_sql_via_mindsdb(f"DESCRIBE {model_name};", timeout=30)
            if "data" in result and len(result["data"]) > 0:
                status = result["data"][0].get("status", "").lower()
                if status == expected_status:
                    return True
                elif status == "error":
                    logger.error(f"Model {model_name} in error state")
                    return False
        except Exception as e:
            logger.warning(f"Error checking model status: {e}")
        time.sleep(5)
    return False


@pytest.fixture(scope="session")
def mindsdb_api_url() -> str:
    """Return MindsDB API base URL."""
    return MINDSDB_API_URL


@pytest.fixture(scope="session")
def hubspot_api_key() -> str:
    """
    Return HubSpot API key from environment.

    This should be set as HUBSPOT_API_KEY environment variable or in a .env file.
    """
    if not HUBSPOT_API_KEY:
        pytest.skip("HUBSPOT_API_KEY environment variable not set")
    return HUBSPOT_API_KEY


@pytest.fixture(scope="session")
def verify_mindsdb_ready() -> str:
    """Wait until the MindsDB HTTP API is reachable."""
    max_retries = 60
    logger.info("Waiting for MindsDB to be ready...")
    for i in range(max_retries):
        try:
            resp = requests.get(f"{MINDSDB_API_URL}/api/status", timeout=5)
            if resp.status_code == 200:
                logger.info("MindsDB is ready!")
                return MINDSDB_API_URL
        except requests.exceptions.RequestException:
            pass
        if i < max_retries - 1:
            time.sleep(1)
    raise Exception("MindsDB is not ready after 60 seconds")


@pytest.fixture(scope="session")
def hubspot_datasource(verify_mindsdb_ready: str, hubspot_api_key: str) -> str:
    """
    Create HubSpot database connection in MindsDB (session-scoped).

    Returns:
        Database name for use in queries
    """
    db_name = HUBSPOT_DB_NAME

    # Drop existing database if present
    logger.info(f"Dropping existing HubSpot database '{db_name}' if present...")
    try:
        execute_sql_via_mindsdb(f"DROP DATABASE IF EXISTS {db_name};", timeout=30)
    except Exception as e:
        logger.warning(f"Could not drop existing database: {e}")

    # Create HubSpot database connection
    create_sql = f"""
    CREATE DATABASE {db_name}
    WITH ENGINE = 'hubspot',
    PARAMETERS = {{
        "api_key": "{hubspot_api_key}"
    }};
    """

    logger.info(f"Creating MindsDB HubSpot database '{db_name}'...")
    try:
        execute_sql_via_mindsdb(create_sql, timeout=60)
        logger.info("MindsDB HubSpot connection created")
    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg:
            logger.info(f"Database {db_name} already exists, using existing")
        else:
            logger.error(f"Failed to create HubSpot database: {e}")
            raise

    # Verify connection by listing tables
    logger.info("Verifying HubSpot connection...")
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            result = execute_sql_via_mindsdb(f"SHOW TABLES FROM {db_name};", timeout=30)
            if "data" in result:
                tables = [
                    (
                        row[0]
                        if isinstance(row, (list, tuple))
                        else row.get("Tables_in_" + db_name, row.get("name", ""))
                    )
                    for row in result["data"]
                ]
                logger.info(
                    f"HubSpot connection verified. Available tables: {tables[:5]}..."
                )
                break
        except Exception as e:
            logger.warning(f"Verification attempt {attempt + 1}/{max_attempts}: {e}")
            if attempt < max_attempts - 1:
                time.sleep(2)
    else:
        logger.warning("Could not verify HubSpot tables (connection may still work)")

    yield db_name
	
    logger.info(f"Cleaning up HubSpot database '{db_name}'...")
    try:
        execute_sql_via_mindsdb(f"DROP DATABASE IF EXISTS {db_name};", timeout=30)
        logger.info("HubSpot database cleaned up")
    except Exception as e:
        logger.warning(f"Cleanup warning: {e}")



def execute_sql(mindsdb_api_url: str):
    """Helper fixture to execute SQL queries against MindsDB."""

    def _execute(query: str, timeout: int = 60) -> Dict[str, Any]:
        """
        Execute SQL query and return response.

        Args:
            query: SQL query string
            timeout: Request timeout in seconds

        Returns:
            Response JSON dict
        """
        return execute_sql_via_mindsdb(query, timeout)

    return _execute


@pytest.fixture(autouse=True)
def log_test_info(request):
    """Log the start and end of each test with its duration."""
    test_name = request.node.name
    logger.info(f"🧪 Starting test: {test_name}")
    start_time = time.time()
    yield
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Completed test: {test_name} ({duration:.2f}s)")


def pytest_configure(config):
    """Register custom pytest markers for this test suite."""
    config.addinivalue_line(
        "markers",
        "handler: HubSpot handler functionality tests",
    )
    config.addinivalue_line(
        "markers",
        "query: SQL query tests for HubSpot tables",
    )
    config.addinivalue_line(
        "markers",
        "kb: Knowledge Base tests",
    )
    config.addinivalue_line(
        "markers",
        "agent: AI Agent tests",
    )
    config.addinivalue_line(
        "markers",
        "data_catalog: Data Catalog method tests",
    )


def pytest_sessionstart(session):
    """Log the start of the pytest session."""
    logger.info("Starting MindsDB HubSpot Handler Test Suite")
    logger.info("=" * 60)


def pytest_sessionfinish(session, exitstatus):
    """Log the end of the pytest session."""
    logger.info("=" * 60)
    if exitstatus == 0:
        logger.info("All tests completed successfully!")
    else:
        logger.error(f"Tests completed with exit status: {exitstatus}")
