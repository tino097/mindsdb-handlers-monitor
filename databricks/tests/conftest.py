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
DATABRICKS_API_TOKEN = os.getenv("DATABRICKS_API_TOKEN")

DATABRICKS_HOSTNAME = os.getenv("DATABRICKS_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_SCHEMA_NAME = os.getenv("DATABRICKS_SCHEMA_NAME")


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


@pytest.fixture(scope="session")
def mindsdb_api_url() -> str:
    """Return MindsDB API base URL."""
    return MINDSDB_API_URL


@pytest.fixture(scope="session")
def databricks_config() -> Dict[str, Optional[str]]:
    """Return Databricks configuration from environment variables."""
    return {
        "api_token": DATABRICKS_API_TOKEN,
        "hostname": DATABRICKS_HOSTNAME,
        "http_path": DATABRICKS_HTTP_PATH,
        "schema_name": DATABRICKS_SCHEMA_NAME,
    }


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
def databricks_datasource(
    verify_mindsdb_ready: str, databricks_config: Dict[str, Optional[str]]
) -> str:
    """Create and return the Databricks datasource name in MindsDB."""
    ds_name = "databricks_test_datasource"
    logger.info("Creating Databricks datasource in MindsDB: %s", ds_name)
    create_ds_sql = f"""
    CREATE DATABASE {ds_name}
    WITH engine = 'databricks',
    parameters = {{
        'access_token': '{databricks_config["api_token"]}',
        'server_hostname': '{databricks_config["hostname"]}',
        'http_path': '{databricks_config["http_path"]}',
        'schema_name': '{databricks_config["schema_name"]}'
    }};
    """
    execute_sql_via_mindsdb(create_ds_sql)
    logger.info("Databricks datasource created: %s", ds_name)
    return ds_name


@pytest.fixture(scope="session")
def databricks_cleanup(databricks_datasource: str):
    """Cleanup Databricks datasource after tests are done."""
    yield
    logger.info("Dropping Databricks datasource: %s", databricks_datasource)
    drop_ds_sql = f"DROP DATABASE IF EXISTS {databricks_datasource};"
    try:
        execute_sql_via_mindsdb(drop_ds_sql)
        logger.info("Databricks datasource dropped: %s", databricks_datasource)
    except Exception as e:
        logger.warning(f"Could not drop Databricks datasource: {e}")


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


def pytest_configure(config):
    """Register custom pytest markers for this test suite."""
    config.addinivalue_line(
        "markers",
        "handler: Databricks handler functionality tests",
    )
    config.addinivalue_line(
        "markers",
        "query: SQL query tests for Databricks tables",
    )


def pytest_sessionstart(session):
    """Log the start of the pytest session."""
    logger.info("Starting MindsDB Databricks Handler Test Suite")
    logger.info("=" * 60)


def pytest_sessionfinish(session, exitstatus):
    """Log the end of the pytest session."""
    logger.info("=" * 60)
    if exitstatus == 0:
        logger.info("All tests completed successfully!")
    else:
        logger.error(f"Tests completed with exit status: {exitstatus}")
