import os
import time
import logging
from typing import Any, Dict

import pytest
import requests


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


MINDSDB_API_URL = os.getenv("MINDSDB_API_URL", "http://localhost:47334")
DATABRICKS_DB = os.getenv("DATABRICKS_DB", "databricks_test_datasource")
OLLAMA_API_BASE = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "tinyllama")


def execute_sql_via_mindsdb(sql: str, timeout: int = 300) -> Dict[str, Any]:
    """Execute a SQL query against MindsDB and return the JSON response."""
    logger.debug("Executing SQL via MindsDB: %s", sql.strip())
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
def verify_mindsdb_ready() -> str:
    """Wait until the MindsDB HTTP API is reachable."""
    max_retries = 60
    logger.info("🧠 Waiting for MindsDB to be ready...")
    for i in range(max_retries):
        try:
            resp = requests.get(f"{MINDSDB_API_URL}/api/status", timeout=5)
            if resp.status_code == 200:
                logger.info("✅ MindsDB is ready!")
                return MINDSDB_API_URL
        except requests.exceptions.RequestException:
            pass
        if i < max_retries - 1:
            time.sleep(1)
    raise Exception("MindsDB is not ready after 60 seconds")


@pytest.fixture(scope="session")
def mindsdb_connection(verify_mindsdb_ready: str) -> str:
    """Create a MindsDB connection to the Databricks database."""
    mindsdb_url = verify_mindsdb_ready
    connection_params = {
        "server_hostname": os.getenv("DATABRICKS_HOSTNAME"),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
        "access_token": os.getenv("DATABRICKS_API_TOKEN"),
        "schema": os.getenv("DATABRICKS_SCHEMA_NAME", "default"),
    }

    param_str = ",\n            ".join(
        f'"{k}": {repr(v) if not isinstance(v, int) else v}'
        for k, v in connection_params.items()
    )

    sql = f"""
        CREATE DATABASE {DATABRICKS_DB}
        WITH ENGINE = 'databricks',
        PARAMETERS = {{
            {param_str}
        }};
    """

    logger.info(f"🔗 Creating MindsDB Databricks database '{DATABRICKS_DB}' ...")
    try:
        execute_sql_via_mindsdb(sql, timeout=60)
        logger.info("✅ MindsDB Databricks connection created")

        test_sql = "SELECT 1 as test_value;"
        execute_sql_via_mindsdb(test_sql, timeout=10)
        logger.info("✅ MindsDB Databricks connection test successful")

        yield mindsdb_url
    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg:
            logger.info(f"⚠️ Database {DATABRICKS_DB} already exists, continuing...")
            yield mindsdb_url
        else:
            logger.error(f"❌ Error setting up MindsDB connection: {e}")
            raise


@pytest.fixture(scope="session")
def databricks_datasource(mindsdb_connection) -> str:
    """Return the Databricks datasource name."""
    return DATABRICKS_DB



@pytest.fixture(autouse=True)
def log_test_info(request):
    """Log the start and end of each test with its duration."""
    test_name = request.node.name
    logger.info(f"🧪 Starting test: {test_name}")
    start_time = time.time()
    yield
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"✅ Completed test: {test_name} ({duration:.2f}s)")


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
    logger.info("🚀 Starting MindsDB Databricks Handler Test Suite")
    logger.info("=" * 60)


def pytest_sessionfinish(session, exitstatus):
    """Log the end of the pytest session."""
    logger.info("=" * 60)
    if exitstatus == 0:
        logger.info("✅ All tests completed successfully!")
    else:
        logger.error(f"❌ Tests completed with exit status: {exitstatus}")
