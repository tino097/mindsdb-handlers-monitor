# mssql/tests/conftest.py
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
MSSQL_DB = os.getenv("MSSQL_DB", "mssql_test")


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
    """Create a MindsDB connection to the MS SQL Server database."""
    mindsdb_url = verify_mindsdb_ready
    connection_params = {
        "host": os.getenv("MSSQL_HOST", "localhost"),
        "port": int(os.getenv("MSSQL_PORT", "1433")),
        "user": os.getenv("MSSQL_USER", "testuser"),
        "password": os.getenv("MSSQL_PASSWORD", "TestUser@123"),
        "database": os.getenv("MSSQL_DATABASE", "TestDB"),
    }
    
    param_str = ",\n            ".join(
        f'"{k}": {repr(v) if not isinstance(v, int) else v}'
        for k, v in connection_params.items()
    )
    
    sql = f"""
        CREATE DATABASE {MSSQL_DB}
        WITH ENGINE = 'mssql',
        PARAMETERS = {{
            {param_str}
        }};
    """
    
    logger.info(f"🔗 Creating MindsDB MS SQL database '{MSSQL_DB}' ...")
    try:
        execute_sql_via_mindsdb(sql, timeout=60)
        logger.info("✅ MindsDB MS SQL connection created")
        
        test_sql = "SELECT 1 as test_value;"
        execute_sql_via_mindsdb(test_sql, timeout=10)
        logger.info("✅ MindsDB MS SQL connection test successful")
        
        yield mindsdb_url
    except Exception as e:
        logger.error(f"❌ Error setting up MindsDB connection: {e}")
        raise


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
        "tpch: TPC-H benchmark query tests for the MS SQL handler",
    )
    config.addinivalue_line(
        "markers",
        "handler: MS SQL handler functionality tests",
    )


def pytest_sessionstart(session):
    """Log the start of the pytest session."""
    print("🚀 Starting MindsDB MS SQL Handler Test Suite")
    print("=" * 60)


def pytest_sessionfinish(session, exitstatus):
    """Log the end of the pytest session."""
    print("=" * 60)
    if exitstatus == 0:
        print("✅ All tests completed successfully!")
    else:
        print(f"❌ Tests completed with exit status: {exitstatus}")