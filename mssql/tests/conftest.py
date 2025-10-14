import os
import time
import logging
from typing import Any, Dict

import pytest
import requests


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# MindsDB HTTP API
MINDSDB_API_URL = os.getenv("MINDSDB_API_URL", "http://localhost:47334")


MSSQL_DB = os.getenv("MSSQL_DB", "mssql_test").lower()


def _mindsdb_post(path: str, json: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    """Low-level POST helper with consistent error handling."""
    url = f"{MINDSDB_API_URL}{path}"
    resp = requests.post(url, json=json, timeout=timeout)
    if resp.status_code != 200:
        raise Exception(f"MindsDB API {path} failed ({resp.status_code}): {resp.text}")
    data = resp.json()
    if data.get("type") == "error":
        raise Exception(f"MindsDB returned error: {data}")
    return data


def execute_sql_via_mindsdb(sql: str, timeout: int = 300) -> Dict[str, Any]:
    """Execute a SQL query against MindsDB and return the JSON response."""
    logger.debug("Executing SQL via MindsDB: %s", sql.strip())
    return _mindsdb_post("/api/sql/query", {"query": sql}, timeout=timeout)


@pytest.fixture(scope="session")
def verify_mindsdb_ready() -> str:
    """Wait until the MindsDB HTTP API is reachable."""
    max_retries = 180  # align with the GH Actions wait
    logger.info("🧠 Waiting for MindsDB to be ready...")
    for i in range(1, max_retries + 1):
        try:
            resp = requests.get(f"{MINDSDB_API_URL}/api/status", timeout=5)
            if resp.status_code == 200:
                logger.info("✅ MindsDB is ready!")
                return MINDSDB_API_URL
        except requests.exceptions.RequestException:
            pass
        if i < max_retries:
            time.sleep(1)
    raise Exception("MindsDB is not ready after 180 seconds")


def _create_or_replace_mssql_datasource() -> None:
    """Create the MindsDB datasource for SQL Server; tolerate existing."""
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

    sql_create = f"""
        CREATE DATABASE {MSSQL_DB}
        WITH ENGINE = 'mssql',
        PARAMETERS = {{
            {param_str}
        }};
    """

    # Try create; if it already exists, drop & recreate to ensure clean state.
    try:
        logger.info(f"🔗 Creating MindsDB MS SQL datasource '{MSSQL_DB}' ...")
        execute_sql_via_mindsdb(sql_create, timeout=90)
        logger.info("✅ MindsDB MS SQL datasource created")
    except Exception as e:
        msg = str(e)
        if "already exists" in msg.lower() or "exists" in msg.lower():
            logger.warning(f"ℹ️ Datasource '{MSSQL_DB}' already exists, recreating...")
            try:
                execute_sql_via_mindsdb(f"DROP DATABASE {MSSQL_DB};", timeout=60)
            except Exception as drop_err:
                logger.warning(f"⚠️ DROP failed (continuing anyway): {drop_err}")
            execute_sql_via_mindsdb(sql_create, timeout=90)
            logger.info("✅ MindsDB MS SQL datasource recreated")
        else:
            raise


def _verify_remote_query_works() -> None:
    """Run a real query THROUGH the datasource to ensure the connection works."""
    # Either of these is fine; pick one that matches your loaded schema.
    # 1) Handler meta:
    execute_sql_via_mindsdb(f"SHOW TABLES FROM {MSSQL_DB};", timeout=30)

    # 2) Or a quick metadata read (works on SQL Server via MindsDB):
    # execute_sql_via_mindsdb(
    #     f"SELECT TOP 1 name FROM {MSSQL_DB}.sys.tables ORDER BY name;",
    #     timeout=30,
    # )


@pytest.fixture(scope="session")
def mindsdb_connection(verify_mindsdb_ready: str):
    """Create a MindsDB connection (datasource) to the MS SQL Server database, and verify it."""
    _create_or_replace_mssql_datasource()
    _verify_remote_query_works()
    yield verify_mindsdb_ready
    # Teardown (optional; comment out if you prefer to keep it around for debugging)
    try:
        logger.info(f"🧹 Dropping MindsDB datasource '{MSSQL_DB}' ...")
        execute_sql_via_mindsdb(f"DROP DATABASE {MSSQL_DB};", timeout=60)
        logger.info("✅ Datasource dropped")
    except Exception as e:
        logger.warning(f"⚠️ Teardown DROP failed: {e}")


@pytest.fixture(autouse=True)
def log_test_info(request):
    """Log the start and end of each test with its duration."""
    test_name = request.node.name
    logger.info(f"🧪 Starting test: {test_name}")
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
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
