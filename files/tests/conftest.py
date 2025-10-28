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
    max_retries = 180
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


@pytest.fixture(scope="session")
def mindsdb_connection(verify_mindsdb_ready: str) -> str:
    """Ensure MindsDB is ready for files handler tests."""
    return verify_mindsdb_ready


@pytest.fixture(scope="session")
def mindsdb_database(mindsdb_connection: str) -> str:
    """Create and return the MindsDB database name for testing."""
    db_name = "mindsdb_files_test_db"
    create_db_query = f"CREATE DATABASE {db_name}"
    execute_sql_via_mindsdb(create_db_query)
    logger.info("Using MindsDB database: %s", db_name)
    return db_name


@pytest.fixture(scope="function", autouse=True)
def setup_files_table(mindsdb_database: str):
    """Set up the files table before each test and clean up after."""
    create_table_query = f"""
    CREATE TABLE {mindsdb_database}.test (
        id INT PRIMARY KEY,
        name VARCHAR(255)
    )
    """
    execute_sql_via_mindsdb(create_table_query)
    yield
    drop_table_query = f"DROP TABLE IF EXISTS {mindsdb_database}.test"
    execute_sql_via_mindsdb(drop_table_query)
