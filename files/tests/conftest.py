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
