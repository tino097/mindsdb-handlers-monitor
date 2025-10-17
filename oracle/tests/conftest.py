# oracle/tests/conftest.py
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
ORACLE_TPCH_DB = os.getenv("ORACLE_TPCH_DB", "oracle_tpch")
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
    """Create a MindsDB connection to the Oracle TPCH database."""
    mindsdb_url = verify_mindsdb_ready
    connection_params = {
        "user": os.getenv("ORACLE_USER", "sampleuser"),
        "password": os.getenv("ORACLE_PASSWORD", "SamplePass123"),
        "dsn": f"{os.getenv('ORACLE_HOST', 'localhost')}:{os.getenv('ORACLE_PORT', '1521')}/{os.getenv('ORACLE_DB', 'XEPDB1')}",
    }

    param_str = ",\n            ".join(
        f'"{k}": {repr(v) if not isinstance(v, int) else v}'
        for k, v in connection_params.items()
    )

    sql = f"""
        CREATE DATABASE {ORACLE_TPCH_DB}
        WITH ENGINE = 'oracle',
        PARAMETERS = {{
            {param_str}
        }};
    """

    logger.info(f"🔗 Creating MindsDB Oracle database '{ORACLE_TPCH_DB}' ...")
    try:
        execute_sql_via_mindsdb(sql, timeout=60)
        logger.info("✅ MindsDB Oracle connection created")

        test_sql = "SELECT 1 as test_value;"
        execute_sql_via_mindsdb(test_sql, timeout=10)
        logger.info("✅ MindsDB Oracle connection test successful")

        yield mindsdb_url
    except Exception as e:
        logger.error(f"❌ Error setting up MindsDB connection: {e}")
        raise


@pytest.fixture(scope="session")
def oracle_regions_kb(mindsdb_connection):
    """Create Knowledge Base for Oracle REGION table."""
    logger.info("📚 Creating oracle_regions_kb Knowledge Base...")

    kb_sql = f"""
    CREATE KNOWLEDGE_BASE oracle_regions_kb
    USING
        embedding_model = {{
            "provider": "ollama",
            "model_name": "{OLLAMA_MODEL}",
            "ollama_serve_url": "{OLLAMA_API_BASE}"
        }},
        storage = {ORACLE_TPCH_DB}.REGION,
        content_columns = ['R_NAME', 'R_COMMENT'],
        id_column = 'R_REGIONKEY';
    """

    try:
        execute_sql_via_mindsdb(kb_sql, timeout=120)
        logger.info("✅ oracle_regions_kb created")
    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg:
            logger.info("⚠️ oracle_regions_kb already exists")
        else:
            logger.error(f"❌ Failed to create KB: {e}")
            raise

    time.sleep(15)
    yield "oracle_regions_kb"

    # Cleanup
    logger.info("🧹 Cleaning up oracle_regions_kb...")
    try:
        execute_sql_via_mindsdb(
            "DROP KNOWLEDGE_BASE IF EXISTS oracle_regions_kb;", timeout=30
        )
    except Exception as e:
        logger.warning(f"⚠️ Could not drop oracle_regions_kb: {e}")


@pytest.fixture(scope="session")
def oracle_nations_kb(mindsdb_connection):
    """Create Knowledge Base for Oracle NATION table."""
    logger.info("📚 Creating oracle_nations_kb Knowledge Base...")

    kb_sql = f"""
    CREATE KNOWLEDGE_BASE oracle_nations_kb
    USING
        embedding_model = {{
            "provider": "ollama",
            "model_name": "{OLLAMA_MODEL}",
            "ollama_serve_url": "{OLLAMA_API_BASE}"
        }},
        storage = {ORACLE_TPCH_DB}.NATION,
        content_columns = ['N_NAME', 'N_COMMENT'],
        id_column = 'N_NATIONKEY';
    """

    try:
        execute_sql_via_mindsdb(kb_sql, timeout=120)
        logger.info("✅ oracle_nations_kb created")
    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg:
            logger.info("⚠️ oracle_nations_kb already exists")
        else:
            logger.error(f"❌ Failed to create KB: {e}")
            raise

    time.sleep(15)
    yield "oracle_nations_kb"

    # Cleanup
    logger.info("🧹 Cleaning up oracle_nations_kb...")
    try:
        execute_sql_via_mindsdb(
            "DROP KNOWLEDGE_BASE IF EXISTS oracle_nations_kb;", timeout=30
        )
    except Exception as e:
        logger.warning(f"⚠️ Could not drop oracle_nations_kb: {e}")


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
        "tpch: TPC-H benchmark query tests for the Oracle handler",
    )
    config.addinivalue_line(
        "markers",
        "handler: Oracle handler functionality tests",
    )


def pytest_sessionstart(session):
    """Log the start of the pytest session."""
    logger.info("🚀 Starting MindsDB Oracle Handler Test Suite")
    logger.info("=" * 60)


def pytest_sessionfinish(session, exitstatus):
    """Log the end of the pytest session."""
    logger.info("=" * 60)
    if exitstatus == 0:
        logger.info("✅ All tests completed successfully!")
    else:
        logger.error(f"❌ Tests completed with exit status: {exitstatus}")
