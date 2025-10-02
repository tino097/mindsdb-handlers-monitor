import os
import time
import logging
import pytest

# Use python-oracledb for Oracle connections
import oracledb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def db_connection_params():
    """Provide Oracle connection parameters to tests."""
    return {
        "user": os.getenv("ORACLE_USER", "sampleuser"),
        "password": os.getenv("ORACLE_PASSWORD", "SamplePass123"),
        "host": os.getenv("ORACLE_HOST", "localhost"),
        "port": os.getenv("ORACLE_PORT", "1521"),
        "service_name": os.getenv("ORACLE_DB", "XEPDB1"),
    }


@pytest.fixture(scope="module")
def db_conn(db_connection_params):
    """Provide a database connection for the test module."""
    dsn = f"{db_connection_params['host']}:{db_connection_params['port']}/{db_connection_params['service_name']}"
    conn = oracledb.connect(
        user=db_connection_params["user"],
        password=db_connection_params["password"],
        dsn=dsn,
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def verify_mindsdb_ready():
    """Ensure MindsDB is ready before running tests."""
    import requests

    max_retries = 60
    mindsdb_url = "http://localhost:47334"

    logger.info("🧠 Waiting for MindsDB to be ready...")
    for i in range(max_retries):
        try:
            resp = requests.get(f"{mindsdb_url}/api/status", timeout=5)
            if resp.status_code == 200:
                logger.info("✅ MindsDB is ready!")
                return mindsdb_url
        except requests.exceptions.RequestException:
            pass
        if i < max_retries - 1:
            time.sleep(1)
        else:
            raise Exception("MindsDB is not ready after 60 seconds")


@pytest.fixture(scope="session")
def mindsdb_connection(verify_mindsdb_ready):
    """Create a MindsDB connection to the Oracle database."""
    import requests

    mindsdb_url = verify_mindsdb_ready
    # Use environment variables for Oracle connection parameters
    connection_params = {
        "host": os.getenv("ORACLE_HOST", "localhost"),
        "port": int(os.getenv("ORACLE_PORT", "1521")),
        "user": os.getenv("ORACLE_USER", "sampleuser"),
        "password": os.getenv("ORACLE_PASSWORD", "SamplePass123"),
        "sid": os.getenv("ORACLE_DB", "XEPDB1"),
    }

    sql = f"""
    CREATE DATABASE IF NOT EXISTS oracle_db
    WITH ENGINE = 'oracle',
    PARAMETERS = {{
        "host": "{connection_params['host']}",
        "port": {connection_params['port']},
        "user": "{connection_params['user']}",
        "password": "{connection_params['password']}",
        "sid": "{connection_params['sid']}"
    }};
    """

    logger.info("🔗 Creating MindsDB Oracle connection...")
    try:
        resp = requests.post(
            f"{mindsdb_url}/api/sql/query", json={"query": sql}, timeout=30
        )
        if resp.status_code != 200:
            raise Exception(
                f"Failed to create MindsDB connection: HTTP {resp.status_code}"
            )
        data = resp.json()
        if data.get("type") == "error":
            raise Exception(f"MindsDB error: {data}")

        logger.info("✅ MindsDB Oracle connection created")

        # Test the connection
        test_sql = "SELECT 1 as test_connection;"
        resp = requests.post(
            f"{mindsdb_url}/api/sql/query", json={"query": test_sql}, timeout=10
        )
        if resp.status_code == 200 and resp.json().get("type") != "error":
            logger.info("✅ MindsDB connection test successful")
        else:
            logger.warning("⚠️ MindsDB connection test failed")

        yield mindsdb_url

    except Exception as e:
        logger.error(f"❌ Error setting up MindsDB connection: {e}")
        raise


@pytest.fixture(autouse=True)
def log_test_info(request):
    """Log test start and end information."""
    test_name = request.node.name
    logger.info(f"🧪 Starting test: {test_name}")
    start_time = time.time()
    yield
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"✅ Completed test: {test_name} ({duration:.2f}s)")



# Helper function for tests
def execute_sql_via_mindsdb(sql, mindsdb_url="http://localhost:47334", timeout=30):
    """Helper to execute SQL via the MindsDB API."""
    import requests

    resp = requests.post(
        f"{mindsdb_url}/api/sql/query", json={"query": sql}, timeout=timeout
    )
    if resp.status_code != 200:
        raise Exception(
            f"MindsDB API request failed with status {resp.status_code}: {resp.text}"
        )
    data = resp.json()
    if data.get("type") == "error":
        raise Exception(f"MindsDB returned error: {data}")
    return data


# Expose the helper function to tests
pytest.execute_sql_via_mindsdb = execute_sql_via_mindsdb
