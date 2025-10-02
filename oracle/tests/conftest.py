import os
import time
import logging
import pytest

# Use python-oracledb for Oracle connections
import oracledb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def load_test_data():
    """Load test data from SQL file into Oracle at the start of the test session."""
    logger.info("🔧 Setting up Oracle test database...")

    # Connection parameters from environment variables (with sensible defaults).
    conn_params = {
        "user": os.getenv("ORACLE_USER", "sampleuser"),
        "password": os.getenv("ORACLE_PASSWORD", "SamplePass123"),
        "host": os.getenv("ORACLE_HOST", "localhost"),
        "port": os.getenv("ORACLE_PORT", "1521"),
        "service_name": os.getenv("ORACLE_DB", "XEPDB1"),
    }

    # Wait for Oracle to be ready.
    max_retries = 30
    for i in range(max_retries):
        try:
            dsn = f"{conn_params['host']}:{conn_params['port']}/{conn_params['service_name']}"
            conn = oracledb.connect(
                user=conn_params["user"], password=conn_params["password"], dsn=dsn
            )
            conn.close()
            logger.info("✅ Oracle connection successful")
            break
        except oracledb.Error as e:
            if i == max_retries - 1:
                raise Exception(
                    f"Oracle is not available after {max_retries} attempts: {e}"
                )
            time.sleep(1)

    # Load test data from a .sql file
    dsn = f"{conn_params['host']}:{conn_params['port']}/{conn_params['service_name']}"
    conn = oracledb.connect(
        user=conn_params["user"], password=conn_params["password"], dsn=dsn
    )
    try:
        with conn.cursor() as cur:
            # Determine the path to the SQL file relative to this file
            sql_path = os.path.join(os.path.dirname(__file__), "test_data.sql")
            if not os.path.exists(sql_path):
                raise FileNotFoundError(f"test_data.sql not found at {sql_path}")

            logger.info(f"📊 Loading test data from {sql_path}")

            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Oracle cursors do not support executing multiple statements in a single call,
            # so split the file on semicolons and execute each statement individually.
            for statement in sql_content.split(";"):
                stmt = statement.strip()
                if stmt:
                    cur.execute(stmt)
            conn.commit()

            # Verify that core tables were populated
            verification_queries = [
                ("customers", "SELECT COUNT(*) FROM customers"),
                ("products", "SELECT COUNT(*) FROM products"),
                ("orders", "SELECT COUNT(*) FROM orders"),
                ("test_table", "SELECT COUNT(*) FROM test_table"),
                ("sales.sales_reps", "SELECT COUNT(*) FROM sales.sales_reps"),
                ("inventory.warehouses", "SELECT COUNT(*) FROM inventory.warehouses"),
            ]

            logger.info("✅ Test data loaded successfully:")
            for table_name, query in verification_queries:
                try:
                    cur.execute(query)
                    count = cur.fetchone()[0]
                    logger.info(f"   - {table_name}: {count} rows")
                except oracledb.Error as e:
                    logger.warning(f"   - {table_name}: Error checking ({e})")

            # Verify that schemas exist
            cur.execute(
                """
                SELECT username FROM all_users 
                WHERE username IN ('SALES', 'INVENTORY', 'ANALYTICS')
                ORDER BY username
            """
            )
            schemas = [row[0] for row in cur.fetchall()]
            logger.info(f"   - Schemas created: {', '.join(schemas)}")

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error loading test data: {e}")
        raise
    finally:
        conn.close()


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

    finally:
        # Drop the MindsDB Oracle connection after tests finish
        try:
            cleanup_sql = "DROP DATABASE IF EXISTS oracle_db;"
            requests.post(
                f"{mindsdb_url}/api/sql/query", json={"query": cleanup_sql}, timeout=10
            )
            logger.info("🧹 MindsDB connection cleaned up")
        except:
            pass  # Ignore cleanup errors


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


# The pytest_configure, pytest_collection_modifyitems, pytest_sessionstart,
# pytest_sessionfinish, and pytest_runtest_logreport functions can be copied
# verbatim from your PostgreSQL setup, as they are database-agnostic.


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
