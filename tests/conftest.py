import os
import psycopg2
import pytest
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def load_test_data():
    """Load test data from SQL file at the start of test session"""
    logger.info("🔧 Setting up test database...")
    
    # Connection parameters
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'dbname': os.getenv('POSTGRES_DB', 'test_db')
    }
    
    # Wait for PostgreSQL to be ready
    max_retries = 30
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**conn_params)
            conn.close()
            logger.info("✅ PostgreSQL connection successful")
            break
        except psycopg2.OperationalError as e:
            if i == max_retries - 1:
                raise Exception(f"PostgreSQL is not available after 30 seconds: {e}")
            time.sleep(1)
    
    # Load test data
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            # Get the path to test_data.sql
            sql_path = os.path.join(
                os.path.dirname(__file__),
                'test_data.sql'
            )
            
            if not os.path.exists(sql_path):
                raise FileNotFoundError(f"test_data.sql not found at {sql_path}")
            
            logger.info(f"📊 Loading test data from {sql_path}")
            
            # Read and execute the SQL file
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Execute the SQL content
            cur.execute(sql_content)
            conn.commit()
            
            # Verify core data was loaded
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
                except psycopg2.Error as e:
                    logger.warning(f"   - {table_name}: Error checking ({e})")
            
            # Verify schemas exist
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name IN ('sales', 'inventory', 'analytics')
                ORDER BY schema_name
            """)
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
    """Provide database connection parameters"""
    return {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'dbname': os.getenv('POSTGRES_DB', 'test_db')
    }


@pytest.fixture(scope="module")
def db_conn(db_connection_params):
    """Provide a database connection for the test module"""
    conn = psycopg2.connect(**db_connection_params)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def verify_mindsdb_ready():
    """Ensure MindsDB is ready before running tests"""
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
    """Create MindsDB database connection"""
    import requests
    
    mindsdb_url = verify_mindsdb_ready
    
    # Create the PostgreSQL database connection in MindsDB
    sql = """
    CREATE DATABASE IF NOT EXISTS postgresql_db
    WITH ENGINE = "postgres",
    PARAMETERS = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres", 
        "password": "postgres",
        "database": "test_db"
    };
    """
    
    logger.info("🔗 Creating MindsDB PostgreSQL connection...")
    
    try:
        resp = requests.post(
            f"{mindsdb_url}/api/sql/query",
            json={"query": sql},
            timeout=30
        )
        
        if resp.status_code != 200:
            raise Exception(f"Failed to create MindsDB connection: HTTP {resp.status_code}")
        
        data = resp.json()
        if data.get("type") == "error":
            raise Exception(f"MindsDB error: {data}")
        
        logger.info("✅ MindsDB PostgreSQL connection created")
        
        # Test the connection
        test_sql = "SELECT 1 as test_connection;"
        resp = requests.post(
            f"{mindsdb_url}/api/sql/query",
            json={"query": test_sql},
            timeout=10
        )
        
        if resp.status_code == 200 and resp.json().get("type") != "error":
            logger.info("✅ MindsDB connection test successful")
        else:
            logger.warning("⚠️ MindsDB connection test failed")
        
        yield mindsdb_url
        
    except Exception as e:
        logger.error(f"❌ Error setting up MindsDB connection: {e}")
        raise
    
    # Cleanup
    finally:
        try:
            cleanup_sql = "DROP DATABASE IF EXISTS postgresql_db;"
            requests.post(
                f"{mindsdb_url}/api/sql/query",
                json={"query": cleanup_sql},
                timeout=10
            )
            logger.info("🧹 MindsDB connection cleaned up")
        except:
            pass  # Ignore cleanup errors


@pytest.fixture(autouse=True)
def log_test_info(request):
    """Log test start and end information"""
    test_name = request.node.name
    logger.info(f"🧪 Starting test: {test_name}")
    
    start_time = time.time()
    yield
    end_time = time.time()
    
    duration = end_time - start_time
    logger.info(f"✅ Completed test: {test_name} ({duration:.2f}s)")


def pytest_configure(config):
    """Configure pytest settings"""
    # Add custom markers
    markers = [
        "basic: Basic connectivity tests",
        "schema: Schema access tests", 
        "complex: Complex query tests",
        "datatype: Data type tests",
        "error: Error handling tests",
        "performance: Performance tests",
        "inventory: Inventory management tests",
        "sales: Sales analytics tests",
        "slow: Tests that take longer to run"
    ]
    
    for marker in markers:
        config.addinivalue_line("markers", marker)


def pytest_collection_modifyitems(config, items):
    """Modify test items during collection"""
    # Add markers based on test class names
    for item in items:
        # Add markers based on class names
        class_markers = {
            "TestBasicConnectivity": "basic",
            "TestSchemaAccess": "schema", 
            "TestComplexQueries": "complex",
            "TestDataTypes": "datatype",
            "TestErrorHandling": "error",
            "TestPerformance": "performance",
            "TestInventoryManagement": "inventory",
            "TestSalesAnalytics": "sales"
        }
        
        for class_name, marker in class_markers.items():
            if class_name in item.nodeid:
                item.add_marker(getattr(pytest.mark, marker))
        
        # Mark performance tests as slow
        if "TestPerformance" in item.nodeid:
            item.add_marker(pytest.mark.slow)


def pytest_sessionstart(session):
    """Called after the Session object has been created"""
    logger.info("🚀 Starting MindsDB PostgreSQL Handler Test Suite")
    logger.info("=" * 60)


def pytest_sessionfinish(session, exitstatus):
    """Called after whole test run finished"""
    logger.info("=" * 60)
    if exitstatus == 0:
        logger.info("✅ All tests completed successfully!")
    else:
        logger.error(f"❌ Tests completed with exit status: {exitstatus}")


def pytest_runtest_logreport(report):
    """Called when a test report is created"""
    if report.when == "call":
        if report.outcome == "passed":
            logger.info(f"✅ {report.nodeid}")
        elif report.outcome == "failed":
            logger.error(f"❌ {report.nodeid}")
            if hasattr(report, 'longrepr'):
                logger.error(f"Error details: {report.longrepr}")
        elif report.outcome == "skipped":
            logger.info(f"⏭️ {report.nodeid}")


# Helper function for tests
def execute_sql_via_mindsdb(sql, mindsdb_url="http://localhost:47334", timeout=30):
    """Helper function to execute SQL via MindsDB API"""
    import requests
    
    resp = requests.post(
        f"{mindsdb_url}/api/sql/query", 
        json={"query": sql}, 
        timeout=timeout
    )
    
    if resp.status_code != 200:
        raise Exception(f"MindsDB API request failed with status {resp.status_code}: {resp.text}")
    
    data = resp.json()
    if data.get("type") == "error":
        raise Exception(f"MindsDB returned error: {data}")
    
    return data


# Make the helper function available to tests
pytest.execute_sql_via_mindsdb = execute_sql_via_mindsdb