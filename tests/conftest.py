import os
import psycopg2
import pytest
import time


@pytest.fixture(scope="session", autouse=True)
def load_test_data():
    """Load test data from SQL file at the start of test session"""
    print("\n🔧 Setting up test database...")
    
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
            break
        except psycopg2.OperationalError:
            if i == max_retries - 1:
                raise Exception("PostgreSQL is not available after 30 seconds")
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
            
            print(f"📊 Loading test data from {sql_path}")
            
            # Read and execute the SQL file
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Execute the SQL content
            cur.execute(sql_content)
            conn.commit()
            
            # Verify data was loaded
            cur.execute("SELECT COUNT(*) FROM customers;")
            customer_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM products;")
            product_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM orders;")
            order_count = cur.fetchone()[0]
            
            print(f"✅ Test data loaded successfully:")
            print(f"   - {customer_count} customers")
            print(f"   - {product_count} products") 
            print(f"   - {order_count} orders")
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Error loading test data: {e}")
        raise
    finally:
        conn.close()


@pytest.fixture(scope="session")
def verify_test_data():
    """Verify that test data is properly loaded"""
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'dbname': os.getenv('POSTGRES_DB', 'test_db')
    }
    
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            # Verify key tables exist and have data
            tables_to_check = [
                ('customers', 5),  # Expected minimum 5 customers
                ('products', 10),  # Expected minimum 10 products
                ('orders', 5),     # Expected minimum 5 orders
                ('test_table', 3)  # Original test table
            ]
            
            for table_name, min_expected in tables_to_check:
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cur.fetchone()[0]
                if count < min_expected:
                    raise Exception(f"Table {table_name} has {count} rows, expected at least {min_expected}")
            
            # Verify schemas exist
            schemas_to_check = ['sales', 'inventory', 'analytics']
            for schema in schemas_to_check:
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.schemata 
                    WHERE schema_name = %s;
                """, (schema,))
                if cur.fetchone()[0] == 0:
                    raise Exception(f"Schema {schema} does not exist")
            
            # Verify views exist
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.views 
                WHERE table_schema = 'analytics' AND table_name = 'customer_summary';
            """)
            if cur.fetchone()[0] == 0:
                raise Exception("Analytics view customer_summary does not exist")
            
            print("✅ All test data verification checks passed")
            
    finally:
        conn.close()


@pytest.fixture(scope="function")
def db_transaction():
    """Provide a database transaction that can be rolled back after each test"""
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'dbname': os.getenv('POSTGRES_DB', 'test_db')
    }
    
    conn = psycopg2.connect(**conn_params)
    trans = conn.begin()
    
    try:
        yield conn
    finally:
        trans.rollback()
        conn.close()


# Configure pytest markers
def pytest_configure(config):
    """Configure custom pytest markers"""
    config.addinivalue_line("markers", "basic: Basic connectivity tests")
    config.addinivalue_line("markers", "schema: Schema access tests")
    config.addinivalue_line("markers", "complex: Complex query tests")
    config.addinivalue_line("markers", "datatype: Data type tests")
    config.addinivalue_line("markers", "error: Error handling tests")
    config.addinivalue_line("markers", "performance: Performance tests")
    config.addinivalue_line("markers", "inventory: Inventory management tests")
    config.addinivalue_line("markers", "sales: Sales analytics tests")


# Test collection hooks
def pytest_collection_modifyitems(config, items):
    """Modify test items during collection"""
    # Add markers based on test class names
    for item in items:
        if "TestBasicConnectivity" in item.nodeid:
            item.add_marker(pytest.mark.basic)
        elif "TestSchemaAccess" in item.nodeid:
            item.add_marker(pytest.mark.schema)
        elif "TestComplexQueries" in item.nodeid:
            item.add_marker(pytest.mark.complex)
        elif "TestDataTypes" in item.nodeid:
            item.add_marker(pytest.mark.datatype)
        elif "TestErrorHandling" in item.nodeid:
            item.add_marker(pytest.mark.error)
        elif "TestPerformance" in item.nodeid:
            item.add_marker(pytest.mark.performance)
        elif "TestInventoryManagement" in item.nodeid:
            item.add_marker(pytest.mark.inventory)
        elif "TestSalesAnalytics" in item.nodeid:
            item.add_marker(pytest.mark.sales)


def pytest_sessionstart(session):
    """Called after the Session object has been created"""
    print("\n🚀 Starting MindsDB PostgreSQL Handler Test Suite")
    print("=" * 60)


def pytest_sessionfinish(session, exitstatus):
    """Called after whole test run finished"""
    print("\n" + "=" * 60)
    if exitstatus == 0:
        print("✅ All tests completed successfully!")
    else:
        print(f"❌ Tests completed with exit status: {exitstatus}")
    print("🧹 Cleaning up test environment...")


def pytest_runtest_setup(item):
    """Called to perform the setup phase for a test item"""
    # Add any per-test setup logic here if needed
    pass


def pytest_runtest_teardown(item, nextitem):
    """Called to perform the teardown phase for a test item"""
    # Add any per-test cleanup logic here if needed
    pass