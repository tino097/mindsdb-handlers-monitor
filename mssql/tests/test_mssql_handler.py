"""
Test suite for MindsDB MS SQL Server handler.

This module tests the MS SQL Server integration with MindsDB,
covering basic connectivity, query execution, and TPC-H benchmark queries.
"""

import os
import logging
import pytest
from mindsdb.integrations.libs.response import RESPONSE_TYPE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def pytest_configure(config):
    """Register custom pytest markers for this test suite."""
    config.addinivalue_line(
        "markers",
        "basic: Basic connectivity and sanity tests",
    )
    config.addinivalue_line(
        "markers",
        "tpch: TPC-H benchmark query tests for the MS SQL handler",
    )


@pytest.fixture(scope="session")
def mindsdb_connection():
    """
    Create a MindsDB connection to MS SQL Server for testing.
    
    Returns:
        MSSQLHandler: An instance of the MindsDB MS SQL handler.
    """
    from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler
    
    connection_params = {
        "host": os.getenv("MSSQL_HOST", "localhost"),
        "port": int(os.getenv("MSSQL_PORT", "1433")),
        "user": os.getenv("MSSQL_USER", "testuser"),
        "password": os.getenv("MSSQL_PASSWORD", "TestUser@123"),
        "database": os.getenv("MSSQL_DATABASE", "TestDB"),
        "server": os.getenv("MSSQL_HOST", "localhost"),  # Some drivers need 'server'
    }
    
    logger.info(f"🔗 Connecting to MS SQL Server at {connection_params['host']}:{connection_params['port']}")
    logger.info(f"   Database: {connection_params['database']}, User: {connection_params['user']}")
    
    handler = SqlServerHandler(name="test_mssql", connection_data=connection_params)
    
    # Test the connection
    connection_status = handler.check_connection()
    if not connection_status.success:
        logger.error(f"❌ Connection failed: {connection_status.error_message}")
        # Try to provide more debugging info
        import pymssql
        try:
            logger.info("🔍 Attempting direct pymssql connection for debugging...")
            conn = pymssql.connect(
                server=connection_params['host'],
                user=connection_params['user'],
                password=connection_params['password'],
                database=connection_params['database'],
                port=connection_params['port']
            )
            conn.close()
            logger.info("✅ Direct pymssql connection succeeded")
        except Exception as e:
            logger.error(f"❌ Direct pymssql connection also failed: {e}")
    
    assert connection_status.success, f"❌ Failed to connect to MS SQL Server: {connection_status.error_message}"
    
    logger.info("✅ Successfully connected to MS SQL Server")
    
    yield handler
    
    # Cleanup
    handler.disconnect()
    logger.info("🔌 Disconnected from MS SQL Server")


@pytest.mark.basic
def test_connection(mindsdb_connection):
    """Test basic connection to MS SQL Server."""
    logger.info("🧪 Testing MS SQL Server connection...")
    
    response = mindsdb_connection.check_connection()
    
    assert response.success is True, f"Connection check failed: {response.error_message}"
    logger.info("✅ Connection test passed")


@pytest.mark.basic
def test_get_tables(mindsdb_connection):
    """Test retrieving table list from MS SQL Server."""
    logger.info("🧪 Testing get_tables...")
    
    response = mindsdb_connection.get_tables()
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    assert len(response.data_frame) > 0, "Expected at least one table"
    
    logger.info(f"✅ Found {len(response.data_frame)} tables")


@pytest.mark.basic
def test_simple_query(mindsdb_connection):
    """Test a simple SELECT query."""
    logger.info("🧪 Testing simple query...")
    
    query = "SELECT TOP 10 * FROM customers"
    response = mindsdb_connection.native_query(query)
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    
    logger.info(f"✅ Query returned {len(response.data_frame)} rows")


# TPC-H Benchmark Query Tests
@pytest.mark.tpch
def test_tpch_q1_pricing_summary(mindsdb_connection):
    """
    TPC-H Query 1: Pricing Summary Report
    Tests aggregation, filtering, and sorting capabilities.
    """
    logger.info("🧪 Running TPC-H Q1: Pricing Summary Report...")
    
    query = """
    SELECT 
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) as sum_qty,
        SUM(l_extendedprice) as sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        AVG(l_quantity) as avg_qty,
        AVG(l_extendedprice) as avg_price,
        AVG(l_discount) as avg_disc,
        COUNT(*) as count_order
    FROM lineitem
    WHERE l_shipdate <= DATEADD(day, -90, '1998-12-01')
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
    """
    
    response = mindsdb_connection.native_query(query)
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    assert len(response.data_frame) > 0, "Expected at least one result row"
    
    logger.info(f"✅ TPC-H Q1 completed: {len(response.data_frame)} result groups")


@pytest.mark.tpch
def test_tpch_q3_shipping_priority(mindsdb_connection):
    """
    TPC-H Query 3: Shipping Priority
    Tests joins, filtering, and complex aggregations.
    """
    logger.info("🧪 Running TPC-H Q3: Shipping Priority...")
    
    query = """
    SELECT TOP 10
        l_orderkey,
        SUM(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON l.l_orderkey = o.o_orderkey
    WHERE c.c_mktsegment = 'BUILDING'
        AND o.o_orderdate < '1995-03-15'
        AND l.l_shipdate > '1995-03-15'
    GROUP BY l_orderkey, o_orderdate, o_shippriority
    ORDER BY revenue DESC, o_orderdate
    """
    
    response = mindsdb_connection.native_query(query)
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    
    logger.info(f"✅ TPC-H Q3 completed: {len(response.data_frame)} orders found")


@pytest.mark.tpch
def test_tpch_q5_local_supplier_volume(mindsdb_connection):
    """
    TPC-H Query 5: Local Supplier Volume
    Tests multi-table joins and complex filtering.
    """
    logger.info("🧪 Running TPC-H Q5: Local Supplier Volume...")
    
    query = """
    SELECT 
        n.n_name,
        SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON l.l_orderkey = o.o_orderkey
    JOIN supplier s ON l.l_suppkey = s.s_suppkey
    JOIN nation n ON s.s_nationkey = n.n_nationkey
    JOIN region r ON n.n_regionkey = r.r_regionkey
    WHERE r.r_name = 'ASIA'
        AND o.o_orderdate >= '1994-01-01'
        AND o.o_orderdate < '1995-01-01'
        AND c.c_nationkey = s.s_nationkey
    GROUP BY n.n_name
    ORDER BY revenue DESC
    """
    
    response = mindsdb_connection.native_query(query)
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    
    logger.info(f"✅ TPC-H Q5 completed: {len(response.data_frame)} nations analyzed")


@pytest.mark.tpch
def test_tpch_q10_returned_item_reporting(mindsdb_connection):
    """
    TPC-H Query 10: Returned Item Reporting
    Tests complex joins with filtering on multiple conditions.
    """
    logger.info("🧪 Running TPC-H Q10: Returned Item Reporting...")
    
    query = """
    SELECT TOP 20
        c.c_custkey,
        c.c_name,
        SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue,
        c.c_acctbal,
        n.n_name,
        c.c_address,
        c.c_phone,
        c.c_comment
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON l.l_orderkey = o.o_orderkey
    JOIN nation n ON c.c_nationkey = n.n_nationkey
    WHERE o.o_orderdate >= '1993-10-01'
        AND o.o_orderdate < DATEADD(month, 3, '1993-10-01')
        AND l.l_returnflag = 'R'
    GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment
    ORDER BY revenue DESC
    """
    
    response = mindsdb_connection.native_query(query)
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    
    logger.info(f"✅ TPC-H Q10 completed: {len(response.data_frame)} customers with returns")


@pytest.mark.basic
def test_date_functions(mindsdb_connection):
    """Test MS SQL Server specific date functions."""
    logger.info("🧪 Testing MS SQL date functions...")
    
    query = """
    SELECT 
        GETDATE() as current_datetime,
        DATEADD(day, 7, GETDATE()) as next_week,
        DATEDIFF(day, '2024-01-01', GETDATE()) as days_since_new_year,
        FORMAT(GETDATE(), 'yyyy-MM-dd') as formatted_date
    """
    
    response = mindsdb_connection.native_query(query)
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    assert len(response.data_frame) == 1, "Expected exactly one result row"
    
    logger.info("✅ Date functions test passed")


@pytest.mark.basic
def test_string_functions(mindsdb_connection):
    """Test MS SQL Server string manipulation functions."""
    logger.info("🧪 Testing MS SQL string functions...")
    
    query = """
    SELECT TOP 5
        c_name,
        UPPER(c_name) as upper_name,
        LOWER(c_name) as lower_name,
        LEN(c_name) as name_length,
        SUBSTRING(c_name, 1, 5) as name_prefix
    FROM customer
    """
    
    response = mindsdb_connection.native_query(query)
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    
    logger.info("✅ String functions test passed")


@pytest.mark.basic
def test_window_functions(mindsdb_connection):
    """Test MS SQL Server window functions."""
    logger.info("🧪 Testing MS SQL window functions...")
    
    query = """
    SELECT TOP 10
        c_custkey,
        c_acctbal,
        ROW_NUMBER() OVER (ORDER BY c_acctbal DESC) as row_num,
        RANK() OVER (ORDER BY c_acctbal DESC) as rank,
        NTILE(4) OVER (ORDER BY c_acctbal DESC) as quartile
    FROM customer
    ORDER BY c_acctbal DESC
    """
    
    response = mindsdb_connection.native_query(query)
    
    assert response.type == RESPONSE_TYPE.TABLE, "Expected TABLE response type"
    assert response.data_frame is not None, "Expected data frame in response"
    
    logger.info("✅ Window functions test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])