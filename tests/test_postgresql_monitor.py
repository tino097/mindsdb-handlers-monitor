import os
import psycopg2
import pytest
import requests
import time

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "test_db")
MINDSDB_API_URL = "http://localhost:47334"


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )


def execute_sql_via_mindsdb(sql):
    """Helper function to execute SQL via MindsDB API"""
    resp = requests.post(f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30)
    assert resp.status_code == 200, f"Request failed with status {resp.status_code}"
    data = resp.json()
    assert data.get("type") != "error", f"MindsDB returned error: {data}"
    return data


@pytest.fixture(scope="module")
def db_conn():
    conn = get_connection()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def mindsdb_connection():
    """Create MindsDB database connection"""
    # Wait for MindsDB to be ready
    max_retries = 30
    for i in range(max_retries):
        try:
            resp = requests.get(f"{MINDSDB_API_URL}/api/status", timeout=5)
            if resp.status_code == 200:
                break
        except requests.exceptions.RequestException:
            pass
        if i < max_retries - 1:
            time.sleep(1)
        else:
            pytest.fail("MindsDB is not ready after 30 seconds")

    sql = """
    CREATE DATABASE postgresql_db
    WITH ENGINE = "postgres",
    PARAMETERS = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres", 
        "password": "postgres",
        "database": "test_db"
    };
    """
    execute_sql_via_mindsdb(sql)
    yield
    
    # Cleanup
    try:
        execute_sql_via_mindsdb("DROP DATABASE postgresql_db;")
    except:
        pass  # Ignore cleanup errors


class TestBasicConnectivity:
    """Test basic database connectivity and setup"""
    
    def test_postgres_connection(self, db_conn):
        """Test direct PostgreSQL connection"""
        with db_conn.cursor() as cur:
            cur.execute("SELECT 1;")
            assert cur.fetchone()[0] == 1
    
    def test_mindsdb_status(self):
        """Test MindsDB API status"""
        resp = requests.get(f"{MINDSDB_API_URL}/api/status", timeout=10)
        assert resp.status_code == 200
    
    def test_mindsdb_postgres_connection(self, mindsdb_connection):
        """Test MindsDB can connect to PostgreSQL"""
        sql = "SELECT 1 as test_value;"
        data = execute_sql_via_mindsdb(sql)
        assert 'data' in data or 'column_names' in data

    def test_original_test_table(self, mindsdb_connection):
        """Test the original simple test table"""
        sql = "SELECT COUNT(*) as row_count FROM postgresql_db.test_table;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"


class TestSchemaAccess:
    """Test accessing different schemas through MindsDB"""
    
    def test_public_schema_access(self, mindsdb_connection):
        """Test accessing tables in public schema"""
        sql = "SELECT COUNT(*) as customer_count FROM postgresql_db.customers;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_sales_schema_access(self, mindsdb_connection):
        """Test accessing tables in sales schema"""
        sql = "SELECT COUNT(*) as rep_count FROM postgresql_db.sales.sales_reps;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_inventory_schema_access(self, mindsdb_connection):
        """Test accessing tables in inventory schema"""
        sql = "SELECT COUNT(*) as warehouse_count FROM postgresql_db.inventory.warehouses;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_analytics_view_access(self, mindsdb_connection):
        """Test accessing views in analytics schema"""
        sql = "SELECT COUNT(*) as summary_count FROM postgresql_db.analytics.customer_summary;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"

    def test_customer_addresses_table(self, mindsdb_connection):
        """Test accessing customer addresses table"""
        sql = "SELECT COUNT(*) as address_count FROM postgresql_db.customer_addresses;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"


class TestComplexQueries:
    """Test complex SQL queries with joins, aggregations, etc."""
    
    def test_inner_join_customers_orders(self, mindsdb_connection):
        """Test INNER JOIN between customers and orders"""
        sql = """
        SELECT 
            c.first_name,
            c.last_name,
            COUNT(o.order_id) as order_count,
            SUM(o.total_amount) as total_spent
        FROM postgresql_db.customers c
        INNER JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name
        ORDER BY total_spent DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_left_join_all_customers(self, mindsdb_connection):
        """Test LEFT JOIN to include customers without orders"""
        sql = """
        SELECT 
            c.first_name,
            c.last_name,
            c.email,
            COALESCE(COUNT(o.order_id), 0) as order_count,
            COALESCE(SUM(o.total_amount), 0) as total_spent
        FROM postgresql_db.customers c
        LEFT JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email
        ORDER BY c.last_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_multiple_join_order_details(self, mindsdb_connection):
        """Test multiple JOINs for complete order details"""
        sql = """
        SELECT 
            c.first_name || ' ' || c.last_name as customer_name,
            o.order_date,
            p.product_name,
            oi.quantity,
            oi.unit_price,
            oi.line_total
        FROM postgresql_db.customers c
        INNER JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        INNER JOIN postgresql_db.order_items oi ON o.order_id = oi.order_id
        INNER JOIN postgresql_db.products p ON oi.product_id = p.product_id
        WHERE o.order_status IN ('shipped', 'delivered')
        ORDER BY o.order_date DESC, customer_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_cross_schema_join(self, mindsdb_connection):
        """Test JOIN across different schemas"""
        sql = """
        SELECT 
            p.product_name,
            p.category,
            w.warehouse_name,
            sl.quantity_on_hand,
            sl.reorder_level,
            CASE 
                WHEN sl.quantity_on_hand <= sl.reorder_level THEN 'Reorder Needed'
                ELSE 'Stock OK'
            END as stock_status
        FROM postgresql_db.products p
        INNER JOIN postgresql_db.inventory.stock_levels sl ON p.product_id = sl.product_id
        INNER JOIN postgresql_db.inventory.warehouses w ON sl.warehouse_id = w.warehouse_id
        ORDER BY p.category, p.product_name, w.warehouse_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_subquery_with_aggregation(self, mindsdb_connection):
        """Test subquery with aggregation functions"""
        sql = """
        SELECT 
            category,
            AVG(price) as avg_price,
            COUNT(*) as product_count,
            (SELECT AVG(price) FROM postgresql_db.products) as overall_avg_price
        FROM postgresql_db.products
        GROUP BY category
        HAVING AVG(price) > (
            SELECT AVG(price) * 0.5 
            FROM postgresql_db.products
        )
        ORDER BY avg_price DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_window_functions(self, mindsdb_connection):
        """Test window functions"""
        sql = """
        SELECT 
            customer_id,
            order_date,
            total_amount,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_sequence,
            SUM(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
            LAG(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date) as previous_order_amount
        FROM postgresql_db.orders
        ORDER BY customer_id, order_date;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"

    def test_complex_analytics_view(self, mindsdb_connection):
        """Test complex analytics view query"""
        sql = """
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            AVG(total_spent) as avg_customer_value,
            SUM(total_orders) as total_orders_segment
        FROM postgresql_db.analytics.customer_summary
        GROUP BY customer_segment
        ORDER BY avg_customer_value DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"

    def test_product_performance_analysis(self, mindsdb_connection):
        """Test product performance analytics"""
        sql = """
        SELECT 
            category,
            COUNT(*) as products_in_category,
            AVG(profit_margin_percent) as avg_margin,
            SUM(total_revenue) as category_revenue
        FROM postgresql_db.analytics.product_performance
        WHERE total_quantity_sold > 0
        GROUP BY category
        ORDER BY category_revenue DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"


class TestDataTypes:
    """Test various PostgreSQL data types and operations"""
    
    def test_numeric_operations(self, mindsdb_connection):
        """Test numeric data type operations"""
        sql = """
        SELECT 
            product_name,
            price,
            cost,
            ROUND(price - cost, 2) as profit,
            ROUND((price - cost) / price * 100, 2) as profit_margin_percent,
            CASE 
                WHEN price > 500 THEN 'Premium'
                WHEN price > 100 THEN 'Mid-range'
                ELSE 'Budget'
            END as price_category
        FROM postgresql_db.products
        ORDER BY profit_margin_percent DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_date_operations(self, mindsdb_connection):
        """Test date/timestamp operations"""
        sql = """
        SELECT 
            order_id,
            order_date,
            ship_date,
            required_date,
            CASE 
                WHEN ship_date IS NULL THEN NULL
                ELSE ship_date - order_date
            END as days_to_ship,
            EXTRACT(MONTH FROM order_date) as order_month,
            EXTRACT(YEAR FROM order_date) as order_year,
            CASE 
                WHEN required_date < ship_date THEN 'Late'
                WHEN required_date = ship_date THEN 'On Time'
                WHEN ship_date IS NULL AND required_date < CURRENT_DATE THEN 'Overdue'
                ELSE 'On Schedule'
            END as delivery_status
        FROM postgresql_db.orders
        ORDER BY order_date;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"
    
    def test_string_operations(self, mindsdb_connection):
        """Test string operations"""
        sql = """
        SELECT 
            UPPER(first_name || ' ' || last_name) as full_name_upper,
            LENGTH(email) as email_length,
            SUBSTRING(email FROM 1 FOR POSITION('@' IN email) - 1) as username,
            SUBSTRING(email FROM POSITION('@' IN email) + 1) as domain,
            CASE 
                WHEN email LIKE '%gmail.com' THEN 'Gmail'
                WHEN email LIKE '%yahoo.com' THEN 'Yahoo'
                WHEN email LIKE '%company.com' THEN 'Company'
                ELSE 'Other'
            END as email_provider,
            customer_type
        FROM postgresql_db.customers
        ORDER BY last_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"

    def test_boolean_operations(self, mindsdb_connection):
        """Test boolean data type operations"""
        sql = """
        SELECT 
            customer_type,
            is_active,
            COUNT(*) as customer_count,
            AVG(CASE WHEN is_active THEN 1 ELSE 0 END) as active_rate
        FROM postgresql_db.customers
        GROUP BY customer_type, is_active
        ORDER BY customer_type, is_active;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"

    def test_generated_columns(self, mindsdb_connection):
        """Test generated/computed columns"""
        sql = """
        SELECT 
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_percent,
            line_total,
            ROUND(line_total / (quantity * unit_price) * 100, 2) as effective_discount_rate
        FROM postgresql_db.order_items
        WHERE quantity > 0
        ORDER BY line_total DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"


class TestInventoryManagement:
    """Test inventory-specific scenarios"""

    def test_stock_availability_check(self, mindsdb_connection):
        """Test stock availability across warehouses"""
        sql = """
        SELECT 
            p.product_name,
            SUM(sl.quantity_on_hand) as total_stock,
            SUM(sl.quantity_reserved) as total_reserved,
            SUM(sl.quantity_available) as total_available,
            COUNT(sl.warehouse_id) as warehouses_with_stock
        FROM postgresql_db.products p
        LEFT JOIN postgresql_db.inventory.stock_levels sl ON p.product_id = sl.product_id
        GROUP BY p.product_id, p.product_name
        ORDER BY total_available DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"

    def test_reorder_alerts(self, mindsdb_connection):
        """Test products needing reorder"""
        sql = """
        SELECT 
            p.product_name,
            w.warehouse_name,
            sl.quantity_on_hand,
            sl.reorder_level,
            sl.max_stock_level,
            (sl.reorder_level - sl.quantity_on_hand) as reorder_quantity
        FROM postgresql_db.inventory.stock_levels sl
        JOIN postgresql_db.products p ON sl.product_id = p.product_id
        JOIN postgresql_db.inventory.warehouses w ON sl.warehouse_id = w.warehouse_id
        WHERE sl.quantity_on_hand <= sl.reorder_level
        ORDER BY reorder_quantity DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"

    def test_inventory_movements(self, mindsdb_connection):
        """Test inventory movement tracking"""
        sql = """
        SELECT 
            p.product_name,
            w.warehouse_name,
            im.movement_type,
            im.quantity,
            im.movement_date,
            im.reference_type,
            im.notes
        FROM postgresql_db.inventory.inventory_movements im
        JOIN postgresql_db.products p ON im.product_id = p.product_id
        JOIN postgresql_db.inventory.warehouses w ON im.warehouse_id = w.warehouse_id
        ORDER BY im.movement_date DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"


class TestSalesAnalytics:
    """Test sales-specific scenarios"""

    def test_sales_rep_performance(self, mindsdb_connection):
        """Test sales representative performance"""
        sql = """
        SELECT 
            sr.first_name || ' ' || sr.last_name as rep_name,
            sr.territory,
            sr.commission_rate,
            COUNT(st.target_id) as quarters_tracked,
            AVG(st.target_amount) as avg_target,
            AVG(st.actual_amount) as avg_actual,
            AVG(CASE 
                WHEN st.target_amount > 0 
                THEN (st.actual_amount / st.target_amount) * 100 
                ELSE 0 
            END) as avg_achievement_rate
        FROM postgresql_db.sales.sales_reps sr
        LEFT JOIN postgresql_db.sales.sales_targets st ON sr.rep_id = st.rep_id
        WHERE sr.is_active = TRUE
        GROUP BY sr.rep_id, sr.first_name, sr.last_name, sr.territory, sr.commission_rate
        ORDER BY avg_achievement_rate DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"

    def test_territory_analysis(self, mindsdb_connection):
        """Test territory performance analysis"""
        sql = """
        SELECT 
            t.territory_name,
            t.region,
            sr.first_name || ' ' || sr.last_name as assigned_rep,
            COUNT(DISTINCT c.customer_id) as customers_in_territory
        FROM postgresql_db.sales.territories t
        LEFT JOIN postgresql_db.sales.sales_reps sr ON t.rep_id = sr.rep_id
        LEFT JOIN postgresql_db.customers c ON c.state = 
            CASE 
                WHEN t.territory_name = 'California' THEN 'CA'
                WHEN t.territory_name = 'New York/New Jersey' THEN 'NY'
                WHEN t.territory_name = 'Texas' THEN 'TX'
                WHEN t.territory_name = 'Illinois/Wisconsin' THEN 'IL'
                WHEN t.territory_name = 'Florida/Georgia' THEN 'FL'
                ELSE NULL
            END
        GROUP BY t.territory_id, t.territory_name, t.region, sr.first_name, sr.last_name
        ORDER BY customers_in_territory DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error"


class TestErrorHandling:
    """Test error scenarios and edge cases"""
    
    def test_invalid_table_name(self, mindsdb_connection):
        """Test querying non-existent table"""
        sql = "SELECT * FROM postgresql_db.non_existent_table;"
        resp = requests.post(f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30)
        data = resp.json()
        # Should return an error
        assert data.get("type") == "error" or resp.status_code != 200
    
    def test_invalid_column_name(self, mindsdb_connection):
        """Test selecting non-existent column"""
        sql = "SELECT non_existent_column FROM postgresql_db.customers;"
        resp = requests.post(f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30)
        data = resp.json()
        # Should return an error
        assert data.get("type") == "error" or resp.status_code != 200
    
    def test_invalid_schema_name(self, mindsdb_connection):
        """Test accessing non-existent schema"""
        sql = "SELECT * FROM postgresql_db.invalid_schema.some_table;"
        resp = requests.post(f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30)
        data = resp.json()
        # Should return an error
        assert data.get("type") == "error" or resp.status_code != 200

    def test_sql_syntax_error(self, mindsdb_connection):
        """Test SQL syntax error handling"""
        sql = "SELEC * FRM postgresql_db.customers;"  # Intentional typos
        resp = requests.post(f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30)
        data = resp.json()
        # Should return an error
        assert data.get("type") == "error" or resp.status_code != 200


class TestPerformance:
    """Test performance scenarios"""
    
    def test_large_join_performance(self, mindsdb_connection):
        """Test performance of large JOIN operations"""
        sql = """
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.city,
            c.state,
            COUNT(DISTINCT o.order_id) as order_count,
            COUNT(DISTINCT oi.item_id) as item_count,
            COUNT(DISTINCT p.product_id) as unique_products,
            SUM(oi.line_total) as total_revenue
        FROM postgresql_db.customers c
        LEFT JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        LEFT JOIN postgresql_db.order_items oi ON o.order_id = oi.order_id
        LEFT JOIN postgresql_db.products p ON oi.product_id = p.product_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.state
        ORDER BY total_revenue DESC;
        """
        start_time = time.time()
        data = execute_sql_via_mindsdb(sql)
        end_time = time.time()
        
        assert data.get("type") != "error"
        execution_time = end_time - start_time
        print(f"Large JOIN query executed in {execution_time:.2f} seconds")
        assert execution_time < 30  # Should complete within 30 seconds
    
    def test_complex_aggregation_performance(self, mindsdb_connection):
        """Test complex aggregation query performance"""
        sql = """
        SELECT 
            c.state,
            c.customer_type,
            COUNT(DISTINCT c.customer_id) as customer_count,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.total_amount) as total_revenue,
            AVG(o.total_amount) as avg_order_value,
            MIN(o.order_date) as first_order,
            MAX(o.order_date) as last_order,
            COUNT(DISTINCT p.category) as product_categories_purchased
        FROM postgresql_db.customers c
        LEFT JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        LEFT JOIN postgresql_db.order_items oi ON o.order_id = oi.order_id
        LEFT JOIN postgresql_db.products p ON oi.product_id = p.product_id
        WHERE c.state IS NOT NULL
        GROUP BY c.state, c.customer_type
        ORDER BY total_revenue DESC NULLS LAST;
        """
        start_time = time.time()
        data = execute_sql_via_mindsdb(sql)
        end_time = time.time()
        
        assert data.get("type") != "error"
        execution_time = end_time - start_time
        print(f"Complex aggregation query executed in {execution_time:.2f} seconds")
        assert execution_time < 20  # Should complete within 20 seconds

    def test_cross_schema_performance(self, mindsdb_connection):
        """Test cross-schema query performance"""
        sql = """
        SELECT 
            p.product_name,
            p.category,
            p.price,
            SUM(sl.quantity_on_hand) as total_inventory,
            COUNT(DISTINCT w.warehouse_id) as warehouse_count,
            COUNT(DISTINCT oi.order_id) as times_ordered,
            SUM(oi.quantity) as total_sold,
            (p.price * SUM(oi.quantity)) as total_revenue
        FROM postgresql_db.products p
        LEFT JOIN postgresql_db.inventory.stock_levels sl ON p.product_id = sl.product_id
        LEFT JOIN postgresql_db.inventory.warehouses w ON sl.warehouse_id = w.warehouse_id
        LEFT JOIN postgresql_db.order_items oi ON p.product_id = oi.product_id
        GROUP BY p.product_id, p.product_name, p.category, p.price
        ORDER BY total_revenue DESC NULLS LAST;
        """
        start_time = time.time()
        data = execute_sql_via_mindsdb(sql)
        end_time = time.time()
        
        assert data.get("type") != "error"
        execution_time = end_time - start_time
        print(f"Cross-schema query executed in {execution_time:.2f} seconds")
        assert execution_time < 15  # Should complete within 15 seconds