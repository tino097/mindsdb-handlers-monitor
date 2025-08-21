import os
import psycopg2
import pytest
import requests
import time
import logging
from decimal import Decimal
from datetime import datetime, date

# Get logger
logger = logging.getLogger(__name__)

# Constants
MINDSDB_API_URL = "http://localhost:47334"


def execute_sql_via_mindsdb(sql, timeout=30):
    """Helper function to execute SQL via MindsDB API"""
    resp = requests.post(
        f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=timeout
    )

    if resp.status_code != 200:
        raise Exception(
            f"MindsDB API request failed with status {resp.status_code}: {resp.text}"
        )

    data = resp.json()
    if data.get("type") == "error":
        raise Exception(f"MindsDB returned error: {data}")

    return data


@pytest.mark.basic
class TestBasicConnectivity:
    """Test basic database connectivity and setup"""

    def test_postgres_connection(self, db_conn):
        """Test direct PostgreSQL connection"""
        with db_conn.cursor() as cur:
            cur.execute("SELECT 1;")
            result = cur.fetchone()[0]
            assert result == 1, "Basic PostgreSQL query failed"

    def test_mindsdb_status(self, verify_mindsdb_ready):
        """Test MindsDB API status"""
        mindsdb_url = verify_mindsdb_ready
        resp = requests.get(f"{mindsdb_url}/api/status", timeout=10)
        assert (
            resp.status_code == 200
        ), f"MindsDB status check failed: {resp.status_code}"

    def test_mindsdb_postgres_connection(self, mindsdb_connection):
        """Test MindsDB can connect to PostgreSQL"""
        sql = "SELECT 1 as test_value;"
        data = execute_sql_via_mindsdb(sql)
        assert (
            "data" in data or "column_names" in data
        ), "MindsDB PostgreSQL connection failed"

    def test_original_test_table(self, mindsdb_connection):
        """Test the original simple test table"""
        sql = "SELECT COUNT(*) as row_count FROM postgresql_db.test_table;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error querying test_table: {data}"

        # Test actual data retrieval
        sql = "SELECT value FROM postgresql_db.test_table ORDER BY id LIMIT 3;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error retrieving test_table data: {data}"

    def test_database_version_info(self, mindsdb_connection):
        """Test retrieving PostgreSQL version through MindsDB"""
        sql = "SELECT version() as pg_version;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error getting PostgreSQL version: {data}"


@pytest.mark.schema
class TestSchemaAccess:
    """Test accessing different schemas through MindsDB"""

    def test_public_schema_access(self, mindsdb_connection):
        """Test accessing tables in public schema"""
        sql = "SELECT COUNT(*) as customer_count FROM postgresql_db.customers;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error accessing public schema: {data}"

    def test_sales_schema_access(self, mindsdb_connection):
        """Test accessing tables in sales schema"""
        sql = "SELECT COUNT(*) as rep_count FROM postgresql_db.sales.sales_reps;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error accessing sales schema: {data}"

    def test_inventory_schema_access(self, mindsdb_connection):
        """Test accessing tables in inventory schema"""
        sql = "SELECT COUNT(*) as warehouse_count FROM postgresql_db.inventory.warehouses;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error accessing inventory schema: {data}"

    def test_analytics_view_access(self, mindsdb_connection):
        """Test accessing views in analytics schema"""
        sql = "SELECT COUNT(*) as summary_count FROM postgresql_db.analytics.customer_summary;"
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error accessing analytics views: {data}"

    def test_customer_addresses_table(self, mindsdb_connection):
        """Test accessing customer addresses table"""
        sql = "SELECT COUNT(*) as address_count FROM postgresql_db.customer_addresses;"
        data = execute_sql_via_mindsdb(sql)
        assert (
            data.get("type") != "error"
        ), f"Error accessing customer_addresses: {data}"

    def test_schema_information_queries(self, mindsdb_connection):
        """Test information schema queries"""
        sql = """
        SELECT table_name, table_schema 
        FROM postgresql_db.information_schema.tables 
        WHERE table_schema IN ('public', 'sales', 'inventory') 
        ORDER BY table_schema, table_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error querying information_schema: {data}"


@pytest.mark.complex
class TestComplexQueries:
    """Test complex SQL queries with joins, aggregations, etc."""

    def test_inner_join_customers_orders(self, mindsdb_connection):
        """Test INNER JOIN between customers and orders"""
        sql = """
        SELECT 
            c.first_name,
            c.last_name,
            c.customer_type,
            COUNT(o.order_id) as order_count,
            SUM(o.total_amount) as total_spent
        FROM postgresql_db.customers c
        INNER JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_type
        ORDER BY total_spent DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in INNER JOIN query: {data}"

    def test_left_join_all_customers(self, mindsdb_connection):
        """Test LEFT JOIN to include customers without orders"""
        sql = """
        SELECT 
            c.first_name,
            c.last_name,
            c.email,
            c.customer_type,
            COALESCE(COUNT(o.order_id), 0) as order_count,
            COALESCE(SUM(o.total_amount), 0) as total_spent
        FROM postgresql_db.customers c
        LEFT JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.customer_type
        ORDER BY c.last_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in LEFT JOIN query: {data}"

    def test_multiple_join_order_details(self, mindsdb_connection):
        """Test multiple JOINs for complete order details"""
        sql = """
        SELECT 
            c.first_name || ' ' || c.last_name as customer_name,
            c.customer_type,
            o.order_date,
            o.order_status,
            p.product_name,
            p.category,
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
        assert data.get("type") != "error", f"Error in multiple JOIN query: {data}"

    def test_cross_schema_join(self, mindsdb_connection):
        """Test JOIN across different schemas"""
        sql = """
        SELECT 
            p.product_name,
            p.category,
            p.brand,
            w.warehouse_name,
            w.location,
            sl.quantity_on_hand,
            sl.quantity_reserved,
            (sl.quantity_on_hand - sl.quantity_reserved) AS quantity_available,
            sl.reorder_level,
            CASE
                WHEN quantity_available <= sl.reorder_level 
                THEN 'Reorder Needed'
                WHEN quantity_available <= sl.reorder_level * 2 
                THEN 'Low Stock'
                ELSE 'Stock OK'
            END as stock_status
        FROM postgresql_db.products p
        INNER JOIN postgresql_db.inventory.stock_levels sl ON p.product_id = sl.product_id
        INNER JOIN postgresql_db.inventory.warehouses w ON sl.warehouse_id = w.warehouse_id
        WHERE w.is_active = TRUE
        ORDER BY p.category, p.product_name, w.warehouse_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in cross-schema JOIN: {data}"

    def test_subquery_with_aggregation(self, mindsdb_connection):
        """Test subquery with aggregation functions"""
        sql = """
        SELECT 
            category,
            brand,
            AVG(price) as avg_price,
            COUNT(*) as product_count,
            (SELECT AVG(price) FROM postgresql_db.products) as overall_avg_price,
            ROUND(AVG(price) / (SELECT AVG(price) FROM postgresql_db.products) * 100, 2) as price_index
        FROM postgresql_db.products
        WHERE is_active = TRUE
        GROUP BY category, brand
        HAVING AVG(price) > (
            SELECT AVG(price) * 0.5 
            FROM postgresql_db.products
        )
        ORDER BY avg_price DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in subquery: {data}"

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
            LAG(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date) as previous_order_amount,
            LEAD(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date) as next_order_amount,
            AVG(total_amount) OVER (PARTITION BY customer_id) as customer_avg_order
        FROM postgresql_db.orders
        ORDER BY customer_id, order_date;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in window functions: {data}"

    def test_complex_analytics_view(self, mindsdb_connection):
        """Test complex analytics view query"""
        sql = """
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            AVG(total_spent) as avg_customer_value,
            SUM(total_orders) as total_orders_segment,
            MIN(first_order_date) as segment_first_order,
            MAX(last_order_date) as segment_last_order
        FROM postgresql_db.analytics.customer_summary
        WHERE total_orders > 0
        GROUP BY customer_segment
        ORDER BY avg_customer_value DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in analytics view query: {data}"

    def test_product_performance_analysis(self, mindsdb_connection):
        """Test product performance analytics"""
        sql = """
        SELECT 
            category,
            COUNT(*) as products_in_category,
            AVG(profit_margin_percent) as avg_margin,
            SUM(total_revenue) as category_revenue,
            SUM(total_quantity_sold) as category_units_sold,
            AVG(price) as avg_price_category
        FROM postgresql_db.analytics.product_performance
        WHERE total_quantity_sold > 0
        GROUP BY category
        ORDER BY category_revenue DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert (
            data.get("type") != "error"
        ), f"Error in product performance query: {data}"

    def test_cte_common_table_expressions(self, mindsdb_connection):
        """Test Common Table Expressions (CTEs)"""
        sql = """
        WITH customer_stats AS (
            SELECT 
                customer_id,
                COUNT(*) as order_count,
                SUM(total_amount) as total_spent,
                AVG(total_amount) as avg_order_value
            FROM postgresql_db.orders
            GROUP BY customer_id
        ),
        customer_categories AS (
            SELECT 
                customer_id,
                CASE 
                    WHEN total_spent > 1000 THEN 'High Value'
                    WHEN total_spent > 500 THEN 'Medium Value'
                    ELSE 'Low Value'
                END as value_category
            FROM customer_stats
        )
        SELECT 
            c.first_name,
            c.last_name,
            c.customer_type,
            cs.order_count,
            cs.total_spent,
            cc.value_category
        FROM postgresql_db.customers c
        INNER JOIN customer_stats cs ON c.customer_id = cs.customer_id
        INNER JOIN customer_categories cc ON c.customer_id = cc.customer_id
        ORDER BY cs.total_spent DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in CTE query: {data}"


@pytest.mark.datatype
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
                WHEN price > 1000 THEN 'Premium'
                WHEN price > 100 THEN 'Mid-range'
                ELSE 'Budget'
            END as price_category,
            CEIL(price) as price_ceiling,
            FLOOR(price) as price_floor,
            ABS(price - cost) as absolute_profit
        FROM postgresql_db.products
        WHERE price > 0 AND cost > 0
        ORDER BY profit_margin_percent DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in numeric operations: {data}"

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
            EXTRACT(DOW FROM order_date) as day_of_week,
            CASE 
                WHEN required_date < ship_date THEN 'Late'
                WHEN required_date = ship_date THEN 'On Time'
                WHEN ship_date IS NULL AND required_date < CURRENT_DATE THEN 'Overdue'
                ELSE 'On Schedule'
            END as delivery_status,
            AGE(CURRENT_DATE, order_date) as order_age
        FROM postgresql_db.orders
        ORDER BY order_date;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in date operations: {data}"

    def test_string_operations(self, mindsdb_connection):
        """Test string operations"""
        sql = """
        SELECT 
            UPPER(first_name || ' ' || last_name) as full_name_upper,
            LOWER(email) as email_lower,
            LENGTH(email) as email_length,
            SUBSTRING(email FROM 1 FOR POSITION('@' IN email) - 1) as username,
            SUBSTRING(email FROM POSITION('@' IN email) + 1) as domain,
            LEFT(phone, 3) as area_code,
            RIGHT(zip_code, 4) as zip_last_four,
            CASE 
                WHEN email LIKE '%gmail.com' THEN 'Gmail'
                WHEN email LIKE '%yahoo.com' THEN 'Yahoo'
                WHEN email LIKE '%company.com' THEN 'Company'
                ELSE 'Other'
            END as email_provider,
            INITCAP(city) as city_proper_case,
            customer_type
        FROM postgresql_db.customers
        ORDER BY last_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in string operations: {data}"

    def test_boolean_operations(self, mindsdb_connection):
        """Test boolean data type operations"""
        sql = """
        SELECT 
            customer_type,
            is_active,
            COUNT(*) as customer_count,
            AVG(CASE WHEN is_active THEN 1 ELSE 0 END) as active_rate,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_customers,
            SUM(CASE WHEN NOT is_active THEN 1 ELSE 0 END) as inactive_customers
        FROM postgresql_db.customers
        GROUP BY customer_type, is_active
        ORDER BY customer_type, is_active;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in boolean operations: {data}"

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
            ROUND(line_total / (quantity * unit_price) * 100, 2) as effective_discount_rate,
            quantity * unit_price as gross_amount,
            (quantity * unit_price) - line_total as discount_amount
        FROM postgresql_db.order_items
        WHERE quantity > 0 AND unit_price > 0
        ORDER BY line_total DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in generated columns: {data}"

    def test_array_and_json_operations(self, mindsdb_connection):
        """Test advanced data type operations"""
        sql = """
        SELECT 
            product_name,
            category,
            price,
            ARRAY[category, brand] as product_tags,
            CONCAT(category, ' - ', brand, ' - ', product_name) as full_description
        FROM postgresql_db.products
        WHERE category IS NOT NULL AND brand IS NOT NULL
        ORDER BY category, brand;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in array operations: {data}"


@pytest.mark.inventory
class TestInventoryManagement:
    """Test inventory-specific scenarios"""

    def test_stock_availability_check(self, mindsdb_connection):
        """Test stock availability across warehouses"""
        sql = """
        SELECT 
            p.product_name,
            p.category,
            p.sku,
            SUM(sl.quantity_on_hand) as total_stock,
            SUM(sl.quantity_reserved) as total_reserved,
            SUM((sl.quantity_on_hand - sl.quantity_reserved) AS quantity_available) as total_available,
            COUNT(sl.warehouse_id) as warehouses_with_stock,
            AVG(sl.reorder_level) as avg_reorder_level,
            MAX(sl.last_updated) as last_inventory_update
        FROM postgresql_db.products p
        LEFT JOIN postgresql_db.inventory.stock_levels sl ON p.product_id = sl.product_id
        WHERE p.is_active = TRUE
        GROUP BY p.product_id, p.product_name, p.category, p.sku
        ORDER BY total_available DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in stock availability query: {data}"

    def test_reorder_alerts(self, mindsdb_connection):
        """Test products needing reorder"""
        sql = """
        SELECT 
            p.product_name,
            p.sku,
            w.warehouse_name,
            w.location,
            sl.quantity_on_hand,
            sl.quantity_reserved,
            (sl.quantity_on_hand - sl.quantity_reserved) AS quantity_available,
            sl.reorder_level,
            sl.max_stock_level,
            (sl.reorder_level - (sl.quantity_on_hand - sl.quantity_reserved) AS quantity_available) as shortage_amount,
            CASE 
                WHEN (sl.quantity_on_hand - sl.quantity_reserved) AS quantity_available <= 0 THEN 'Out of Stock'
                WHEN (sl.quantity_on_hand - sl.quantity_reserved) AS quantity_available <= sl.reorder_level THEN 'Reorder Now'
                WHEN (sl.quantity_on_hand - sl.quantity_reserved) AS quantity_available <= sl.reorder_level * 1.5 THEN 'Low Stock Warning'
                ELSE 'Adequate Stock'
            END as stock_alert_level
        FROM postgresql_db.inventory.stock_levels sl
        JOIN postgresql_db.products p ON sl.product_id = p.product_id
        JOIN postgresql_db.inventory.warehouses w ON sl.warehouse_id = w.warehouse_id
        WHERE (sl.quantity_on_hand - sl.quantity_reserved) AS quantity_available <= sl.reorder_level AND w.is_active = TRUE
        ORDER BY stock_alert_level, shortage_amount DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in reorder alerts query: {data}"

    def test_inventory_movements(self, mindsdb_connection):
        """Test inventory movement tracking"""
        sql = """
        SELECT 
            p.product_name,
            p.sku,
            w.warehouse_name,
            im.movement_type,
            im.quantity,
            im.movement_date,
            im.reference_type,
            im.reference_id,
            im.notes,
            CASE 
                WHEN im.movement_type = 'in' THEN 'Stock Increase'
                WHEN im.movement_type = 'out' THEN 'Stock Decrease'
                WHEN im.movement_type = 'transfer' THEN 'Stock Transfer'
                ELSE 'Stock Adjustment'
            END as movement_description
        FROM postgresql_db.inventory.inventory_movements im
        JOIN postgresql_db.products p ON im.product_id = p.product_id
        JOIN postgresql_db.inventory.warehouses w ON im.warehouse_id = w.warehouse_id
        ORDER BY im.movement_date DESC, p.product_name;
        """
        data = execute_sql_via_mindsdb(sql)
        assert (
            data.get("type") != "error"
        ), f"Error in inventory movements query: {data}"

    def test_warehouse_utilization(self, mindsdb_connection):
        """Test warehouse capacity utilization"""
        sql = """
        SELECT 
            w.warehouse_name,
            w.location,
            w.capacity,
            w.manager_name,
            COUNT(DISTINCT sl.product_id) as unique_products,
            SUM(sl.quantity_on_hand) as total_units_stored,
            ROUND(SUM(sl.quantity_on_hand)::DECIMAL / w.capacity * 100, 2) as utilization_percent,
            CASE 
                WHEN SUM(sl.quantity_on_hand)::DECIMAL / w.capacity > 0.9 THEN 'Near Capacity'
                WHEN SUM(sl.quantity_on_hand)::DECIMAL / w.capacity > 0.7 THEN 'High Utilization'
                WHEN SUM(sl.quantity_on_hand)::DECIMAL / w.capacity > 0.5 THEN 'Moderate Utilization'
                ELSE 'Low Utilization'
            END as utilization_status
        FROM postgresql_db.inventory.warehouses w
        LEFT JOIN postgresql_db.inventory.stock_levels sl ON w.warehouse_id = sl.warehouse_id
        WHERE w.is_active = TRUE
        GROUP BY w.warehouse_id, w.warehouse_name, w.location, w.capacity, w.manager_name
        ORDER BY utilization_percent DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert (
            data.get("type") != "error"
        ), f"Error in warehouse utilization query: {data}"


@pytest.mark.sales
class TestSalesAnalytics:
    """Test sales-specific scenarios"""

    def test_sales_rep_performance(self, mindsdb_connection):
        """Test sales representative performance"""
        sql = """
        SELECT 
            sr.employee_id,
            sr.first_name || ' ' || sr.last_name as rep_name,
            sr.territory,
            sr.commission_rate,
            sr.hire_date,
            COUNT(st.target_id) as quarters_tracked,
            AVG(st.target_amount) as avg_target,
            AVG(st.actual_amount) as avg_actual,
            SUM(st.target_amount) as total_target,
            SUM(st.actual_amount) as total_actual,
            AVG(CASE 
                WHEN st.target_amount > 0 
                THEN (st.actual_amount / st.target_amount) * 100 
                ELSE 0 
            END) as avg_achievement_rate,
            EXTRACT(DAYS FROM AGE(CURRENT_DATE, sr.hire_date)) as days_employed
        FROM postgresql_db.sales.sales_reps sr
        LEFT JOIN postgresql_db.sales.sales_targets st ON sr.rep_id = st.rep_id
        WHERE sr.is_active = TRUE
        GROUP BY sr.rep_id, sr.employee_id, sr.first_name, sr.last_name, sr.territory, sr.commission_rate, sr.hire_date
        ORDER BY avg_achievement_rate DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert (
            data.get("type") != "error"
        ), f"Error in sales rep performance query: {data}"

    def test_territory_analysis(self, mindsdb_connection):
        """Test territory performance analysis"""
        sql = """
        SELECT 
            t.territory_name,
            t.region,
            t.country,
            sr.first_name || ' ' || sr.last_name as assigned_rep,
            sr.commission_rate,
            COUNT(DISTINCT CASE 
                WHEN t.territory_name LIKE '%California%' AND c.state = 'CA' THEN c.customer_id
                WHEN t.territory_name LIKE '%New York%' AND c.state = 'NY' THEN c.customer_id
                WHEN t.territory_name LIKE '%Texas%' AND c.state = 'TX' THEN c.customer_id
                WHEN t.territory_name LIKE '%Illinois%' AND c.state = 'IL' THEN c.customer_id
                WHEN t.territory_name LIKE '%Florida%' AND c.state = 'FL' THEN c.customer_id
                ELSE NULL
            END) as customers_in_territory,
            COUNT(DISTINCT st.target_id) as targets_set,
            AVG(st.target_amount) as avg_target_amount
        FROM postgresql_db.sales.territories t
        LEFT JOIN postgresql_db.sales.sales_reps sr ON t.rep_id = sr.rep_id
        LEFT JOIN postgresql_db.customers c ON (
            (t.territory_name LIKE '%California%' AND c.state = 'CA') OR
            (t.territory_name LIKE '%New York%' AND c.state = 'NY') OR
            (t.territory_name LIKE '%Texas%' AND c.state = 'TX') OR
            (t.territory_name LIKE '%Illinois%' AND c.state = 'IL') OR
            (t.territory_name LIKE '%Florida%' AND c.state = 'FL')
        )
        LEFT JOIN postgresql_db.sales.sales_targets st ON sr.rep_id = st.rep_id
        GROUP BY t.territory_id, t.territory_name, t.region, t.country, sr.first_name, sr.last_name, sr.commission_rate
        ORDER BY customers_in_territory DESC;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in territory analysis query: {data}"

    def test_quarterly_sales_targets(self, mindsdb_connection):
        """Test quarterly sales target analysis"""
        sql = """
        SELECT 
            st.year,
            st.quarter,
            COUNT(DISTINCT st.rep_id) as reps_with_targets,
            SUM(st.target_amount) as total_target,
            SUM(st.actual_amount) as total_actual,
            AVG(st.target_amount) as avg_target_per_rep,
            AVG(st.actual_amount) as avg_actual_per_rep,
            ROUND(SUM(st.actual_amount) / NULLIF(SUM(st.target_amount), 0) * 100, 2) as overall_achievement_rate,
            COUNT(CASE WHEN st.actual_amount >= st.target_amount THEN 1 END) as reps_meeting_target,
            COUNT(CASE WHEN st.actual_amount < st.target_amount THEN 1 END) as reps_missing_target
        FROM postgresql_db.sales.sales_targets st
        GROUP BY st.year, st.quarter
        ORDER BY st.year, st.quarter;
        """
        data = execute_sql_via_mindsdb(sql)
        assert data.get("type") != "error", f"Error in quarterly targets query: {data}"


@pytest.mark.error
class TestErrorHandling:
    """Test error scenarios and edge cases"""

    def test_invalid_table_name(self, mindsdb_connection):
        """Test querying non-existent table"""
        sql = "SELECT * FROM postgresql_db.non_existent_table;"
        resp = requests.post(
            f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30
        )
        data = resp.json()
        # Should return an error
        assert (
            data.get("type") == "error" or resp.status_code != 200
        ), "Expected error for non-existent table"

    def test_invalid_column_name(self, mindsdb_connection):
        """Test selecting non-existent column"""
        sql = "SELECT non_existent_column FROM postgresql_db.customers;"
        resp = requests.post(
            f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30
        )
        data = resp.json()
        # Should return an error
        assert (
            data.get("type") == "error" or resp.status_code != 200
        ), "Expected error for non-existent column"

    def test_invalid_schema_name(self, mindsdb_connection):
        """Test accessing non-existent schema"""
        sql = "SELECT * FROM postgresql_db.invalid_schema.some_table;"
        resp = requests.post(
            f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30
        )
        data = resp.json()
        # Should return an error
        assert (
            data.get("type") == "error" or resp.status_code != 200
        ), "Expected error for non-existent schema"

    def test_sql_syntax_error(self, mindsdb_connection):
        """Test SQL syntax error handling"""
        sql = "SELEC * FRM postgresql_db.customers;"  # Intentional typos
        resp = requests.post(
            f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30
        )
        data = resp.json()
        # Should return an error
        assert (
            data.get("type") == "error" or resp.status_code != 200
        ), "Expected error for syntax error"

    def test_division_by_zero(self, mindsdb_connection):
        """Test division by zero error handling"""
        sql = "SELECT 1/0 as division_error FROM postgresql_db.customers LIMIT 1;"
        resp = requests.post(
            f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30
        )
        data = resp.json()
        # Should handle gracefully (may return error or NULL/infinity depending on PostgreSQL settings)
        assert resp.status_code == 200, "Should handle division by zero gracefully"

    def test_invalid_data_type_conversion(self, mindsdb_connection):
        """Test invalid data type conversion"""
        sql = (
            "SELECT CAST('invalid_date' AS DATE) FROM postgresql_db.customers LIMIT 1;"
        )
        resp = requests.post(
            f"{MINDSDB_API_URL}/api/sql/query", json={"query": sql}, timeout=30
        )
        data = resp.json()
        # Should return an error
        assert (
            data.get("type") == "error" or resp.status_code != 200
        ), "Expected error for invalid date conversion"


@pytest.mark.performance
@pytest.mark.slow
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
            c.customer_type,
            COUNT(DISTINCT o.order_id) as order_count,
            COUNT(DISTINCT oi.item_id) as item_count,
            COUNT(DISTINCT p.product_id) as unique_products,
            SUM(oi.line_total) as total_revenue,
            AVG(o.total_amount) as avg_order_value,
            MAX(o.order_date) as last_order_date,
            STRING_AGG(DISTINCT p.category, ', ') as categories_purchased
        FROM postgresql_db.customers c
        LEFT JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        LEFT JOIN postgresql_db.order_items oi ON o.order_id = oi.order_id
        LEFT JOIN postgresql_db.products p ON oi.product_id = p.product_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.state, c.customer_type
        ORDER BY total_revenue DESC NULLS LAST;
        """
        start_time = time.time()
        data = execute_sql_via_mindsdb(sql, timeout=45)
        end_time = time.time()

        assert data.get("type") != "error", f"Error in large JOIN query: {data}"
        execution_time = end_time - start_time
        logger.info(f"Large JOIN query executed in {execution_time:.2f} seconds")
        assert execution_time < 30, f"Query took too long: {execution_time:.2f} seconds"

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
            COUNT(DISTINCT p.category) as product_categories_purchased,
            COUNT(DISTINCT p.brand) as brands_purchased,
            SUM(oi.quantity) as total_items_ordered,
            AVG(oi.unit_price) as avg_item_price,
            STDDEV(o.total_amount) as order_value_std_dev
        FROM postgresql_db.customers c
        LEFT JOIN postgresql_db.orders o ON c.customer_id = o.customer_id
        LEFT JOIN postgresql_db.order_items oi ON o.order_id = oi.order_id
        LEFT JOIN postgresql_db.products p ON oi.product_id = p.product_id
        WHERE c.state IS NOT NULL
        GROUP BY c.state, c.customer_type
        HAVING COUNT(DISTINCT o.order_id) > 0
        ORDER BY total_revenue DESC NULLS LAST;
        """
        start_time = time.time()
        data = execute_sql_via_mindsdb(sql, timeout=30)
        end_time = time.time()

        assert (
            data.get("type") != "error"
        ), f"Error in complex aggregation query: {data}"
        execution_time = end_time - start_time
        logger.info(
            f"Complex aggregation query executed in {execution_time:.2f} seconds"
        )
        assert execution_time < 20, f"Query took too long: {execution_time:.2f} seconds"

    def test_cross_schema_performance(self, mindsdb_connection):
        """Test cross-schema query performance"""
        sql = """
        SELECT 
            p.product_name,
            p.category,
            p.brand,
            p.price,
            p.cost,
            (p.price - p.cost) as profit_per_unit,
            SUM(sl.quantity_on_hand) as total_inventory,
            COUNT(DISTINCT w.warehouse_id) as warehouse_count,
            COUNT(DISTINCT oi.order_id) as times_ordered,
            COALESCE(SUM(oi.quantity), 0) as total_sold,
            COALESCE(SUM(oi.line_total), 0) as total_revenue,
            AVG(w.capacity) as avg_warehouse_capacity,
            STRING_AGG(DISTINCT w.location, ', ') as warehouse_locations,
            CASE 
                WHEN SUM(sl.quantity_on_hand) > 100 THEN 'High Stock'
                WHEN SUM(sl.quantity_on_hand) > 50 THEN 'Medium Stock'
                WHEN SUM(sl.quantity_on_hand) > 0 THEN 'Low Stock'
                ELSE 'Out of Stock'
            END as inventory_status
        FROM postgresql_db.products p
        LEFT JOIN postgresql_db.inventory.stock_levels sl ON p.product_id = sl.product_id
        LEFT JOIN postgresql_db.inventory.warehouses w ON sl.warehouse_id = w.warehouse_id
        LEFT JOIN postgresql_db.order_items oi ON p.product_id = oi.product_id
        WHERE p.is_active = TRUE
        GROUP BY p.product_id, p.product_name, p.category, p.brand, p.price, p.cost
        ORDER BY total_revenue DESC NULLS LAST;
        """
        start_time = time.time()
        data = execute_sql_via_mindsdb(sql, timeout=25)
        end_time = time.time()

        assert data.get("type") != "error", f"Error in cross-schema query: {data}"
        execution_time = end_time - start_time
        logger.info(f"Cross-schema query executed in {execution_time:.2f} seconds")
        assert execution_time < 15, f"Query took too long: {execution_time:.2f} seconds"

    def test_window_function_performance(self, mindsdb_connection):
        """Test window function performance with complex partitioning"""
        sql = """
        SELECT 
            o.order_id,
            o.customer_id,
            o.order_date,
            o.total_amount,
            c.customer_type,
            c.state,
            ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date) as customer_order_sequence,
            ROW_NUMBER() OVER (PARTITION BY c.state ORDER BY o.total_amount DESC) as state_order_rank,
            SUM(o.total_amount) OVER (PARTITION BY o.customer_id ORDER BY o.order_date 
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as customer_running_total,
            AVG(o.total_amount) OVER (PARTITION BY c.customer_type) as type_avg_order,
            LAG(o.total_amount, 1) OVER (PARTITION BY o.customer_id ORDER BY o.order_date) as previous_order_amount,
            LEAD(o.total_amount, 1) OVER (PARTITION BY o.customer_id ORDER BY o.order_date) as next_order_amount,
            PERCENT_RANK() OVER (ORDER BY o.total_amount) as order_value_percentile,
            NTILE(4) OVER (ORDER BY o.total_amount) as order_value_quartile
        FROM postgresql_db.orders o
        JOIN postgresql_db.customers c ON o.customer_id = c.customer_id
        ORDER BY o.customer_id, o.order_date;
        """
        start_time = time.time()
        data = execute_sql_via_mindsdb(sql, timeout=20)
        end_time = time.time()

        assert data.get("type") != "error", f"Error in window function query: {data}"
        execution_time = end_time - start_time
        logger.info(f"Window function query executed in {execution_time:.2f} seconds")
        assert execution_time < 15, f"Query took too long: {execution_time:.2f} seconds"
