# mssql/tests/test_mssql_handler.py
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest
from conftest import MSSQL_DB, execute_sql_via_mindsdb


@pytest.mark.handler
class TestMSSQLHandlerFunctionality:
    """Test MS SQL Server-specific handler functionality."""

    def test_table_listing(self, mindsdb_connection):
        """Test that we can list tables from MS SQL Server."""
        sql = f"SHOW TABLES FROM {MSSQL_DB}"
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

        # Should have 8 TPC-H tables
        tables = [str(row[0]).lower() for row in result["data"]]
        assert len(tables) == 8, f"Expected 8 tables, found {len(tables)}"
        expected_tables = [
            "region",
            "nation",
            "supplier",
            "part",
            "partsupp",
            "customer",
            "orders",
            "lineitem",
        ]
        for table in expected_tables:
            assert table in tables, f"Table {table} not found"

    def test_simple_select(self, mindsdb_connection):
        """Test simple SELECT query."""
        sql = f"""
            SELECT r_regionkey, r_name, r_comment
            FROM {MSSQL_DB}.region
            LIMIT 3
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) <= 3

    def test_string_functions(self, mindsdb_connection):
        """Test basic string functions."""
        sql = f"""
            SELECT 
                UPPER(r_name) as upper_name,
                LOWER(r_name) as lower_name
            FROM {MSSQL_DB}.region
            LIMIT 3
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_numeric_functions(self, mindsdb_connection):
        """Test basic numeric functions."""
        sql = f"""
            SELECT 
                ROUND(s_acctbal, 0) as rounded_bal,
                ABS(s_acctbal) as abs_bal
            FROM {MSSQL_DB}.supplier
            LIMIT 5
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_aggregation_functions(self, mindsdb_connection):
        """Test aggregation functions."""
        sql = f"""
            SELECT 
                COUNT(*) as region_count,
                COUNT(DISTINCT r_name) as distinct_regions
            FROM {MSSQL_DB}.region
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) == 1

    def test_where_clause(self, mindsdb_connection):
        """Test WHERE clause filtering."""
        sql = f"""
            SELECT r_regionkey, r_name
            FROM {MSSQL_DB}.region
            WHERE r_regionkey < 3
            ORDER BY r_regionkey
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_join_query(self, mindsdb_connection):
        """Test simple JOIN query."""
        sql = f"""
            SELECT 
                r.r_name as region_name,
                n.n_name as nation_name
            FROM {MSSQL_DB}.region r
            INNER JOIN {MSSQL_DB}.nation n ON r.r_regionkey = n.n_regionkey
            ORDER BY r.r_name, n.n_name
            LIMIT 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_group_by_query(self, mindsdb_connection):
        """Test GROUP BY with aggregations."""
        sql = f"""
            SELECT
                n.n_regionkey,
                COUNT(*) as nation_count
            FROM {MSSQL_DB}.nation n
            GROUP BY n.n_regionkey
            ORDER BY n.n_regionkey
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_order_by_query(self, mindsdb_connection):
        """Test ORDER BY clause."""
        sql = f"""
            SELECT s_suppkey, s_name, s_acctbal
            FROM {MSSQL_DB}.supplier
            ORDER BY s_acctbal DESC
            LIMIT 5
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) <= 5
