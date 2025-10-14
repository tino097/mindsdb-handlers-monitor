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
        tables = [row[0] for row in result["data"]]
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

    def test_mssql_date_functions(self, mindsdb_connection):
        """Test MS SQL Server-specific date functions."""
        sql = f"""
            SELECT GETDATE() as current_date,
                   FORMAT(GETDATE(), 'yyyy-MM-dd') as formatted_date,
                   DATEADD(day, 7, GETDATE()) as next_week
            FROM {MSSQL_DB}.customer
            WHERE c_custkey = 1
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) == 1

    def test_mssql_string_functions(self, mindsdb_connection):
        """Test MS SQL Server string functions."""
        sql = f"""
            SELECT TOP 3
                SUBSTRING(r_name, 1, 3) as short_name,
                UPPER(r_name) as upper_name,
                LEN(r_name) as name_length
            FROM {MSSQL_DB}.region
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_mssql_numeric_functions(self, mindsdb_connection):
        """Test MS SQL Server numeric functions."""
        sql = f"""
            SELECT TOP 5
                ROUND(s_acctbal, 0) as rounded_bal,
                FLOOR(s_acctbal) as floor_bal,
                CEILING(s_acctbal) as ceiling_bal,
                s_suppkey % 10 as mod_value
            FROM {MSSQL_DB}.supplier
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_top_clause_pagination(self, mindsdb_connection):
        """Test MS SQL Server TOP clause for pagination."""
        sql = f"""
            SELECT TOP 3 * FROM {MSSQL_DB}.region
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) <= 3

    def test_window_functions(self, mindsdb_connection):
        """Test MS SQL Server window functions."""
        sql = f"""
            SELECT TOP 5
                c_custkey,
                c_acctbal,
                ROW_NUMBER() OVER (ORDER BY c_acctbal DESC) as row_num,
                RANK() OVER (ORDER BY c_acctbal DESC) as rank,
                NTILE(4) OVER (ORDER BY c_acctbal DESC) as quartile
            FROM {MSSQL_DB}.customer
            ORDER BY c_acctbal DESC
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) <= 5
