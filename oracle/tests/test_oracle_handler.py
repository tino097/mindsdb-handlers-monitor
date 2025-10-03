# oracle/tests/test_oracle_handler.py
import pytest
from conftest import ORACLE_TPCH_DB, execute_sql_via_mindsdb


@pytest.mark.handler
class TestOracleHandlerFunctionality:
    """Test Oracle-specific handler functionality."""

    def test_table_listing(self, mindsdb_connection):
        """Test that we can list tables from Oracle."""
        sql = f"SHOW TABLES FROM {ORACLE_TPCH_DB}"
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

        # Should have 8 TPC-H tables
        tables = [row[0] for row in result["data"]]
        expected_tables = [
            "REGION",
            "NATION",
            "SUPPLIER",
            "PART",
            "PARTSUPP",
            "CUSTOMER",
            "ORDERS",
            "LINEITEM",
        ]
        for table in expected_tables:
            assert table in tables, f"Table {table} not found"

    def test_describe_table(self, mindsdb_connection):
        """Test DESCRIBE functionality."""
        sql = f"DESCRIBE {ORACLE_TPCH_DB}.REGION"
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) > 0

    def test_oracle_date_functions(self, mindsdb_connection):
        """Test Oracle-specific date functions."""
        sql = f"""
            SELECT SYSDATE as current_date,
                   TO_CHAR(SYSDATE, 'YYYY-MM-DD') as formatted_date
            FROM {ORACLE_TPCH_DB}.dual
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) == 1

    def test_oracle_string_functions(self, mindsdb_connection):
        """Test Oracle string functions."""
        sql = f"""
            SELECT SUBSTR(r_name, 1, 3) as short_name,
                   UPPER(r_name) as upper_name,
                   LENGTH(r_name) as name_length
            FROM {ORACLE_TPCH_DB}.REGION
            WHERE ROWNUM <= 3
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_oracle_numeric_functions(self, mindsdb_connection):
        """Test Oracle numeric functions."""
        sql = f"""
            SELECT ROUND(s_acctbal, 0) as rounded_bal,
                   TRUNC(s_acctbal, 1) as truncated_bal,
                   MOD(s_suppkey, 10) as mod_value
            FROM {ORACLE_TPCH_DB}.SUPPLIER
            WHERE ROWNUM <= 5
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_rownum_pagination(self, mindsdb_connection):
        """Test Oracle ROWNUM for pagination."""
        sql = f"""
            SELECT * FROM {ORACLE_TPCH_DB}.REGION
            WHERE ROWNUM <= 3
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) <= 3
