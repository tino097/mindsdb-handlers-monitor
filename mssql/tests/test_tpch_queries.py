# mssql/tests/test_tpch_queries.py
import os
import sys
import time
import logging
from datetime import datetime
from typing import Any, Dict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest
from conftest import MSSQL_DB, execute_sql_via_mindsdb


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# TPC‑H Query Tests
# -----------------------------------------------------------------------------


@pytest.mark.tpch
class TestTPCHQueries:
    """Execute TPC‑H benchmark queries through MindsDB against MS SQL Server.

    Each test method corresponds to one of the official TPC‑H queries adapted
    for MindsDB-compatible SQL syntax. The queries reference tables owned by the SQL
    Server user connected through MindsDB. The database name is injected into
    the query using an f-string and the `MSSQL_DB` constant.
    
    Note: Uses standard SQL (LIMIT, EXTRACT) instead of T-SQL (TOP, YEAR) for MindsDB compatibility.
    """

    def test_q01_pricing_summary(self, mindsdb_connection):
        """Query 1: Pricing Summary Report"""
        sql = f"""
            SELECT l_returnflag, l_linestatus,
                SUM(l_quantity) as sum_qty,
                SUM(l_extendedprice) as sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                AVG(l_quantity) as avg_qty,
                AVG(l_extendedprice) as avg_price,
                AVG(l_discount) as avg_disc,
                COUNT(*) as count_order
            FROM {MSSQL_DB}.lineitem
            WHERE l_shipdate <= '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """
        result = execute_sql_via_mindsdb(sql)
        logger.info(f"Q1 Pricing Summary result: {result}")
        assert len(result.get("data", [])) > 0, "Query returned no data"

    def test_q03_shipping_priority(self, mindsdb_connection):
        """Query 3: Shipping Priority"""
        sql = f"""
            SELECT 
                l_orderkey, 
                SUM(l_extendedprice * (1 - l_discount)) as revenue,
                o_orderdate, 
                o_shippriority
            FROM {MSSQL_DB}.customer, {MSSQL_DB}.orders,
                {MSSQL_DB}.lineitem
            WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey 
                AND o_orderdate < '1995-03-15'
                AND l_shipdate > '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC
            LIMIT 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q05_local_supplier_volume(self, mindsdb_connection):
        """Query 5: Local Supplier Volume"""
        sql = f"""
            SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue
            FROM {MSSQL_DB}.customer, {MSSQL_DB}.orders,
                {MSSQL_DB}.lineitem, {MSSQL_DB}.supplier,
                {MSSQL_DB}.nation, {MSSQL_DB}.region
            WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                AND r_name = 'ASIA' 
                AND o_orderdate >= '1994-01-01'
                AND o_orderdate < '1995-01-01'
            GROUP BY n_name
            ORDER BY revenue DESC
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q06_forecasting_revenue(self, mindsdb_connection):
        """Query 6: Forecasting Revenue Change"""
        sql = f"""
            SELECT SUM(l_extendedprice * l_discount) as revenue
            FROM {MSSQL_DB}.lineitem
            WHERE l_shipdate >= '1994-01-01' 
                AND l_shipdate < '1995-01-01'
                AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q10_returned_item(self, mindsdb_connection):
        """Query 10: Returned Item Reporting"""
        sql = f"""
            SELECT 
                c_custkey, c_name,
                SUM(l_extendedprice * (1 - l_discount)) as revenue,
                c_acctbal, n_name
            FROM {MSSQL_DB}.customer, {MSSQL_DB}.orders,
                {MSSQL_DB}.lineitem, {MSSQL_DB}.nation
            WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                AND o_orderdate >= '1993-10-01' 
                AND o_orderdate < '1994-01-01'
                AND l_returnflag = 'R' AND c_nationkey = n_nationkey
            GROUP BY c_custkey, c_name, c_acctbal, n_name
            ORDER BY revenue DESC
            LIMIT 20
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q12_shipping_modes(self, mindsdb_connection):
        """Query 12: Shipping Modes and Order Priority"""
        sql = f"""
            SELECT l_shipmode,
                SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                    THEN 1 ELSE 0 END) as high_line_count,
                SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                    THEN 1 ELSE 0 END) as low_line_count
            FROM {MSSQL_DB}.orders, {MSSQL_DB}.lineitem
            WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
                AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
                AND l_receiptdate >= '1994-01-01' 
                AND l_receiptdate < '1995-01-01'
            GROUP BY l_shipmode
            ORDER BY l_shipmode
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q14_promotion_effect(self, mindsdb_connection):
        """Query 14: Promotion Effect"""
        sql = f"""
            SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                    THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
            FROM {MSSQL_DB}.lineitem, {MSSQL_DB}.part
            WHERE l_partkey = p_partkey 
                AND l_shipdate >= '1995-09-01'
                AND l_shipdate < '1995-10-01'
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_simple_aggregation(self, mindsdb_connection):
        """Test simple aggregation without complex joins"""
        sql = f"""
            SELECT 
                COUNT(*) as customer_count,
                AVG(c_acctbal) as avg_balance,
                MAX(c_acctbal) as max_balance,
                MIN(c_acctbal) as min_balance
            FROM {MSSQL_DB}.customer
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) == 1

    def test_group_by_aggregation(self, mindsdb_connection):
        """Test GROUP BY with aggregations"""
        sql = f"""
            SELECT 
                c_mktsegment,
                COUNT(*) as customer_count,
                AVG(c_acctbal) as avg_balance
            FROM {MSSQL_DB}.customer
            GROUP BY c_mktsegment
            ORDER BY customer_count DESC
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_join_with_aggregation(self, mindsdb_connection):
        """Test JOIN with aggregation"""
        sql = f"""
            SELECT 
                r.r_name as region_name,
                COUNT(n.n_nationkey) as nation_count
            FROM {MSSQL_DB}.region r
            INNER JOIN {MSSQL_DB}.nation n ON r.r_regionkey = n.n_regionkey
            GROUP BY r.r_name
            ORDER BY nation_count DESC
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_multi_table_join(self, mindsdb_connection):
        """Test multi-table JOIN"""
        sql = f"""
            SELECT 
                c.c_name,
                n.n_name as nation,
                r.r_name as region,
                c.c_acctbal
            FROM {MSSQL_DB}.customer c
            INNER JOIN {MSSQL_DB}.nation n ON c.c_nationkey = n.n_nationkey
            INNER JOIN {MSSQL_DB}.region r ON n.n_regionkey = r.r_regionkey
            WHERE c.c_acctbal > 0
            ORDER BY c.c_acctbal DESC
            LIMIT 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_subquery(self, mindsdb_connection):
        """Test subquery"""
        sql = f"""
            SELECT 
                c_name,
                c_acctbal
            FROM {MSSQL_DB}.customer
            WHERE c_acctbal > (
                SELECT AVG(c_acctbal) 
                FROM {MSSQL_DB}.customer
            )
            ORDER BY c_acctbal DESC
            LIMIT 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_having_clause(self, mindsdb_connection):
        """Test HAVING clause"""
        sql = f"""
            SELECT 
                c_mktsegment,
                COUNT(*) as customer_count,
                AVG(c_acctbal) as avg_balance
            FROM {MSSQL_DB}.customer
            GROUP BY c_mktsegment
            HAVING COUNT(*) > 0
            ORDER BY avg_balance DESC
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_case_statement(self, mindsdb_connection):
        """Test CASE statement"""
        sql = f"""
            SELECT 
                c_name,
                c_acctbal,
                CASE 
                    WHEN c_acctbal < 0 THEN 'Negative'
                    WHEN c_acctbal = 0 THEN 'Zero'
                    WHEN c_acctbal < 1000 THEN 'Low'
                    WHEN c_acctbal < 5000 THEN 'Medium'
                    ELSE 'High'
                END as balance_category
            FROM {MSSQL_DB}.customer
            LIMIT 20
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_distinct_values(self, mindsdb_connection):
        """Test DISTINCT values"""
        sql = f"""
            SELECT DISTINCT c_mktsegment
            FROM {MSSQL_DB}.customer
            ORDER BY c_mktsegment
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_count_distinct(self, mindsdb_connection):
        """Test COUNT DISTINCT"""
        sql = f"""
            SELECT 
                COUNT(DISTINCT c_mktsegment) as segment_count,
                COUNT(DISTINCT c_nationkey) as nation_count
            FROM {MSSQL_DB}.customer
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
        assert len(result["data"]) == 1

    def test_null_handling(self, mindsdb_connection):
        """Test NULL handling"""
        sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(c_phone) as non_null_phones,
                SUM(CASE WHEN c_phone IS NULL THEN 1 ELSE 0 END) as null_phones
            FROM {MSSQL_DB}.customer
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_string_operations(self, mindsdb_connection):
        """Test string operations"""
        sql = f"""
            SELECT 
                UPPER(c_name) as upper_name,
                LOWER(c_mktsegment) as lower_segment,
                LENGTH(c_name) as name_length
            FROM {MSSQL_DB}.customer
            LIMIT 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_numeric_operations(self, mindsdb_connection):
        """Test numeric operations"""
        sql = f"""
            SELECT 
                c_acctbal,
                ROUND(c_acctbal, 0) as rounded_balance,
                ABS(c_acctbal) as abs_balance,
                c_acctbal * 1.1 as balance_with_markup
            FROM {MSSQL_DB}.customer
            WHERE c_acctbal <> 0
            LIMIT 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_between_operator(self, mindsdb_connection):
        """Test BETWEEN operator"""
        sql = f"""
            SELECT 
                COUNT(*) as customer_count
            FROM {MSSQL_DB}.customer
            WHERE c_acctbal BETWEEN 1000 AND 5000
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_in_operator(self, mindsdb_connection):
        """Test IN operator"""
        sql = f"""
            SELECT 
                c_mktsegment,
                COUNT(*) as count
            FROM {MSSQL_DB}.customer
            WHERE c_mktsegment IN ('BUILDING', 'AUTOMOBILE', 'MACHINERY')
            GROUP BY c_mktsegment
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_like_operator(self, mindsdb_connection):
        """Test LIKE operator"""
        sql = f"""
            SELECT COUNT(*) as count
            FROM {MSSQL_DB}.part
            WHERE p_type LIKE '%BRASS%'
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result