import os
import time
import logging
from datetime import datetime
from typing import Any, Dict

import pytest
from conftest import ORACLE_TPCH_DB,  execute_sql_via_mindsdb


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# TPC‑H Query Tests
# -----------------------------------------------------------------------------


class TestTPCHQueries:
    """Execute all 22 TPC‑H benchmark queries through MindsDB.

    Each test method corresponds to one of the official TPC‑H queries.  The
    queries reference tables owned by the Oracle user connected through
    MindsDB.  The database name is injected into the query using an f-string
    and the `ORACLE_TPCH_DB` constant.  For queries that produce results,
    we assert that the response contains a `data` key; for the pricing
    summary query (Q1), we additionally assert that at least one row is
    returned.
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
            FROM {ORACLE_TPCH_DB}.lineitem
            WHERE l_shipdate <= TO_DATE('1998-09-02', 'YYYY-MM-DD')
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """
        result = execute_sql_via_mindsdb(sql)
        logger.info(f"Q1 Pricing Summary result: {result}")
        assert len(result.get("data", [])) > 0, "Query returned no data"

    def test_q02_minimum_cost_supplier(self, mindsdb_connection):
        """Query 2: Minimum Cost Supplier"""
        sql = f"""
            SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr
            FROM {ORACLE_TPCH_DB}.part, {ORACLE_TPCH_DB}.supplier,
                 {ORACLE_TPCH_DB}.partsupp, {ORACLE_TPCH_DB}.nation,
                 {ORACLE_TPCH_DB}.region
            WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
                AND p_size = 15 AND p_type LIKE '%BRASS'
                AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE'
            LIMIT 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q03_shipping_priority(self, mindsdb_connection):
        """Query 3: Shipping Priority"""
        sql = f"""
            SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue,
                o_orderdate, o_shippriority
            FROM {ORACLE_TPCH_DB}.customer, {ORACLE_TPCH_DB}.orders,
                {ORACLE_TPCH_DB}.lineitem
            WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey 
                AND o_orderdate < TO_DATE('1995-03-15', 'YYYY-MM-DD')
                AND l_shipdate > TO_DATE('1995-03-15', 'YYYY-MM-DD')
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC
            LIMIT 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q04_order_priority(self, mindsdb_connection):
        """Query 4: Order Priority Checking"""
        sql = f"""
            SELECT o_orderpriority, COUNT(*) as order_count
            FROM {ORACLE_TPCH_DB}.orders
            WHERE o_orderdate >= TO_DATE('1993-07-01', 'YYYY-MM-DD') 
                AND o_orderdate < TO_DATE('1993-10-01', 'YYYY-MM-DD')
                AND EXISTS (
                    SELECT * FROM {ORACLE_TPCH_DB}.lineitem
                    WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
                )
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q05_local_supplier_volume(self, mindsdb_connection):
        """Query 5: Local Supplier Volume"""
        sql = f"""
            SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue
            FROM {ORACLE_TPCH_DB}.customer, {ORACLE_TPCH_DB}.orders,
                {ORACLE_TPCH_DB}.lineitem, {ORACLE_TPCH_DB}.supplier,
                {ORACLE_TPCH_DB}.nation, {ORACLE_TPCH_DB}.region
            WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                AND r_name = 'ASIA' 
                AND o_orderdate >= TO_DATE('1994-01-01', 'YYYY-MM-DD')
                AND o_orderdate < TO_DATE('1995-01-01', 'YYYY-MM-DD')
            GROUP BY n_name
            ORDER BY revenue DESC
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q06_forecasting_revenue(self, mindsdb_connection):
        """Query 6: Forecasting Revenue Change"""
        sql = f"""
            SELECT SUM(l_extendedprice * l_discount) as revenue
            FROM {ORACLE_TPCH_DB}.lineitem
            WHERE l_shipdate >= TO_DATE('1994-01-01', 'YYYY-MM-DD') 
                AND l_shipdate < TO_DATE('1995-01-01', 'YYYY-MM-DD')
                AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q07_volume_shipping(self, mindsdb_connection):
        """Query 7: Volume Shipping"""
        sql = f"""
            SELECT supp_nation, cust_nation, l_year, SUM(volume) as revenue
            FROM (
                SELECT n1.n_name as supp_nation, n2.n_name as cust_nation,
                    EXTRACT(YEAR FROM l_shipdate) as l_year,
                    l_extendedprice * (1 - l_discount) as volume
                FROM {ORACLE_TPCH_DB}.supplier, {ORACLE_TPCH_DB}.lineitem,
                    {ORACLE_TPCH_DB}.orders, {ORACLE_TPCH_DB}.customer,
                    {ORACLE_TPCH_DB}.nation n1, {ORACLE_TPCH_DB}.nation n2
                WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey
                    AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey
                    AND c_nationkey = n2.n_nationkey
                    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
                    AND l_shipdate BETWEEN TO_DATE('1995-01-01', 'YYYY-MM-DD') 
                        AND TO_DATE('1996-12-31', 'YYYY-MM-DD')
            ) shipping
            GROUP BY supp_nation, cust_nation, l_year
            ORDER BY supp_nation, cust_nation, l_year
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q08_national_market_share(self, mindsdb_connection):
        """Query 8: National Market Share"""
        sql = f"""
            SELECT o_year,
                SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) as mkt_share
            FROM (
                SELECT EXTRACT(YEAR FROM o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation
                FROM {ORACLE_TPCH_DB}.part, {ORACLE_TPCH_DB}.supplier,
                    {ORACLE_TPCH_DB}.lineitem, {ORACLE_TPCH_DB}.orders,
                    {ORACLE_TPCH_DB}.customer, {ORACLE_TPCH_DB}.nation n1,
                    {ORACLE_TPCH_DB}.nation n2, {ORACLE_TPCH_DB}.region
                WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
                    AND l_orderkey = o_orderkey AND o_custkey = c_custkey
                    AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
                    AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
                    AND o_orderdate BETWEEN TO_DATE('1995-01-01', 'YYYY-MM-DD')
                        AND TO_DATE('1996-12-31', 'YYYY-MM-DD')
                    AND p_type = 'ECONOMY ANODIZED STEEL'
            ) all_nations
            GROUP BY o_year
            ORDER BY o_year
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q09_product_type_profit(self, mindsdb_connection):
        """Query 9: Product Type Profit Measure"""
        sql = f"""
            SELECT nation, o_year, SUM(amount) as sum_profit
            FROM (
                SELECT n_name as nation, EXTRACT(YEAR FROM o_orderdate) as o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                FROM {ORACLE_TPCH_DB}.part, {ORACLE_TPCH_DB}.supplier,
                     {ORACLE_TPCH_DB}.lineitem, {ORACLE_TPCH_DB}.partsupp,
                     {ORACLE_TPCH_DB}.orders, {ORACLE_TPCH_DB}.nation
                WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
                    AND ps_partkey = l_partkey AND p_partkey = l_partkey
                    AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
                    AND p_name LIKE '%green%'
            ) profit
            GROUP BY nation, o_year
            ORDER BY nation, o_year DESC
            LIMIT 20
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q10_returned_item(self, mindsdb_connection):
        """Query 10: Returned Item Reporting"""
        sql = f"""
            SELECT c_custkey, c_name,
            SUM(l_extendedprice * (1 - l_discount)) as revenue,
            c_acctbal, n_name
            FROM {ORACLE_TPCH_DB}.customer, {ORACLE_TPCH_DB}.orders,
                {ORACLE_TPCH_DB}.lineitem, {ORACLE_TPCH_DB}.nation
            WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                AND o_orderdate >= TO_DATE('1993-10-01', 'YYYY-MM-DD') 
                AND o_orderdate < TO_DATE('1994-01-01', 'YYYY-MM-DD')
                AND l_returnflag = 'R' AND c_nationkey = n_nationkey
            GROUP BY c_custkey, c_name, c_acctbal, n_name
            ORDER BY revenue DESC
            LIMIT 20
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q11_important_stock(self, mindsdb_connection):
        """Query 11: Important Stock Identification"""
        sql = f"""
            SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
            FROM {ORACLE_TPCH_DB}.partsupp, {ORACLE_TPCH_DB}.supplier,
                 {ORACLE_TPCH_DB}.nation
            WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey
                AND n_name = 'GERMANY'
            GROUP BY ps_partkey
            HAVING SUM(ps_supplycost * ps_availqty) > 1000
            ORDER BY value DESC
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
            FROM {ORACLE_TPCH_DB}.orders, {ORACLE_TPCH_DB}.lineitem
            WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
                AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
                AND l_receiptdate >= TO_DATE('1994-01-01', 'YYYY-MM-DD') 
                AND l_receiptdate < TO_DATE('1995-01-01', 'YYYY-MM-DD')
            GROUP BY l_shipmode
            ORDER BY l_shipmode
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q13_customer_distribution(self, mindsdb_connection):
        """Query 13: Customer Distribution"""
        sql = f"""
            SELECT c_count, COUNT(*) as custdist
            FROM (
                SELECT c_custkey, COUNT(o_orderkey) as c_count
                FROM {ORACLE_TPCH_DB}.customer
                LEFT OUTER JOIN {ORACLE_TPCH_DB}.orders ON c_custkey = o_custkey
                    AND o_comment NOT LIKE '%special%requests%'
                GROUP BY c_custkey
            ) c_orders
            GROUP BY c_count
            ORDER BY custdist DESC, c_count DESC
            LIMIT 20
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q14_promotion_effect(self, mindsdb_connection):
        """Query 14: Promotion Effect"""
        sql = f"""
            SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                    THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
            FROM {ORACLE_TPCH_DB}.lineitem, {ORACLE_TPCH_DB}.part
            WHERE l_partkey = p_partkey 
                AND l_shipdate >= TO_DATE('1995-09-01', 'YYYY-MM-DD')
                AND l_shipdate < TO_DATE('1995-10-01', 'YYYY-MM-DD')
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q15_top_supplier(self, mindsdb_connection):
        """Query 15: Top Supplier"""
        sql = f"""
            SELECT s_suppkey, s_name, total_revenue
            FROM {ORACLE_TPCH_DB}.supplier,
                (SELECT l_suppkey as supplier_no,
                    SUM(l_extendedprice * (1 - l_discount)) as total_revenue
                FROM {ORACLE_TPCH_DB}.lineitem
                WHERE l_shipdate >= TO_DATE('1996-01-01', 'YYYY-MM-DD') 
                    AND l_shipdate < TO_DATE('1996-04-01', 'YYYY-MM-DD')
                GROUP BY l_suppkey) revenue
            WHERE s_suppkey = supplier_no
            ORDER BY total_revenue DESC
            LIMIT 5
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q16_parts_supplier(self, mindsdb_connection):
        """Query 16: Parts/Supplier Relationship"""
        sql = f"""
            SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) as supplier_cnt
            FROM {ORACLE_TPCH_DB}.partsupp, {ORACLE_TPCH_DB}.part
            WHERE p_partkey = ps_partkey
                AND p_brand <> 'Brand#45'
                AND p_type NOT LIKE 'MEDIUM POLISHED%'
                AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
            GROUP BY p_brand, p_type, p_size
            ORDER BY supplier_cnt DESC
            LIMIT 20
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q17_small_quantity_order(self, mindsdb_connection):
        """Query 17: Small‑Quantity‑Order Revenue"""
        sql = f"""
            SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
            FROM {ORACLE_TPCH_DB}.lineitem, {ORACLE_TPCH_DB}.part
            WHERE p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container = 'MED BOX'
                AND l_quantity < 10
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q18_large_volume_customer(self, mindsdb_connection):
        """Query 18: Large Volume Customer"""
        sql = f"""
            SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
                SUM(l_quantity) as total_qty
            FROM {ORACLE_TPCH_DB}.customer, {ORACLE_TPCH_DB}.orders,
                 {ORACLE_TPCH_DB}.lineitem
            WHERE o_orderkey IN (
                SELECT l_orderkey
                FROM {ORACLE_TPCH_DB}.lineitem
                GROUP BY l_orderkey
                HAVING SUM(l_quantity) > 300
            )
            AND c_custkey = o_custkey AND o_orderkey = l_orderkey
            GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
            ORDER BY o_totalprice DESC
            LIMIT 20
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q19_discounted_revenue(self, mindsdb_connection):
        """Query 19: Discounted Revenue"""
        sql = f"""
            SELECT SUM(l_extendedprice * (1 - l_discount)) as revenue
            FROM {ORACLE_TPCH_DB}.lineitem, {ORACLE_TPCH_DB}.part
            WHERE p_partkey = l_partkey
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
                AND (
                    (p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                     AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5)
                    OR
                    (p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                     AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10)
                    OR
                    (p_brand = 'Brand#34' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                     AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15)
                )
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q20_potential_part_promotion(self, mindsdb_connection):
        """Query 20: Potential Part Promotion"""
        sql = f"""
            SELECT s_name, s_address
            FROM {ORACLE_TPCH_DB}.supplier, {ORACLE_TPCH_DB}.nation
            WHERE s_suppkey IN (
                SELECT ps_suppkey
                FROM {ORACLE_TPCH_DB}.partsupp
                WHERE ps_partkey IN (
                    SELECT p_partkey
                    FROM {ORACLE_TPCH_DB}.part
                    WHERE p_name LIKE 'forest%'
                )
                AND ps_availqty > 100
            )
            AND s_nationkey = n_nationkey AND n_name = 'CANADA'
            ORDER BY s_name
            LIMIT 20
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result

    def test_q21_suppliers_waiting(self, mindsdb_connection):
        """Query 21: Suppliers Who Kept Orders Waiting"""
        sql = f"""
            SELECT s_name, COUNT(*) as numwait
            FROM {ORACLE_TPCH_DB}.supplier, {ORACLE_TPCH_DB}.lineitem l1,
                 {ORACLE_TPCH_DB}.orders, {ORACLE_TPCH_DB}.nation
            WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
                AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
                AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
            GROUP BY s_name
            ORDER BY numwait DESC
            LIMIT 20
        """
        # This query can take longer; allow extra timeout
        result = execute_sql_via_mindsdb(sql, timeout=600)
        assert "data" in result

    def test_q22_global_sales_opportunity(self, mindsdb_connection):
        """Query 22: Global Sales Opportunity"""
        sql = f"""
            SELECT cntrycode, COUNT(*) as numcust, SUM(c_acctbal) as totacctbal
            FROM (
                SELECT SUBSTR(c_phone, 1, 2) as cntrycode, c_acctbal
                FROM {ORACLE_TPCH_DB}.customer
                WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                    AND c_acctbal > 0
                    AND NOT EXISTS (
                        SELECT * FROM {ORACLE_TPCH_DB}.orders
                        WHERE o_custkey = c_custkey
                    )
            ) custsale
            GROUP BY cntrycode
            ORDER BY cntrycode
        """
        result = execute_sql_via_mindsdb(sql)
        assert "data" in result
