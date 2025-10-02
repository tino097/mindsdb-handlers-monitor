"""
TPC-H Query Tests for MindsDB Oracle Handler
pytest-based test suite for all 22 TPC-H queries
"""

import os
import pytest
import requests
import time
import json
from datetime import datetime


@pytest.fixture(scope="module")
def mindsdb_client():
    """Setup MindsDB client and Oracle connection"""
    base_url = os.getenv("MINDSDB_API_URL", "http://localhost:47334")
    db_name = "oracle_tpch"

    # Wait for MindsDB
    print("\nWaiting for MindsDB...")
    for i in range(60):
        try:
            response = requests.get(f"{base_url}/api/status", timeout=5)
            if response.status_code == 200:
                print("MindsDB ready")
                break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("MindsDB failed to start")

    # Create database connection
    print("Creating Oracle connection...")
    query = f"""
    CREATE DATABASE {db_name}
    WITH ENGINE = 'oracle',
    PARAMETERS = {{
        "host": "{os.getenv('ORACLE_HOST', 'localhost')}",
        "port": {os.getenv('ORACLE_PORT', '1521')},
        "user": "{os.getenv('ORACLE_USER', 'sampleuser')}",
        "password": "{os.getenv('ORACLE_PASSWORD', 'SamplePass123')}",
        "sid": "{os.getenv('ORACLE_DB', 'XEPDB1')}"
    }};
    """

    try:
        response = requests.post(
            f"{base_url}/api/sql/query", json={"query": query}, timeout=30
        )
    except Exception as e:
        print(f"Warning: {e} (connection may exist)")

    yield {"base_url": base_url, "db_name": db_name}


def execute_query(client, sql, timeout=300):
    """Execute a query through MindsDB API"""
    response = requests.post(
        f"{client['base_url']}/api/sql/query", json={"query": sql}, timeout=timeout
    )

    assert response.status_code == 200, f"Query failed: {response.text}"
    return response.json()


def test_simple_select(mindsdb_client):
    """Simple SELECT test to verify connection"""
    print(f"Testing connection to Oracle database '{mindsdb_client['db_name']}'...")
    result = execute_query(
        mindsdb_client,
        f"SELECT COUNT(*) as cnt FROM {mindsdb_client['db_name']}.customer",
    )
    assert "data" in result
    assert result["data"][0]["CNT"] > 0, "Customer table is empty"


def test_q01_pricing_summary(mindsdb_client):
    """Query 1: Pricing Summary Report"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT l_returnflag, l_linestatus,
            SUM(l_quantity) as sum_qty,
            SUM(l_extendedprice) as sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            AVG(l_quantity) as avg_qty,
            AVG(l_extendedprice) as avg_price,
            AVG(l_discount) as avg_disc,
            COUNT(*) as count_order
        FROM {mindsdb_client['db_name']}.lineitem
        WHERE l_shipdate <= '1998-09-02'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    )
    assert len(result.get("data", [])) > 0, "Query returned no data"


def test_q02_minimum_cost_supplier(mindsdb_client):
    """Query 2: Minimum Cost Supplier"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr
        FROM {mindsdb_client['db_name']}.part, {mindsdb_client['db_name']}.supplier,
             {mindsdb_client['db_name']}.partsupp, {mindsdb_client['db_name']}.nation,
             {mindsdb_client['db_name']}.region
        WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
            AND p_size = 15 AND p_type LIKE '%BRASS'
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
        LIMIT 10
    """,
    )
    # May return 0 rows if no matching data
    assert "data" in result


def test_q03_shipping_priority(mindsdb_client):
    """Query 3: Shipping Priority"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate, o_shippriority
        FROM {mindsdb_client['db_name']}.customer, {mindsdb_client['db_name']}.orders,
             {mindsdb_client['db_name']}.lineitem
        WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-15'
            AND l_shipdate > '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC
        LIMIT 10
    """,
    )
    assert "data" in result


def test_q04_order_priority(mindsdb_client):
    """Query 4: Order Priority Checking"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT o_orderpriority, COUNT(*) as order_count
        FROM {mindsdb_client['db_name']}.orders
        WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01'
            AND EXISTS (
                SELECT * FROM {mindsdb_client['db_name']}.lineitem
                WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
            )
        GROUP BY o_orderpriority
        ORDER BY o_orderpriority
    """,
    )
    assert "data" in result


def test_q05_local_supplier_volume(mindsdb_client):
    """Query 5: Local Supplier Volume"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue
        FROM {mindsdb_client['db_name']}.customer, {mindsdb_client['db_name']}.orders,
             {mindsdb_client['db_name']}.lineitem, {mindsdb_client['db_name']}.supplier,
             {mindsdb_client['db_name']}.nation, {mindsdb_client['db_name']}.region
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
            AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
            AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01'
            AND o_orderdate < '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC
    """,
    )
    assert "data" in result


def test_q06_forecasting_revenue(mindsdb_client):
    """Query 6: Forecasting Revenue Change"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT SUM(l_extendedprice * l_discount) as revenue
        FROM {mindsdb_client['db_name']}.lineitem
        WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
    """,
    )
    assert "data" in result


def test_q07_volume_shipping(mindsdb_client):
    """Query 7: Volume Shipping"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT supp_nation, cust_nation, l_year, SUM(volume) as revenue
        FROM (
            SELECT n1.n_name as supp_nation, n2.n_name as cust_nation,
                EXTRACT(YEAR FROM l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            FROM {mindsdb_client['db_name']}.supplier, {mindsdb_client['db_name']}.lineitem,
                 {mindsdb_client['db_name']}.orders, {mindsdb_client['db_name']}.customer,
                 {mindsdb_client['db_name']}.nation n1, {mindsdb_client['db_name']}.nation n2
            WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey
                AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey
                AND c_nationkey = n2.n_nationkey
                AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                    OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
                AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
        ) shipping
        GROUP BY supp_nation, cust_nation, l_year
        ORDER BY supp_nation, cust_nation, l_year
    """,
    )
    assert "data" in result


def test_q08_national_market_share(mindsdb_client):
    """Query 8: National Market Share"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT o_year, 
            SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) as mkt_share
        FROM (
            SELECT EXTRACT(YEAR FROM o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation
            FROM {mindsdb_client['db_name']}.part, {mindsdb_client['db_name']}.supplier,
                 {mindsdb_client['db_name']}.lineitem, {mindsdb_client['db_name']}.orders,
                 {mindsdb_client['db_name']}.customer, {mindsdb_client['db_name']}.nation n1,
                 {mindsdb_client['db_name']}.nation n2, {mindsdb_client['db_name']}.region
            WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
                AND l_orderkey = o_orderkey AND o_custkey = c_custkey
                AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
                AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
                AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
                AND p_type = 'ECONOMY ANODIZED STEEL'
        ) all_nations
        GROUP BY o_year
        ORDER BY o_year
    """,
    )
    assert "data" in result


def test_q09_product_type_profit(mindsdb_client):
    """Query 9: Product Type Profit Measure"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT nation, o_year, SUM(amount) as sum_profit
        FROM (
            SELECT n_name as nation, EXTRACT(YEAR FROM o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            FROM {mindsdb_client['db_name']}.part, {mindsdb_client['db_name']}.supplier,
                 {mindsdb_client['db_name']}.lineitem, {mindsdb_client['db_name']}.partsupp,
                 {mindsdb_client['db_name']}.orders, {mindsdb_client['db_name']}.nation
            WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
                AND ps_partkey = l_partkey AND p_partkey = l_partkey
                AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
                AND p_name LIKE '%green%'
        ) profit
        GROUP BY nation, o_year
        ORDER BY nation, o_year DESC
        LIMIT 20
    """,
    )
    assert "data" in result


def test_q10_returned_item(mindsdb_client):
    """Query 10: Returned Item Reporting"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT c_custkey, c_name, 
            SUM(l_extendedprice * (1 - l_discount)) as revenue,
            c_acctbal, n_name
        FROM {mindsdb_client['db_name']}.customer, {mindsdb_client['db_name']}.orders, 
             {mindsdb_client['db_name']}.lineitem, {mindsdb_client['db_name']}.nation
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
            AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'
            AND l_returnflag = 'R' AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, n_name
        ORDER BY revenue DESC
        LIMIT 20
    """,
    )
    assert "data" in result


def test_q11_important_stock(mindsdb_client):
    """Query 11: Important Stock Identification"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) as value
        FROM {mindsdb_client['db_name']}.partsupp, {mindsdb_client['db_name']}.supplier,
             {mindsdb_client['db_name']}.nation
        WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY'
        GROUP BY ps_partkey
        HAVING SUM(ps_supplycost * ps_availqty) > 1000
        ORDER BY value DESC
        LIMIT 20
    """,
    )
    assert "data" in result


def test_q12_shipping_modes(mindsdb_client):
    """Query 12: Shipping Modes and Order Priority"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT l_shipmode,
            SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                THEN 1 ELSE 0 END) as high_line_count,
            SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                THEN 1 ELSE 0 END) as low_line_count
        FROM {mindsdb_client['db_name']}.orders, {mindsdb_client['db_name']}.lineitem
        WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
            AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
            AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'
        GROUP BY l_shipmode
        ORDER BY l_shipmode
    """,
    )
    assert "data" in result


def test_q13_customer_distribution(mindsdb_client):
    """Query 13: Customer Distribution"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT c_count, COUNT(*) as custdist
        FROM (
            SELECT c_custkey, COUNT(o_orderkey) as c_count
            FROM {mindsdb_client['db_name']}.customer
            LEFT OUTER JOIN {mindsdb_client['db_name']}.orders ON c_custkey = o_custkey
                AND o_comment NOT LIKE '%special%requests%'
            GROUP BY c_custkey
        ) c_orders
        GROUP BY c_count
        ORDER BY custdist DESC, c_count DESC
        LIMIT 20
    """,
    )
    assert "data" in result


def test_q14_promotion_effect(mindsdb_client):
    """Query 14: Promotion Effect"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                THEN l_extendedprice * (1 - l_discount) ELSE 0 END) 
            / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
        FROM {mindsdb_client['db_name']}.lineitem, {mindsdb_client['db_name']}.part
        WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01'
            AND l_shipdate < '1995-10-01'
    """,
    )
    assert "data" in result


def test_q15_top_supplier(mindsdb_client):
    """Query 15: Top Supplier"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT s_suppkey, s_name, total_revenue
        FROM {mindsdb_client['db_name']}.supplier,
            (SELECT l_suppkey as supplier_no, 
                SUM(l_extendedprice * (1 - l_discount)) as total_revenue
             FROM {mindsdb_client['db_name']}.lineitem
             WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'
             GROUP BY l_suppkey) revenue
        WHERE s_suppkey = supplier_no
        ORDER BY total_revenue DESC
        LIMIT 5
    """,
    )
    assert "data" in result


def test_q16_parts_supplier(mindsdb_client):
    """Query 16: Parts/Supplier Relationship"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) as supplier_cnt
        FROM {mindsdb_client['db_name']}.partsupp, {mindsdb_client['db_name']}.part
        WHERE p_partkey = ps_partkey
            AND p_brand <> 'Brand#45'
            AND p_type NOT LIKE 'MEDIUM POLISHED%'
            AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
        GROUP BY p_brand, p_type, p_size
        ORDER BY supplier_cnt DESC
        LIMIT 20
    """,
    )
    assert "data" in result


def test_q17_small_quantity_order(mindsdb_client):
    """Query 17: Small-Quantity-Order Revenue"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
        FROM {mindsdb_client['db_name']}.lineitem, {mindsdb_client['db_name']}.part
        WHERE p_partkey = l_partkey
            AND p_brand = 'Brand#23'
            AND p_container = 'MED BOX'
            AND l_quantity < 10
    """,
    )
    assert "data" in result


def test_q18_large_volume_customer(mindsdb_client):
    """Query 18: Large Volume Customer"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
            SUM(l_quantity) as total_qty
        FROM {mindsdb_client['db_name']}.customer, {mindsdb_client['db_name']}.orders,
             {mindsdb_client['db_name']}.lineitem
        WHERE o_orderkey IN (
            SELECT l_orderkey
            FROM {mindsdb_client['db_name']}.lineitem
            GROUP BY l_orderkey
            HAVING SUM(l_quantity) > 300
        )
        AND c_custkey = o_custkey AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC
        LIMIT 20
    """,
    )
    assert "data" in result


def test_q19_discounted_revenue(mindsdb_client):
    """Query 19: Discounted Revenue"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT SUM(l_extendedprice * (1 - l_discount)) as revenue
        FROM {mindsdb_client['db_name']}.lineitem, {mindsdb_client['db_name']}.part
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
    """,
    )
    assert "data" in result


def test_q20_potential_part_promotion(mindsdb_client):
    """Query 20: Potential Part Promotion"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT s_name, s_address
        FROM {mindsdb_client['db_name']}.supplier, {mindsdb_client['db_name']}.nation
        WHERE s_suppkey IN (
            SELECT ps_suppkey
            FROM {mindsdb_client['db_name']}.partsupp
            WHERE ps_partkey IN (
                SELECT p_partkey
                FROM {mindsdb_client['db_name']}.part
                WHERE p_name LIKE 'forest%'
            )
            AND ps_availqty > 100
        )
        AND s_nationkey = n_nationkey AND n_name = 'CANADA'
        ORDER BY s_name
        LIMIT 20
    """,
    )
    assert "data" in result


def test_q21_suppliers_waiting(mindsdb_client):
    """Query 21: Suppliers Who Kept Orders Waiting"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT s_name, COUNT(*) as numwait
        FROM {mindsdb_client['db_name']}.supplier, {mindsdb_client['db_name']}.lineitem l1,
             {mindsdb_client['db_name']}.orders, {mindsdb_client['db_name']}.nation
        WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
            AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
            AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
        GROUP BY s_name
        ORDER BY numwait DESC
        LIMIT 20
    """,
        timeout=600,
    )
    assert "data" in result


def test_q22_global_sales_opportunity(mindsdb_client):
    """Query 22: Global Sales Opportunity"""
    result = execute_query(
        mindsdb_client,
        f"""
        SELECT cntrycode, COUNT(*) as numcust, SUM(c_acctbal) as totacctbal
        FROM (
            SELECT SUBSTR(c_phone, 1, 2) as cntrycode, c_acctbal
            FROM {mindsdb_client['db_name']}.customer
            WHERE SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                AND c_acctbal > 0
                AND NOT EXISTS (
                    SELECT * FROM {mindsdb_client['db_name']}.orders
                    WHERE o_custkey = c_custkey
                )
        ) custsale
        GROUP BY cntrycode
        ORDER BY cntrycode
    """,
    )
    assert "data" in result
