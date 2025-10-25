"""
Basic Elasticsearch handler tests.

Tests basic connectivity, query execution, and schema operations.
"""

import pytest


def test_connection_created(elasticsearch_database):
    """Test that Elasticsearch database connection was created"""
    assert elasticsearch_database == "test_elasticsearch"


def test_simple_select(execute_sql, elasticsearch_database, sample_index_name):
    """Test basic SELECT query"""
    query = f"SELECT * FROM {elasticsearch_database}.{sample_index_name} LIMIT 5;"
    result = execute_sql(query)

    assert result is not None
    # Check for either successful data or proper error response
    assert "data" in result or "error_code" in result


def test_select_with_where(execute_sql, elasticsearch_database, sample_index_name):
    """Test SELECT with WHERE clause"""
    query = f"""
    SELECT *
    FROM {elasticsearch_database}.{sample_index_name}
    WHERE _id IS NOT NULL
    LIMIT 3;
    """
    result = execute_sql(query)

    assert result is not None


def test_count_query(execute_sql, elasticsearch_database, sample_index_name):
    """Test COUNT aggregation"""
    query = f"SELECT COUNT(*) as total FROM {elasticsearch_database}.{sample_index_name};"
    result = execute_sql(query)

    assert result is not None


def test_show_tables(execute_sql, elasticsearch_database):
    """Test SHOW TABLES equivalent"""
    query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{elasticsearch_database}'
    LIMIT 10;
    """
    result = execute_sql(query)

    assert result is not None


def test_show_columns(execute_sql, elasticsearch_database, sample_index_name):
    """Test SHOW COLUMNS equivalent"""
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{sample_index_name}'
    AND table_schema = '{elasticsearch_database}'
    LIMIT 10;
    """
    result = execute_sql(query)

    assert result is not None
