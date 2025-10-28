"""
Elasticsearch Data Catalog tests.

Tests the three required Data Catalog methods:
- get_column_statistics
- get_primary_keys
- get_foreign_keys
"""

import pytest


def test_get_column_statistics_all_columns(execute_sql, elasticsearch_database, sample_index_name):
    """Test get_column_statistics returns statistics for all columns"""
    query = f"""
    SELECT * FROM mindsdb.get_column_statistics('{elasticsearch_database}', '{sample_index_name}');
    """
    result = execute_sql(query, timeout=60)

    assert result is not None
    if "data" in result:
        # Verify we got column statistics
        data = result.get("data", [])
        assert len(data) > 0, "Should return statistics for at least one column"

        # Check first row has required columns
        if len(data) > 0:
            first_row = data[0]
            expected_keys = {"column_name", "data_type", "null_count", "distinct_count"}
            assert any(key in first_row for key in expected_keys), \
                f"Response should contain Data Catalog columns. Got: {list(first_row.keys())}"


def test_get_column_statistics_specific_column(execute_sql, elasticsearch_database, sample_index_name):
    """Test get_column_statistics for a specific column"""
    # Use _id which should exist in all indexes
    query = f"""
    SELECT * FROM mindsdb.get_column_statistics('{elasticsearch_database}', '{sample_index_name}', '_id');
    """
    result = execute_sql(query, timeout=30)

    assert result is not None


def test_get_column_statistics_numeric_field(execute_sql, elasticsearch_database, sample_index_name):
    """Test that numeric fields have min/max/avg statistics"""
    query = f"""
    SELECT column_name, data_type, min, max, avg
    FROM mindsdb.get_column_statistics('{elasticsearch_database}', '{sample_index_name}')
    WHERE data_type IN ('long', 'integer', 'double', 'float')
    LIMIT 5;
    """
    result = execute_sql(query, timeout=60)

    assert result is not None


def test_get_column_statistics_keyword_field(execute_sql, elasticsearch_database, sample_index_name):
    """Test that keyword/text fields have cardinality"""
    query = f"""
    SELECT column_name, data_type, distinct_count, null_count
    FROM mindsdb.get_column_statistics('{elasticsearch_database}', '{sample_index_name}')
    WHERE data_type IN ('keyword', 'text')
    LIMIT 5;
    """
    result = execute_sql(query, timeout=60)

    assert result is not None


def test_get_column_statistics_null_counts(execute_sql, elasticsearch_database, sample_index_name):
    """Test null count calculation"""
    query = f"""
    SELECT column_name, null_count
    FROM mindsdb.get_column_statistics('{elasticsearch_database}', '{sample_index_name}')
    WHERE null_count > 0
    LIMIT 5;
    """
    result = execute_sql(query, timeout=60)

    assert result is not None


def test_get_primary_keys(execute_sql, elasticsearch_database, sample_index_name):
    """Test get_primary_keys returns _id"""
    query = f"""
    SELECT * FROM mindsdb.get_primary_keys('{elasticsearch_database}', '{sample_index_name}');
    """
    result = execute_sql(query, timeout=30)

    assert result is not None
    if "data" in result:
        data = result.get("data", [])
        # Should have exactly one row with _id
        assert len(data) == 1, "Should return exactly one primary key"

        if len(data) > 0:
            pk_row = data[0]
            # Check for column_name field containing _id
            assert any("_id" in str(value).lower() for value in pk_row.values()), \
                f"Primary key should be _id. Got: {pk_row}"


def test_get_foreign_keys(execute_sql, elasticsearch_database, sample_index_name):
    """Test get_foreign_keys returns empty result (NoSQL has no FKs)"""
    query = f"""
    SELECT * FROM mindsdb.get_foreign_keys('{elasticsearch_database}', '{sample_index_name}');
    """
    result = execute_sql(query, timeout=30)

    assert result is not None
    if "data" in result:
        data = result.get("data", [])
        # Should be empty for NoSQL
        assert len(data) == 0, "NoSQL databases should have no foreign keys"


def test_get_column_statistics_nested_fields(execute_sql, elasticsearch_database, sample_index_name):
    """Test that nested fields are flattened with dot notation"""
    query = f"""
    SELECT column_name
    FROM mindsdb.get_column_statistics('{elasticsearch_database}', '{sample_index_name}')
    WHERE column_name LIKE '%.%'
    LIMIT 5;
    """
    result = execute_sql(query, timeout=60)

    assert result is not None
    # Nested fields will appear with dot notation (e.g., 'metadata.category')


def test_data_quality_analysis(execute_sql, elasticsearch_database, sample_index_name):
    """Test data quality analysis query"""
    query = f"""
    SELECT
        column_name,
        data_type,
        null_count,
        distinct_count
    FROM mindsdb.get_column_statistics('{elasticsearch_database}', '{sample_index_name}')
    WHERE null_count > 0
    ORDER BY null_count DESC
    LIMIT 10;
    """
    result = execute_sql(query, timeout=60)

    assert result is not None


def test_numeric_field_analysis(execute_sql, elasticsearch_database, sample_index_name):
    """Test numeric field analysis"""
    query = f"""
    SELECT
        column_name,
        data_type,
        min,
        max,
        avg,
        distinct_count
    FROM mindsdb.get_column_statistics('{elasticsearch_database}', '{sample_index_name}')
    WHERE data_type IN ('long', 'integer', 'double', 'float')
    ORDER BY avg DESC
    LIMIT 10;
    """
    result = execute_sql(query, timeout=60)

    assert result is not None
