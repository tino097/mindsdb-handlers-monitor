# Elasticsearch Handler Monitor

## Overview

This test suite validates the MindsDB Elasticsearch handler, including:
- Basic connectivity and authentication
- Data Catalog support (get_column_statistics, get_primary_keys, get_foreign_keys)
- Complex queries with array and nested field handling
- Kibana sample dataset compatibility

## 📊 Latest Test Results

![Tests](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/elasticsearch.yml/badge.svg)

<!-- TEST_RESULTS_START -->
| Metric | Value |
|--------|-------|
| **Total Tests** | 48 |
| **Status** | Waiting for first run |

[📋 View detailed results](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/elasticsearch.yml)
<!-- TEST_RESULTS_END -->

## Test Coverage

### Handler Tests (6 tests - test_handler.py)
- ✅ Connection and authentication
- ✅ Simple SELECT queries
- ✅ WHERE clause filtering
- ✅ COUNT aggregation
- ✅ Schema introspection (SHOW TABLES, SHOW COLUMNS)

### Data Catalog Tests (10 tests - test_data_catalog.py)
- ✅ get_column_statistics (all columns, specific column, by type)
- ✅ get_primary_keys (_id validation)
- ✅ get_foreign_keys (empty for NoSQL)
- ✅ Nested field flattening with dot notation
- ✅ Data quality and numeric field analysis

### Elasticsearch Query Tests (32 tests - test_elasticsearch_queries.py)
- ✅ ORDER BY and sorting (4 tests)
- ✅ Multiple aggregation functions (4 tests)
- ✅ Complex WHERE conditions - AND/OR/IN (4 tests)
- ✅ Date and timestamp filtering (3 tests)
- ✅ Boolean field queries (3 tests)
- ✅ Array field handling (2 tests)
- ✅ Geo-point field queries (2 tests)
- ✅ Pagination and LIMIT (4 tests)
- ✅ Error handling (3 tests)
- ✅ Log dataset queries (3 tests)

## Test Data

**Note**: Unlike SQL-based handlers, this handler uses **official Kibana sample datasets** loaded via API instead of static SQL/data files.

The tests use Kibana sample datasets:
- `kibana_sample_data_flights` - 13,059 documents with geo-point fields and nested structures
- `kibana_sample_data_ecommerce` - 4,675 documents with array fields and nested objects
- `kibana_sample_data_logs` - 14,074 documents with various field types

These datasets are loaded dynamically:
- **CI/CD**: GitHub Actions workflow starts Kibana temporarily and loads via API
- **Local**: Use Kibana API or load manually (see instructions below)
- **Tests**: Use pytest fixtures that connect to running Elasticsearch with sample data

## Running Tests Locally

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt

# Start Elasticsearch (Docker)
docker run -d \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  --name elasticsearch-test \
  elasticsearch:8.11.0
```

### Load Sample Data (Optional)
```bash
# Wait for Elasticsearch to start
sleep 30

# Load Kibana sample data
curl -X POST "localhost:9200/_plugins/_kibana/api/sample_data/flights"
curl -X POST "localhost:9200/_plugins/_kibana/api/sample_data/ecommerce"
curl -X POST "localhost:9200/_plugins/_kibana/api/sample_data/logs"
```

### Run Tests
```bash
# All tests
pytest tests/ -v

# Handler tests only
pytest tests/test_handler.py -v

# Data Catalog tests only
pytest tests/test_data_catalog.py -v

# Specific test
pytest tests/test_data_catalog.py::test_get_column_statistics_all_columns -v
```

## Test Configuration

Tests use environment variables for configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `ELASTICSEARCH_HOST` | `localhost:9200` | Elasticsearch host and port |
| `ELASTICSEARCH_USER` | `elastic` | Username (if auth enabled) |
| `ELASTICSEARCH_PASSWORD` | `changeme` | Password (if auth enabled) |
| `MINDSDB_API_URL` | `http://localhost:47334` | MindsDB API endpoint |

## Features Tested

### SQL-First Architecture
- ✅ Automatic SQL API → Search API fallback for arrays
- ✅ Array-to-JSON conversion for SQL compatibility
- ✅ Proper error handling and retry logic

### Data Catalog Support
- ✅ Column statistics with single aggregation query
- ✅ Text field .keyword suffix handling
- ✅ Object/nested type exclusion from cardinality
- ✅ Graceful error handling with fallback
- ✅ All Elasticsearch field types supported

### Performance
- ✅ Single query for all column statistics
- ✅ Efficient pagination for large datasets
- ✅ Memory-efficient result processing

## Known Limitations

1. **JOINs**: Not supported (Elasticsearch limitation)
2. **Complex Subqueries**: Limited by Elasticsearch SQL capabilities
3. **Real-time Data**: Near-real-time search due to refresh intervals

## Contributing

When adding new tests:
1. Follow existing test structure and naming conventions
2. Use pytest fixtures for setup/teardown
3. Include docstrings explaining what each test validates
4. Ensure tests work with mock data (no hard dependency on running Elasticsearch)
5. Update this README with new test descriptions

## Links

- [MindsDB Elasticsearch Handler Docs](https://docs.mindsdb.com/integrations/data-integrations/elasticsearch)
- [Elasticsearch Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana Sample Data](https://www.elastic.co/guide/en/kibana/current/get-started.html#gs-get-data-into-kibana)

---

**Last Updated**: 2025-10-20
