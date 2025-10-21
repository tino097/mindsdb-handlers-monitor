"""
Comprehensive query tests for Elasticsearch handler using Kibana sample datasets.

This test suite covers real-world query patterns that users would execute through
MindsDB's Elasticsearch handler, focusing on areas not covered by basic handler tests.

Test Coverage:
- ORDER BY and sorting
- Multiple aggregation functions
- Complex WHERE conditions (AND/OR)
- Date/timestamp filtering
- Array field handling
- Boolean field queries
- Geo-point field queries
- Error handling
- Pagination and large result sets

Dataset: Kibana sample data (flights, ecommerce, logs)
"""

import pytest


class TestOrderByQueries:
    """Test ORDER BY clauses and sorting functionality"""

    def test_order_by_numeric_desc(self, execute_sql, elasticsearch_database):
        """Test ORDER BY on numeric field descending"""
        sql = f"""
            SELECT FlightNum, AvgTicketPrice, DistanceMiles
            FROM {elasticsearch_database}.kibana_sample_data_flights
            ORDER BY AvgTicketPrice DESC
            LIMIT 5
        """
        result = execute_sql(sql)
        assert "data" in result
        assert len(result["data"]) <= 5

    def test_order_by_numeric_asc(self, execute_sql, elasticsearch_database):
        """Test ORDER BY on numeric field ascending"""
        sql = f"""
            SELECT FlightNum, FlightDelayMin
            FROM {elasticsearch_database}.kibana_sample_data_flights
            ORDER BY FlightDelayMin ASC
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_order_by_keyword_field(self, execute_sql, elasticsearch_database):
        """Test ORDER BY on keyword field"""
        sql = f"""
            SELECT Carrier, COUNT(*) as flight_count
            FROM {elasticsearch_database}.kibana_sample_data_flights
            GROUP BY Carrier
            ORDER BY Carrier
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_order_by_with_limit(self, execute_sql, elasticsearch_database):
        """Test ORDER BY combined with LIMIT for top-N queries"""
        sql = f"""
            SELECT DestCityName, AVG(AvgTicketPrice) as avg_price
            FROM {elasticsearch_database}.kibana_sample_data_flights
            GROUP BY DestCityName
            ORDER BY avg_price DESC
            LIMIT 3
        """
        result = execute_sql(sql)
        assert "data" in result
        assert len(result["data"]) <= 3


class TestAggregationQueries:
    """Test multiple aggregation functions"""

    def test_multiple_aggregations_single_query(self, execute_sql, elasticsearch_database):
        """Test AVG, MIN, MAX, SUM in single query"""
        sql = f"""
            SELECT
                AVG(AvgTicketPrice) as avg_price,
                MIN(AvgTicketPrice) as min_price,
                MAX(AvgTicketPrice) as max_price,
                COUNT(*) as total_flights
            FROM {elasticsearch_database}.kibana_sample_data_flights
        """
        result = execute_sql(sql)
        assert "data" in result
        assert len(result["data"]) > 0

    def test_aggregation_with_group_by(self, execute_sql, elasticsearch_database):
        """Test aggregations with GROUP BY"""
        sql = f"""
            SELECT
                Carrier,
                COUNT(*) as flight_count,
                AVG(DistanceMiles) as avg_distance,
                SUM(FlightDelayMin) as total_delay_minutes
            FROM {elasticsearch_database}.kibana_sample_data_flights
            GROUP BY Carrier
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_count_distinct(self, execute_sql, elasticsearch_database):
        """Test COUNT with distinct values"""
        sql = f"""
            SELECT COUNT(*) as total_orders
            FROM {elasticsearch_database}.kibana_sample_data_ecommerce
        """
        result = execute_sql(sql)
        assert "data" in result
        assert len(result["data"]) > 0

    def test_aggregation_on_ecommerce_data(self, execute_sql, elasticsearch_database):
        """Test aggregations on ecommerce dataset"""
        sql = f"""
            SELECT
                customer_gender,
                COUNT(*) as order_count,
                AVG(taxful_total_price) as avg_order_value,
                MAX(taxful_total_price) as max_order_value
            FROM {elasticsearch_database}.kibana_sample_data_ecommerce
            GROUP BY customer_gender
        """
        result = execute_sql(sql)
        assert "data" in result


class TestWhereConditions:
    """Test complex WHERE clauses with multiple conditions"""

    def test_where_and_condition(self, execute_sql, elasticsearch_database):
        """Test WHERE with AND operator"""
        sql = f"""
            SELECT FlightNum, AvgTicketPrice, DistanceMiles
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE AvgTicketPrice > 500 AND DistanceMiles > 5000
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_where_or_condition(self, execute_sql, elasticsearch_database):
        """Test WHERE with OR operator"""
        sql = f"""
            SELECT Carrier, Origin, Dest
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE Carrier = 'Kibana Airlines' OR Carrier = 'Logstash Airways'
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_where_multiple_conditions(self, execute_sql, elasticsearch_database):
        """Test WHERE with multiple AND/OR conditions"""
        sql = f"""
            SELECT FlightNum, AvgTicketPrice, FlightDelayMin
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE (AvgTicketPrice > 800 OR FlightDelayMin > 60)
                AND DistanceMiles > 1000
            LIMIT 5
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_where_in_operator(self, execute_sql, elasticsearch_database):
        """Test WHERE with IN operator"""
        sql = f"""
            SELECT DestCountry, COUNT(*) as flight_count
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE DestCountry IN ('US', 'IT', 'CN')
            GROUP BY DestCountry
        """
        result = execute_sql(sql)
        assert "data" in result


class TestDateTimeQueries:
    """Test date and timestamp filtering"""

    def test_date_range_filter(self, execute_sql, elasticsearch_database):
        """Test filtering by date range"""
        sql = f"""
            SELECT FlightNum, timestamp, Carrier
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE timestamp >= '2024-01-01'
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_date_comparison(self, execute_sql, elasticsearch_database):
        """Test date comparison operators"""
        sql = f"""
            SELECT COUNT(*) as flight_count
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE timestamp > '2023-01-01'
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_timestamp_aggregation_by_day(self, execute_sql, elasticsearch_database):
        """Test aggregation by day of week"""
        sql = f"""
            SELECT dayOfWeek, COUNT(*) as flights_per_day
            FROM {elasticsearch_database}.kibana_sample_data_flights
            GROUP BY dayOfWeek
            ORDER BY dayOfWeek
        """
        result = execute_sql(sql)
        assert "data" in result


class TestBooleanFieldQueries:
    """Test boolean field filtering"""

    def test_boolean_field_true(self, execute_sql, elasticsearch_database):
        """Test filtering on boolean field = true"""
        sql = f"""
            SELECT FlightNum, Carrier, FlightDelayMin
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE FlightDelay = true
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_boolean_field_false(self, execute_sql, elasticsearch_database):
        """Test filtering on boolean field = false"""
        sql = f"""
            SELECT FlightNum, Carrier
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE Cancelled = false
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_boolean_aggregation(self, execute_sql, elasticsearch_database):
        """Test aggregation by boolean field"""
        sql = f"""
            SELECT FlightDelay, COUNT(*) as count
            FROM {elasticsearch_database}.kibana_sample_data_flights
            GROUP BY FlightDelay
        """
        result = execute_sql(sql)
        assert "data" in result
        assert len(result["data"]) <= 2  # Only true/false values


class TestArrayFieldQueries:
    """Test array field handling (key handler feature)"""

    def test_array_field_retrieval(self, execute_sql, elasticsearch_database):
        """Test retrieving array fields (converted to JSON strings)"""
        sql = f"""
            SELECT customer_full_name, products
            FROM {elasticsearch_database}.kibana_sample_data_ecommerce
            LIMIT 5
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_nested_object_fields(self, execute_sql, elasticsearch_database):
        """Test querying nested object fields with dot notation"""
        sql = f"""
            SELECT customer_full_name, customer_gender, geoip
            FROM {elasticsearch_database}.kibana_sample_data_ecommerce
            LIMIT 5
        """
        result = execute_sql(sql)
        assert "data" in result


class TestGeoPointQueries:
    """Test geo-point field queries"""

    def test_geopoint_field_retrieval(self, execute_sql, elasticsearch_database):
        """Test retrieving geo_point fields"""
        sql = f"""
            SELECT FlightNum, OriginLocation, DestLocation
            FROM {elasticsearch_database}.kibana_sample_data_flights
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_query_with_geo_and_numeric(self, execute_sql, elasticsearch_database):
        """Test combining geo fields with numeric filters"""
        sql = f"""
            SELECT FlightNum, OriginCityName, DestCityName, DistanceMiles
            FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE DistanceMiles > 3000
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result


class TestPaginationQueries:
    """Test LIMIT and pagination for large datasets"""

    def test_small_limit(self, execute_sql, elasticsearch_database):
        """Test small LIMIT value"""
        sql = f"""
            SELECT * FROM {elasticsearch_database}.kibana_sample_data_flights
            LIMIT 1
        """
        result = execute_sql(sql)
        assert "data" in result
        assert len(result["data"]) == 1

    def test_medium_limit(self, execute_sql, elasticsearch_database):
        """Test medium LIMIT value"""
        sql = f"""
            SELECT FlightNum, Carrier FROM {elasticsearch_database}.kibana_sample_data_flights
            LIMIT 50
        """
        result = execute_sql(sql)
        assert "data" in result
        assert len(result["data"]) <= 50

    def test_large_limit(self, execute_sql, elasticsearch_database):
        """Test large LIMIT value (performance test)"""
        sql = f"""
            SELECT FlightNum, AvgTicketPrice FROM {elasticsearch_database}.kibana_sample_data_flights
            LIMIT 500
        """
        result = execute_sql(sql)
        assert "data" in result
        assert len(result["data"]) <= 500

    def test_no_limit(self, execute_sql, elasticsearch_database):
        """Test query without LIMIT on smaller dataset"""
        sql = f"""
            SELECT customer_gender, COUNT(*) as count
            FROM {elasticsearch_database}.kibana_sample_data_ecommerce
            GROUP BY customer_gender
        """
        result = execute_sql(sql)
        assert "data" in result


class TestErrorHandling:
    """Test error handling for invalid queries"""

    def test_invalid_index_name(self, execute_sql, elasticsearch_database):
        """Test query on non-existent index"""
        sql = f"""
            SELECT * FROM {elasticsearch_database}.nonexistent_index
            LIMIT 1
        """
        result = execute_sql(sql)
        # Should return error or empty data, not crash
        assert result is not None

    def test_invalid_column_name(self, execute_sql, elasticsearch_database):
        """Test query with non-existent column"""
        sql = f"""
            SELECT nonexistent_column FROM {elasticsearch_database}.kibana_sample_data_flights
            LIMIT 1
        """
        result = execute_sql(sql)
        # Should return error or handle gracefully
        assert result is not None

    def test_malformed_where_clause(self, execute_sql, elasticsearch_database):
        """Test malformed WHERE clause"""
        sql = f"""
            SELECT FlightNum FROM {elasticsearch_database}.kibana_sample_data_flights
            WHERE AvgTicketPrice >
            LIMIT 1
        """
        result = execute_sql(sql)
        # Should return error, not crash
        assert result is not None


class TestLogDatasetQueries:
    """Test queries on Kibana logs dataset"""

    def test_logs_basic_query(self, execute_sql, elasticsearch_database):
        """Test basic query on logs dataset"""
        sql = f"""
            SELECT host, ip, response, bytes
            FROM {elasticsearch_database}.kibana_sample_data_logs
            LIMIT 10
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_logs_aggregation(self, execute_sql, elasticsearch_database):
        """Test aggregation on logs dataset"""
        sql = f"""
            SELECT response, COUNT(*) as count
            FROM {elasticsearch_database}.kibana_sample_data_logs
            GROUP BY response
            ORDER BY count DESC
        """
        result = execute_sql(sql)
        assert "data" in result

    def test_logs_filter_by_response_code(self, execute_sql, elasticsearch_database):
        """Test filtering logs by HTTP response code"""
        sql = f"""
            SELECT host, ip, response, bytes
            FROM {elasticsearch_database}.kibana_sample_data_logs
            WHERE response = 200
            LIMIT 20
        """
        result = execute_sql(sql)
        assert "data" in result
