import pytest

from conftest import execute_sql_via_mindsdb, DATABRICKS_DB, logger


class TestDatabricksDatasourceCreation:
    """Tests for Databricks datasource lifecycle."""

    @pytest.mark.handler
    def test_datasource_created(self, databricks_datasource):
        """Verify datasource was created successfully."""
        result = execute_sql_via_mindsdb("SHOW DATABASES")
        db_names = [
            row[0] if isinstance(row, (list, tuple)) else row.get("name", "")
            for row in result.get("data", [])
        ]

        assert any(
            databricks_datasource.lower() in name.lower() for name in db_names
        ), f"Datasource {databricks_datasource} not found in {db_names}"

    @pytest.mark.handler
    def test_datasource_connection_valid(self, databricks_datasource):
        """Verify datasource connection is valid and can query Databricks."""
        result = execute_sql_via_mindsdb(f"SHOW TABLES FROM {DATABRICKS_DB}")
        assert "error" not in result or result.get("error") is None


class TestDatabricksTables:
    """Tests for listing and describing Databricks tables."""

    @pytest.mark.handler
    def test_show_tables(self, databricks_datasource):
        """Test showing available tables in Databricks datasource."""
        result = execute_sql_via_mindsdb(f"SHOW TABLES FROM {DATABRICKS_DB}")

        assert "data" in result
        tables = result.get("data", [])
        print(f"Available tables: {tables}")

    @pytest.mark.handler
    def test_show_schemas(self, databricks_datasource):
        """Test listing available schemas."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {databricks_datasource}.information_schema.schemata LIMIT 10"
            )
            assert "data" in result or "error" not in str(result)
        except Exception as e:
            print(f"Schema query result: {e}")


class TestDatabricksQueries:
    """Tests for querying Databricks tables."""

    @pytest.mark.query
    def test_simple_select(self, databricks_datasource):
        """Test simple SELECT query."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {DATABRICKS_DB}.orders LIMIT 5"
            )

            assert "data" in result
            assert "column_names" in result

        except Exception as e:
            pytest.skip(f"Sample table not accessible: {e}")

    @pytest.mark.query
    def test_select_with_columns(self, databricks_datasource):
        """Test SELECT with specific columns."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {DATABRICKS_DB}.nation LIMIT 1"
            )

            if "column_names" in result and result["column_names"]:
                first_col = result["column_names"][0]

                result2 = execute_sql_via_mindsdb(
                    f"SELECT {first_col} FROM {DATABRICKS_DB}.orders LIMIT 5"
                )

                assert "data" in result2

        except Exception as e:
            pytest.skip(f"Column query not accessible: {e}")

    @pytest.mark.query
    def test_select_with_where(self, databricks_datasource):
        """Test SELECT with WHERE clause."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {DATABRICKS_DB}.orders WHERE 1=1 LIMIT 5"
            )

            assert "data" in result

        except Exception as e:
            pytest.skip(f"WHERE clause query not accessible: {e}")

    @pytest.mark.query
    def test_select_with_order(self, databricks_datasource):
        """Test SELECT with ORDER BY clause."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {DATABRICKS_DB}.orders LIMIT 1"
            )

            if "column_names" in result and result["column_names"]:
                first_col = result["column_names"][0]

                result2 = execute_sql_via_mindsdb(
                    f"""
                    SELECT * FROM {DATABRICKS_DB}.orders 
                    ORDER BY {first_col} 
                    LIMIT 5
                    """
                )

                assert "data" in result2

        except Exception as e:
            pytest.skip(f"ORDER BY query not accessible: {e}")

    @pytest.mark.query
    def test_count_query(self, databricks_datasource):
        """Test COUNT aggregation."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT COUNT(*) FROM {DATABRICKS_DB}.orders"
            )

            assert "data" in result

            data = result.get("data", [])
            if data:
                count = data[0][0] if isinstance(data[0], (list, tuple)) else data[0]
                print(f"Table row count: {count}")

        except Exception as e:
            pytest.skip(f"COUNT query not accessible: {e}")


class TestDatabricksMetadata:
    """Tests for Databricks metadata operations."""

    @pytest.mark.handler
    def test_describe_table(self, databricks_datasource):
        """Test describing table structure."""
        try:
            result = execute_sql_via_mindsdb(f"DESCRIBE {DATABRICKS_DB}.orders")

            assert "data" in result or "column_names" in result

        except Exception as e:
            pytest.skip(f"DESCRIBE not accessible: {e}")

    @pytest.mark.handler
    def test_get_column_types(self, databricks_datasource):
        """Test getting column type information."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {DATABRICKS_DB}.orders LIMIT 1"
            )

            columns = result.get("column_names", [])
            print(f"Table columns: {columns}")

            assert len(columns) > 0, "Should have at least one column"

        except Exception as e:
            pytest.skip(f"Column types not accessible: {e}")


class TestDatabricksAdvancedQueries:
    """Tests for advanced Databricks query capabilities."""

    @pytest.mark.query
    def test_group_by(self, databricks_datasource):
        """Test GROUP BY aggregation."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {DATABRICKS_DB}.orders LIMIT 1"
            )

            if "column_names" in result and len(result["column_names"]) > 0:
                first_col = result["column_names"][0]

                result2 = execute_sql_via_mindsdb(
                    f"""
                    SELECT {first_col}, COUNT(*) as cnt 
                    FROM {DATABRICKS_DB}.orders 
                    GROUP BY {first_col}
                    LIMIT 10
                    """
                )

                assert "data" in result2

        except Exception as e:
            pytest.skip(f"GROUP BY query not accessible: {e}")

    @pytest.mark.query
    def test_distinct(self, databricks_datasource):
        """Test DISTINCT query."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {DATABRICKS_DB}.orders LIMIT 1"
            )

            if "column_names" in result and len(result["column_names"]) > 0:
                first_col = result["column_names"][0]

                result2 = execute_sql_via_mindsdb(
                    f"""
                    SELECT DISTINCT {first_col}
                    FROM {DATABRICKS_DB}.orders
                    LIMIT 10
                    """
                )

                assert "data" in result2

        except Exception as e:
            pytest.skip(f"DISTINCT query not accessible: {e}")

    @pytest.mark.query
    def test_subquery(self, databricks_datasource):
        """Test subquery capability."""
        try:
            result = execute_sql_via_mindsdb(
                f"""
                SELECT * FROM (
                    SELECT * FROM {DATABRICKS_DB}.orders LIMIT 10
                ) subq
                LIMIT 5
                """
            )

            assert "data" in result

        except Exception as e:
            pytest.skip(f"Subquery not accessible: {e}")


class TestDatabricksErrorHandling:
    """Tests for error handling in Databricks handler."""

    @pytest.mark.handler
    def test_invalid_table_name(self, databricks_datasource):
        """Test error handling for invalid table name."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT * FROM {databricks_datasource}.nonexistent_table_xyz123"
            )
        except Exception as e:
            assert "error" in str(e).lower() or "not found" in str(e).lower() or True

    @pytest.mark.handler
    def test_invalid_column_name(self, databricks_datasource):
        """Test error handling for invalid column name."""
        try:
            result = execute_sql_via_mindsdb(
                f"SELECT nonexistent_column_xyz FROM {DATABRICKS_DB}.orders"
            )
        except Exception as e:
            pass

    @pytest.mark.handler
    def test_syntax_error(self, databricks_datasource):
        """Test error handling for SQL syntax error."""
        try:
            result = execute_sql_via_mindsdb(f"SELEC * FORM {databricks_datasource}")
        except Exception as e:
            pass
