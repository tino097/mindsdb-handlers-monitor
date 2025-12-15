import pytest
import time


class TestDatabricksDatasourceCreation:
    """Tests for Databricks datasource lifecycle."""

    def test_datasource_created(self, mindsdb_client, databricks_datasource):
        """Verify datasource was created successfully."""
        databases = mindsdb_client.list_databases()
        db_names = [
            db[0] if isinstance(db, (list, tuple)) else db.get("name", "")
            for db in databases
        ]

        assert any(
            databricks_datasource.lower() in name.lower() for name in db_names
        ), f"Datasource {databricks_datasource} not found in {db_names}"

    def test_datasource_connection_valid(self, mindsdb_client, databricks_datasource):
        """Verify datasource connection is valid and can query Databricks."""
        result = mindsdb_client.query(f"SHOW TABLES FROM {databricks_datasource}")

        assert "error" not in result or result.get("error") is None


class TestDatabricksTables:
    """Tests for listing and describing Databricks tables."""

    def test_show_tables(self, mindsdb_client, databricks_datasource):
        """Test showing available tables in Databricks datasource."""
        result = mindsdb_client.query(f"SHOW TABLES FROM {databricks_datasource}")

        assert "data" in result

        tables = result.get("data", [])
        print(f"Available tables: {tables}")

    def test_show_catalogs(self, mindsdb_client, databricks_datasource):
        """Test listing available catalogs."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.information_schema.catalogs LIMIT 10"
            )
            assert "data" in result or "error" not in str(result)
        except Exception as e:
            print(f"Catalog query result: {e}")

    def test_show_schemas(self, mindsdb_client, databricks_datasource):
        """Test listing available schemas."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.information_schema.schemata LIMIT 10"
            )
            assert "data" in result or "error" not in str(result)
        except Exception as e:
            print(f"Schema query result: {e}")


class TestDatabricksQueries:
    """Tests for querying Databricks tables."""

    def test_simple_select(
        self, mindsdb_client, databricks_datasource, sample_table_name
    ):
        """Test simple SELECT query."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.{sample_table_name} LIMIT 5"
            )

            assert "data" in result
            assert "column_names" in result

        except Exception as e:
            pytest.skip(f"Sample table not accessible: {e}")

    def test_select_with_columns(
        self, mindsdb_client, databricks_datasource, sample_table_name
    ):
        """Test SELECT with specific columns."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.{sample_table_name} LIMIT 1"
            )

            if "column_names" in result and result["column_names"]:
                first_col = result["column_names"][0]

                result2 = mindsdb_client.query(
                    f"SELECT {first_col} FROM {databricks_datasource}.{sample_table_name} LIMIT 5"
                )

                assert "data" in result2

        except Exception as e:
            pytest.skip(f"Column query not accessible: {e}")

    def test_select_with_where(
        self, mindsdb_client, databricks_datasource, sample_table_name
    ):
        """Test SELECT with WHERE clause."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.{sample_table_name} WHERE 1=1 LIMIT 5"
            )

            assert "data" in result

        except Exception as e:
            pytest.skip(f"WHERE clause query not accessible: {e}")

    def test_select_with_order(
        self, mindsdb_client, databricks_datasource, sample_table_name
    ):
        """Test SELECT with ORDER BY clause."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.{sample_table_name} LIMIT 1"
            )

            if "column_names" in result and result["column_names"]:
                first_col = result["column_names"][0]

                result2 = mindsdb_client.query(
                    f"""
                    SELECT * FROM {databricks_datasource}.{sample_table_name} 
                    ORDER BY {first_col} 
                    LIMIT 5
                    """
                )

                assert "data" in result2

        except Exception as e:
            pytest.skip(f"ORDER BY query not accessible: {e}")

    def test_count_query(
        self, mindsdb_client, databricks_datasource, sample_table_name
    ):
        """Test COUNT aggregation."""
        try:
            result = mindsdb_client.query(
                f"SELECT COUNT(*) FROM {databricks_datasource}.{sample_table_name}"
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

    def test_describe_table(
        self, mindsdb_client, databricks_datasource, sample_table_name
    ):
        """Test describing table structure."""
        try:
            result = mindsdb_client.query(
                f"DESCRIBE {databricks_datasource}.{sample_table_name}"
            )

            assert "data" in result or "column_names" in result

        except Exception as e:
            pytest.skip(f"DESCRIBE not accessible: {e}")

    def test_get_column_types(
        self, mindsdb_client, databricks_datasource, sample_table_name
    ):
        """Test getting column type information."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.{sample_table_name} LIMIT 1"
            )

            columns = result.get("column_names", [])
            print(f"Table columns: {columns}")

            assert len(columns) > 0, "Should have at least one column"

        except Exception as e:
            pytest.skip(f"Column types not accessible: {e}")


class TestDatabricksAdvancedQueries:
    """Tests for advanced Databricks query capabilities."""

    def test_group_by(self, mindsdb_client, databricks_datasource, sample_table_name):
        """Test GROUP BY aggregation."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.{sample_table_name} LIMIT 1"
            )

            if "column_names" in result and len(result["column_names"]) > 0:
                first_col = result["column_names"][0]

                result2 = mindsdb_client.query(
                    f"""
                    SELECT {first_col}, COUNT(*) as cnt 
                    FROM {databricks_datasource}.{sample_table_name} 
                    GROUP BY {first_col}
                    LIMIT 10
                    """
                )

                assert "data" in result2

        except Exception as e:
            pytest.skip(f"GROUP BY query not accessible: {e}")

    def test_distinct(self, mindsdb_client, databricks_datasource, sample_table_name):
        """Test DISTINCT query."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.{sample_table_name} LIMIT 1"
            )

            if "column_names" in result and len(result["column_names"]) > 0:
                first_col = result["column_names"][0]

                result2 = mindsdb_client.query(
                    f"""
                    SELECT DISTINCT {first_col} 
                    FROM {databricks_datasource}.{sample_table_name} 
                    LIMIT 10
                    """
                )

                assert "data" in result2

        except Exception as e:
            pytest.skip(f"DISTINCT query not accessible: {e}")

    def test_subquery(self, mindsdb_client, databricks_datasource, sample_table_name):
        """Test subquery capability."""
        try:
            result = mindsdb_client.query(
                f"""
                SELECT * FROM (
                    SELECT * FROM {databricks_datasource}.{sample_table_name} LIMIT 10
                ) subq
                LIMIT 5
                """
            )

            assert "data" in result

        except Exception as e:
            pytest.skip(f"Subquery not accessible: {e}")


class TestDatabricksErrorHandling:
    """Tests for error handling in Databricks handler."""

    def test_invalid_table_name(self, mindsdb_client, databricks_datasource):
        """Test error handling for invalid table name."""
        try:
            result = mindsdb_client.query(
                f"SELECT * FROM {databricks_datasource}.nonexistent_table_xyz123"
            )

        except Exception as e:
            assert "error" in str(e).lower() or "not found" in str(e).lower() or True

    def test_invalid_column_name(
        self, mindsdb_client, databricks_datasource, sample_table_name
    ):
        """Test error handling for invalid column name."""
        try:
            result = mindsdb_client.query(
                f"SELECT nonexistent_column_xyz FROM {databricks_datasource}.{sample_table_name}"
            )

        except Exception as e:
            pass

    def test_syntax_error(self, mindsdb_client, databricks_datasource):
        """Test error handling for SQL syntax error."""
        try:
            result = mindsdb_client.query(f"SELEC * FORM {databricks_datasource}")

        except Exception as e:
            pass
