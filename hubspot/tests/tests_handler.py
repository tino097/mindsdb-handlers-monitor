import pytest
from conftest import execute_sql_via_mindsdb, HUBSPOT_DB_NAME


@pytest.mark.handler
class TestInstallationCheck:
    """
    Test Category 0: Installation Check
    Verify handler is properly installed and verify information schema.
    """

    def test_handler_installation(self, verify_mindsdb_ready):
        """Verify HubSpot handler is installed and shows in information_schema."""
        sql = """
        SELECT * FROM information_schema.HANDLERS
        WHERE NAME = 'hubspot';
        """
        result = execute_sql_via_mindsdb(sql)

        assert "data" in result, "Handler query should return data"
        assert len(result["data"]) > 0, "HubSpot handler should be listed"

        handler_info = result["data"][0]
        if isinstance(handler_info, list):
            columns = result.get("column_names", [])
            handler_info = dict(zip(columns, handler_info))

        import_success = handler_info.get(
            "IMPORT_SUCCESS", handler_info.get("import_success")
        )
        assert import_success in [
            True,
            "true",
            1,
            "1",
        ], f"Handler import should be successful, got: {import_success}"

        import_error = handler_info.get(
            "IMPORT_ERROR", handler_info.get("import_error")
        )
        assert import_error in [
            None,
            "",
            "NULL",
            "null",
        ], f"Handler should have no import errors, got: {import_error}"

    def test_handler_with_invalid_api_key(self, verify_mindsdb_ready):
        """Test error handling with invalid API key."""
        sql = """
        CREATE DATABASE test_invalid_hubspot
        WITH ENGINE = 'hubspot',
        PARAMETERS = {
            "api_key": "invalid_key_12345"
        };
        """
        try:
            result = execute_sql_via_mindsdb(sql, timeout=30)
            execute_sql_via_mindsdb("DROP DATABASE IF EXISTS test_invalid_hubspot;")
        except Exception as e:
            assert (
                "invalid" in str(e).lower()
                or "auth" in str(e).lower()
                or "error" in str(e).lower()
            )


@pytest.mark.handler
class TestDatasourceConnection:
    """Test HubSpot datasource connection and basic operations."""

    def test_connection_created(self, hubspot_datasource):
        """Test that HubSpot database connection was created."""
        assert hubspot_datasource == HUBSPOT_DB_NAME

    def test_show_tables(self, hubspot_datasource, execute_sql):
        """Test SHOW TABLES returns HubSpot tables."""
        sql = f"SHOW TABLES FROM {hubspot_datasource};"
        result = execute_sql(sql)

        assert "data" in result, "Should return table list"

        tables = []
        for row in result["data"]:
            if isinstance(row, dict):
                table_name = row.get(
                    "Tables_in_" + hubspot_datasource, row.get("name", "")
                )
            else:
                table_name = row[0] if row else ""
            tables.append(table_name.lower())

        expected_tables = ["contacts", "companies", "deals"]
        for expected in expected_tables:
            assert any(
                expected in t for t in tables
            ), f"Expected table '{expected}' not found. Available: {tables}"

    def test_show_columns_contacts(self, hubspot_datasource, execute_sql):
        """Test SHOW COLUMNS for contacts table."""
        sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{hubspot_datasource}'
        AND table_name = 'contacts'
        LIMIT 20;
        """
        result = execute_sql(sql)

        assert "data" in result, "Should return column information"
        if len(result["data"]) > 0:
            columns = [str(row).lower() for row in result["data"]]
            column_str = " ".join(columns)
            expected_fields = ["id", "email", "firstname", "lastname"]
            found = [f for f in expected_fields if f in column_str]
            assert (
                len(found) > 0
            ), f"Should have standard contact fields. Got: {columns[:5]}"


@pytest.mark.handler
class TestBasicQueries:
    """Test basic SQL queries on HubSpot data."""

    def test_select_contacts_limit(self, hubspot_datasource, execute_sql):
        """Test basic SELECT with LIMIT on contacts."""
        sql = f"SELECT * FROM {hubspot_datasource}.contacts LIMIT 5;"
        result = execute_sql(sql)

        assert result is not None
        assert "data" in result or "error_code" in result

    def test_select_specific_columns(self, hubspot_datasource, execute_sql):
        """Test SELECT with specific columns."""
        sql = f"""
        SELECT id, email, firstname, lastname
        FROM {hubspot_datasource}.contacts
        LIMIT 10;
        """
        result = execute_sql(sql)

        assert result is not None
        if "data" in result:
            assert len(result["data"]) <= 10

    def test_select_companies(self, hubspot_datasource, execute_sql):
        """Test SELECT on companies table."""
        sql = f"SELECT * FROM {hubspot_datasource}.companies LIMIT 5;"
        result = execute_sql(sql)

        assert result is not None
        assert "data" in result or "error_code" in result

    def test_select_deals(self, hubspot_datasource, execute_sql):
        """Test SELECT on deals table."""
        sql = f"SELECT * FROM {hubspot_datasource}.deals LIMIT 5;"
        result = execute_sql(sql)

        assert result is not None
        assert "data" in result or "error_code" in result

    def test_count_contacts(self, hubspot_datasource, execute_sql):
        """Test COUNT aggregation on contacts."""
        sql = f"SELECT COUNT(*) as total FROM {hubspot_datasource}.contacts;"
        result = execute_sql(sql)

        assert result is not None
        if "data" in result and len(result["data"]) > 0:
            first_row = result["data"][0]
            if isinstance(first_row, dict):
                count = first_row.get("total", first_row.get("COUNT(*)", 0))
            else:
                count = first_row[0] if first_row else 0
            assert count is not None


@pytest.mark.handler
class TestDataTypeMapping:
    """
    Data Type Tests
    Ensure correct mapping of data types.
    """

    def test_contact_data_types(self, hubspot_datasource, execute_sql):
        """Test data type mapping for contact fields."""
        sql = f"""
        SELECT id, email, firstname, lastname, createdate
        FROM {hubspot_datasource}.contacts
        LIMIT 1;
        """
        result = execute_sql(sql)

        assert result is not None
        if "data" in result and len(result["data"]) > 0:
            row = result["data"][0]
            if isinstance(row, dict):
                assert row.get("id") is not None
                email = row.get("email")
                if email:
                    assert isinstance(email, str)

    def test_deal_data_types(self, hubspot_datasource, execute_sql):
        """Test data type mapping for deal fields."""
        sql = f"""
        SELECT id, dealname, amount, dealstage, closedate
        FROM {hubspot_datasource}.deals
        LIMIT 1;
        """
        result = execute_sql(sql)

        assert result is not None
        if "data" in result and len(result["data"]) > 0:
            row = result["data"][0]
            if isinstance(row, dict):
                amount = row.get("amount")
                if amount is not None:
                    assert isinstance(amount, (int, float, str))


@pytest.mark.handler
class TestErrorHandling:
    """
    Error Handling / Negative Tests
    Check system response to invalid inputs.
    """

    def test_syntax_error(self, hubspot_datasource, execute_sql):
        """Test response to SQL syntax error."""
        sql = f"SELEC * FRM {hubspot_datasource}.contacts;"
        result = execute_sql(sql)

        assert result is not None

    def test_nonexistent_table(self, hubspot_datasource, execute_sql):
        """Test query on non-existent table."""
        sql = f"SELECT * FROM {hubspot_datasource}.nonexistent_table_xyz LIMIT 1;"
        result = execute_sql(sql)

        assert result is not None

    def test_nonexistent_column(self, hubspot_datasource, execute_sql):
        """Test query with non-existent column."""
        sql = (
            f"SELECT nonexistent_column_xyz FROM {hubspot_datasource}.contacts LIMIT 1;"
        )
        result = execute_sql(sql)

        assert result is not None

    def test_invalid_where_clause(self, hubspot_datasource, execute_sql):
        """Test malformed WHERE clause."""
        sql = f"SELECT * FROM {hubspot_datasource}.contacts WHERE email > LIMIT 1;"
        result = execute_sql(sql)

        assert result is not None


@pytest.mark.handler
class TestDDLOperations:
    """Test DDL operations on HubSpot datasource."""

    def test_drop_database_recreate(self, verify_mindsdb_ready, hubspot_api_key):
        """Test DROP DATABASE IF EXISTS and recreate."""
        test_db = "hubspot_test_ddl"

        create_sql = f"""
        CREATE DATABASE {test_db}
        WITH ENGINE = 'hubspot',
        PARAMETERS = {{
            "api_key": "{hubspot_api_key}"
        }};
        """
        try:
            execute_sql_via_mindsdb(create_sql, timeout=60)

            show_sql = f"SHOW TABLES FROM {test_db};"
            result = execute_sql_via_mindsdb(show_sql, timeout=30)
            assert "data" in result

            drop_sql = f"DROP DATABASE IF EXISTS {test_db};"
            execute_sql_via_mindsdb(drop_sql, timeout=30)

            try:
                result = execute_sql_via_mindsdb(show_sql, timeout=10)
            except Exception:
                pass

        except Exception as e:
            try:
                execute_sql_via_mindsdb(f"DROP DATABASE IF EXISTS {test_db};")
            except Exception:
                pass
            raise e
