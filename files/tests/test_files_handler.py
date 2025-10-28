import pytest
from conftest import execute_sql_via_mindsdb


@pytest.mark.handler
class TestFilesHandler:
    def test_file_queries(self):
        query = "SELECT * FROM files"
        response = execute_sql_via_mindsdb(query)
        assert response.get("status") == "success"
        assert isinstance(response.get("data"), list)
        assert len(response.get("data")) >= 1
        first_row = response["data"][0]
        assert "id" in first_row
        assert "name" in first_row

    def test_file_count(self):
        query = "SELECT COUNT(*) FROM files"
        response = execute_sql_via_mindsdb(query)
        assert response.get("status") == "success"
        assert len(response.get("data", [])) == 1

    def test_file_insert(self):
        query = "INSERT INTO files (id, name) VALUES (2, 'new_file.txt')"
        response = execute_sql_via_mindsdb(query)
        assert response.get("status") == "success"
