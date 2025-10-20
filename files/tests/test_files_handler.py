import pytest
from conftest import execute_sql_via_mindsdb


@pytest.mark.handler
class TestFilesHandler:
    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM files WHERE id = 1",
            "SELECT * FROM files WHERE name = 'test.txt'",
        ],
    )
    def test_file_queries(self, query):
        response = execute_sql_via_mindsdb(query)
        assert response.get("status") == "success"

    def test_file_count(self):
        query = "SELECT COUNT(*) FROM files"
        response = execute_sql_via_mindsdb(query)
        assert response.get("status") == "success"
        assert len(response.get("data", [])) == 1

    def test_file_insert(self):
        query = "INSERT INTO files (id, name) VALUES (2, 'new_file.txt')"
        response = execute_sql_via_mindsdb(query)
        assert response.get("status") == "success"
