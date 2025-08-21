import os
import psycopg2
import pytest
import requests

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "test_db")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )


@pytest.fixture(scope="module")
def db_conn():
    conn = get_connection()
    yield conn
    conn.close()


def test_connection(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT 1;")
        assert cur.fetchone()[0] == 1


def test_create_postgresql_db_via_http():
    sql = """
    CREATE DATABASE postgresql_db
    WITH ENGINE = "postgres",
    PARAMETERS = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres", 
        "password": "postgres",
        "database": "test_db"
    };
    """
    resp = requests.post("http://localhost:47334/api/sql/query",
                         json={"query": sql})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("type") != "error", f"Error: {data}"
    select_sql = """
    SELECT * FROM postgresql_db.test_table;
    """

    resp = requests.post(
        "http://localhost:47334/api/sql/query", json={"query": select_sql}
    )
    assert resp.status_code == 200
    data = resp.json()
    print(f"::notice::Data returned: {data}")
    assert data.get("type") != "error", f"Error: {data}"
