import os
import psycopg2
import pytest
import mindsdb

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


def test_mindsdb_create_postgresql_database():
    server = mindsdb.connect("http://127.0.0.1:47334")
    sql = """
    CREATE DATABASE postgresql_db
    WITH ENGINE = "postgresql",
    PARAMETERS = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres", 
        "password": "postgres",
        "database": "test_db"
    };
    """
    result = server.sql(sql)
    # Check that the database was created successfully
    assert (
        result.success
    ), f"Failed to create database: {result.error if hasattr(result, 'error') else result}"
