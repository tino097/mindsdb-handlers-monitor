import os
import psycopg2
import pytest

DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_USER = os.getenv('POSTGRES_USER', 'postgres')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
DB_NAME = os.getenv('POSTGRES_DB', 'test_db')

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
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

def test_insert_and_query(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, value TEXT);")
        cur.execute("INSERT INTO test_table (value) VALUES ('testdata') RETURNING id;")
        inserted_id = cur.fetchone()[0]
        cur.execute("SELECT value FROM test_table WHERE id = %s;", (inserted_id,))
        value = cur.fetchone()[0]
        assert value == 'testdata'
        cur.execute("DELETE FROM test_table WHERE id = %s;", (inserted_id,))
        db_conn.commit()
