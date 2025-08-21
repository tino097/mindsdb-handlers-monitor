import os
import psycopg2
import pytest


@pytest.fixture(scope="session", autouse=True)
def load_test_data():
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        dbname=os.getenv('POSTGRES_DB', 'test_db')
    )
    with conn.cursor() as cur:
        sql_path = os.path.join(
            os.path.dirname(__file__),
            'test_data.sql'
        )
        with open(sql_path) as f:
            cur.execute(f.read())
        conn.commit()
    conn.close()
