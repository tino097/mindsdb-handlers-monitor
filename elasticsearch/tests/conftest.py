"""
pytest fixtures for Elasticsearch handler tests.

Provides reusable test fixtures for:
- MindsDB API connection
- Elasticsearch database connection
- Test data setup and teardown
"""

import os
import pytest
import requests
from time import sleep
from typing import Dict, Any


@pytest.fixture(scope="session")
def mindsdb_api_url() -> str:
    """MindsDB API base URL"""
    return os.getenv("MINDSDB_API_URL", "http://localhost:47334")


@pytest.fixture(scope="session")
def elasticsearch_config() -> Dict[str, str]:
    """Elasticsearch connection configuration"""
    return {
        "hosts": os.getenv("ELASTICSEARCH_HOST", "localhost:9200"),
        "user": os.getenv("ELASTICSEARCH_USER", "elastic"),
        "password": os.getenv("ELASTICSEARCH_PASSWORD", "changeme")
    }


@pytest.fixture(scope="session")
def wait_for_mindsdb(mindsdb_api_url: str):
    """Wait for MindsDB to be ready"""
    max_attempts = 60
    for i in range(max_attempts):
        try:
            response = requests.get(f"{mindsdb_api_url}/api/status", timeout=2)
            if response.status_code == 200:
                print(f"✅ MindsDB is ready at {mindsdb_api_url}")
                return
        except requests.exceptions.RequestException:
            pass

        if i < max_attempts - 1:
            sleep(1)

    raise RuntimeError(f"MindsDB did not start within {max_attempts} seconds")


@pytest.fixture(scope="session")
def elasticsearch_database(mindsdb_api_url: str, elasticsearch_config: Dict[str, str], wait_for_mindsdb):
    """
    Create Elasticsearch database connection in MindsDB.

    This fixture:
    1. Creates the database connection
    2. Verifies connectivity
    3. Cleans up after tests
    """
    db_name = "test_elasticsearch"

    # Create database
    create_query = f"""
    CREATE DATABASE {db_name}
    WITH ENGINE = 'elasticsearch',
    PARAMETERS = {{
        "hosts": "{elasticsearch_config['hosts']}",
        "user": "{elasticsearch_config['user']}",
        "password": "{elasticsearch_config['password']}"
    }};
    """

    response = requests.post(
        f"{mindsdb_api_url}/api/sql/query",
        json={"query": create_query},
        timeout=30
    )

    if response.status_code not in (200, 201):
        print(f"Warning: Database creation returned status {response.status_code}")
        print(f"Response: {response.text}")

    # Verify connection
    check_query = f"SELECT 1 FROM {db_name}.kibana_sample_data_flights LIMIT 1;"

    max_attempts = 5
    connection_ok = False
    for i in range(max_attempts):
        try:
            response = requests.post(
                f"{mindsdb_api_url}/api/sql/query",
                json={"query": check_query},
                timeout=10
            )
            if response.status_code == 200:
                connection_ok = True
                print(f"✅ Elasticsearch database '{db_name}' connected")
                break
        except Exception as e:
            print(f"Connection check attempt {i+1}/{max_attempts}: {e}")

        sleep(2)

    if not connection_ok:
        print(f"⚠️  Could not verify Elasticsearch connection (this may be OK if using mocks)")

    yield db_name

    # Cleanup
    drop_query = f"DROP DATABASE {db_name};"
    try:
        requests.post(
            f"{mindsdb_api_url}/api/sql/query",
            json={"query": drop_query},
            timeout=10
        )
        print(f"✅ Cleaned up database '{db_name}'")
    except Exception as e:
        print(f"⚠️  Cleanup warning: {e}")


@pytest.fixture
def execute_sql(mindsdb_api_url: str):
    """Helper to execute SQL queries against MindsDB"""
    def _execute(query: str, timeout: int = 30) -> Dict[str, Any]:
        """
        Execute SQL query and return response.

        Args:
            query: SQL query string
            timeout: Request timeout in seconds

        Returns:
            Response JSON dict
        """
        response = requests.post(
            f"{mindsdb_api_url}/api/sql/query",
            json={"query": query},
            timeout=timeout
        )
        return response.json()

    return _execute


@pytest.fixture
def sample_index_name() -> str:
    """Name of a Kibana sample index to use for testing"""
    # Try different Kibana sample datasets
    options = [
        "kibana_sample_data_flights",
        "kibana_sample_data_ecommerce",
        "kibana_sample_data_logs"
    ]
    return options[0]  # Default to flights dataset
