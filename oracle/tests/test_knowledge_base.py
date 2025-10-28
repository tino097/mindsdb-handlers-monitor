import os
import time
import logging

import pytest
import requests
from conftest import ORACLE_TPCH_DB, execute_sql_via_mindsdb


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


OLLAMA_API_BASE = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "tinyllama")


class TestOllamaSetup:
    """Verify that Ollama is running and the model is available."""

    def test_ollama_is_running(self):
        """Test that Ollama API is accessible."""
        logger.info("🤖 Testing Ollama connection...")
        response = requests.get(f"{OLLAMA_API_BASE}/api/tags", timeout=5)
        assert response.status_code == 200, "Ollama API is not accessible"
        logger.info("✅ Ollama API is accessible")

    def test_ollama_model_available(self):
        """Test that the specified model is available."""
        logger.info(f"🔍 Checking for {OLLAMA_MODEL} model...")
        response = requests.get(f"{OLLAMA_API_BASE}/api/tags", timeout=5)
        assert response.status_code == 200

        models = response.json().get("models", [])
        model_names = [m.get("name", "").split(":")[0] for m in models]

        assert (
            OLLAMA_MODEL in model_names
        ), f"Model {OLLAMA_MODEL} not found. Available: {model_names}"
        logger.info(f"✅ Model {OLLAMA_MODEL} is available")

    def test_ollama_simple_generation(self):
        """Test Ollama can generate responses."""
        logger.info("🧪 Testing Ollama generation...")
        payload = {
            "model": OLLAMA_MODEL,
            "prompt": "What is 2+2? Answer with just the number.",
            "stream": False,
        }

        response = requests.post(
            f"{OLLAMA_API_BASE}/api/generate", json=payload, timeout=30
        )

        assert response.status_code == 200, "Ollama generation failed"
        result = response.json()
        assert "response" in result, "No response from Ollama"
        assert len(result["response"]) > 0, "Empty response from Ollama"
        logger.info(f"✅ Ollama generated response: {result['response'][:50]}")


class TestKnowledgeBase:
    """Test MindsDB Knowledge Base functionality with Oracle TPC-H data."""

    def test_query_kb_list_regions(self, oracle_regions_kb, mindsdb_connection):
        """Test querying the Knowledge Base to list regions."""
        logger.info("❓ Asking KB to list regions...")

        sql = """
        SELECT chunk_content, distance, relevance
        FROM oracle_regions_kb
        WHERE content = 'What are all the regions?'
        LIMIT 10;
        """

        result = execute_sql_via_mindsdb(sql, timeout=90)
        assert "data" in result, "No data returned from Knowledge Base"
        assert len(result["data"]) > 0, "Empty response from Knowledge Base"

        logger.info(f"✅ KB returned {len(result['data'])} results")

        # Check if regions are in the results
        all_content = " ".join(
            [str(row.get("chunk_content", "")).lower() for row in result["data"]]
        )
        regions = ["africa", "america", "asia", "europe"]
        found_regions = [r for r in regions if r in all_content]
        assert len(found_regions) > 0, f"No regions found."

    def test_query_kb_nations_by_region(self, oracle_regions_kb, mindsdb_connection):
        """Test querying the Knowledge Base for nations in a specific region."""
        logger.info("❓ Asking KB for nations in 'Europe' region...")

        sql = """
            SELECT chunk_content, distance, relevance
            FROM oracle_regions_kb
            WHERE content = 'Which nations are in the Europe region?'
            LIMIT 10;
        """
        result = execute_sql_via_mindsdb(sql, timeout=90)
        assert "data" in result, "No data returned from Knowledge Base"
        assert len(result["data"]) > 0, "Empty response from Knowledge Base"

        logger.info(f"✅ KB returned {len(result['data'])} results")

        # Check if European nations are in the results
        all_content = " ".join(
            [str(row.get("chunk_content", "")).lower() for row in result["data"]]
        )
        european_nations = ["france", "germany", "italy", "spain", "uk"]
        found_nations = [n for n in european_nations if n in all_content]
        assert len(found_nations) > 0, f"No European nations found."


class TestCleanup:
    """Cleanup test resources after all tests complete."""

    def test_cleanup_nation_kb(self, mindsdb_connection):
        """Drop the nation knowledge base."""
        logger.info("🧹 Cleaning up nation KB...")

        sql = "DROP KNOWLEDGE BASE IF EXISTS oracle_nations_kb;"
        try:
            execute_sql_via_mindsdb(sql, timeout=30)
            logger.info("✅ Nation KB dropped")
        except Exception as e:
            logger.warning(f"⚠️ Could not drop nation KB: {e}")

    def test_cleanup_region_kb(self, mindsdb_connection):
        """Drop the region knowledge base."""
        logger.info("🧹 Cleaning up region KB...")

        sql = "DROP KNOWLEDGE BASE IF EXISTS oracle_regions_kb;"
        try:
            execute_sql_via_mindsdb(sql, timeout=30)
            logger.info("✅ Region KB dropped")
        except Exception as e:
            logger.warning(f"⚠️ Could not drop region KB: {e}")

    def test_cleanup_views(self, mindsdb_connection):
        """Drop the test views."""
        logger.info("🧹 Cleaning up views...")

        views = ["oracle_regions_view", "oracle_nations_view"]
        for view in views:
            sql = f"DROP VIEW IF EXISTS {view};"
            try:
                execute_sql_via_mindsdb(sql, timeout=30)
                logger.info(f"✅ View {view} dropped")
            except Exception as e:
                logger.warning(f"⚠️ Could not drop view {view}: {e}")

    def test_cleanup_llm_model(self, mindsdb_connection):
        """Drop the LLM model (optional - might want to keep for other tests)."""
        logger.info("🧹 Cleaning up LLM model...")

        sql = "DROP MODEL IF EXISTS ollama_tinyllama;"
        try:
            execute_sql_via_mindsdb(sql, timeout=30)
            logger.info("✅ LLM model dropped")
        except Exception as e:
            logger.warning(f"⚠️ Could not drop LLM model: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
