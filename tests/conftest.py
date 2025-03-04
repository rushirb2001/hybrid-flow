"""Pytest configuration and fixtures for HybridFlow tests."""

import os
import tempfile
from pathlib import Path
from typing import Generator

import pytest
from qdrant_client import QdrantClient
from neo4j import GraphDatabase


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return path to test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def bailey_data_dir(test_data_dir: Path) -> Path:
    """Return path to Bailey test data."""
    return test_data_dir / "bailey"


@pytest.fixture(scope="session")
def sabiston_data_dir(test_data_dir: Path) -> Path:
    """Return path to Sabiston test data."""
    return test_data_dir / "sabiston"


@pytest.fixture(scope="session")
def schwartz_data_dir(test_data_dir: Path) -> Path:
    """Return path to Schwartz test data."""
    return test_data_dir / "schwartz"


@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """Provide a temporary database file path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = Path(tmp_file.name)

    yield db_path

    # Cleanup
    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def qdrant_test_client() -> Generator[QdrantClient, None, None]:
    """Provide a Qdrant test client with in-memory storage."""
    client = QdrantClient(":memory:")
    yield client
    # Cleanup happens automatically with in-memory storage


@pytest.fixture
def qdrant_host() -> str:
    """Return Qdrant host from environment or default."""
    return os.getenv("QDRANT_HOST", "localhost")


@pytest.fixture
def qdrant_port() -> int:
    """Return Qdrant port from environment or default."""
    return int(os.getenv("QDRANT_PORT", "6333"))


@pytest.fixture
def neo4j_uri() -> str:
    """Return Neo4j URI from environment or default."""
    return os.getenv("NEO4J_URI", "bolt://localhost:7687")


@pytest.fixture
def neo4j_user() -> str:
    """Return Neo4j user from environment or default."""
    return os.getenv("NEO4J_USER", "neo4j")


@pytest.fixture
def neo4j_password() -> str:
    """Return Neo4j password from environment or default."""
    return os.getenv("NEO4J_PASSWORD", "password")


@pytest.fixture
def neo4j_test_driver(
    neo4j_uri: str, neo4j_user: str, neo4j_password: str
) -> Generator[GraphDatabase.driver, None, None]:
    """Provide a Neo4j test driver connection."""
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    yield driver

    # Cleanup: Clear test data
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    driver.close()


@pytest.fixture
def sample_json_data() -> dict:
    """Provide sample JSON data for testing."""
    return {
        "id": "test-001",
        "title": "Test Document",
        "content": "This is a test document for HybridFlow",
        "metadata": {
            "source": "test",
            "version": "1.0",
            "tags": ["test", "sample"]
        }
    }


@pytest.fixture
def sample_hierarchical_data() -> dict:
    """Provide sample hierarchical data for testing."""
    return {
        "id": "chapter-001",
        "title": "Introduction",
        "sections": [
            {
                "id": "section-001-01",
                "title": "Overview",
                "content": "This is the overview section",
                "subsections": [
                    {
                        "id": "subsection-001-01-01",
                        "title": "Background",
                        "content": "Background information"
                    }
                ]
            }
        ]
    }
