"""Pytest configuration and fixtures for HybridFlow tests."""

import os
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from hybridflow.storage.qdrant_client import QdrantStorage
from hybridflow.storage.neo4j_client import Neo4jStorage


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
def qdrant_test_storage() -> Generator[QdrantStorage, None, None]:
    """Provide a Qdrant test storage with production connection."""
    storage = QdrantStorage(
        host=os.getenv("QDRANT_HOST", "localhost"),
        port=int(os.getenv("QDRANT_PORT", "6333")),
    )
    yield storage
    # No cleanup - preserve production data


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
def neo4j_test_storage(
    neo4j_uri: str, neo4j_user: str, neo4j_password: str
) -> Generator[Neo4jStorage, None, None]:
    """Provide a Neo4j test storage connection."""
    storage = Neo4jStorage(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)

    yield storage

    # No cleanup - preserve production data
    storage.close()


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
