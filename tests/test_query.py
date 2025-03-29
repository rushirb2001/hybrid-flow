"""Tests for query engine functionality."""

import pytest

from hybridflow.retrieval.query import QueryEngine
from hybridflow.storage.qdrant_client import QdrantStorage
from hybridflow.storage.neo4j_client import Neo4jStorage


@pytest.fixture
def qdrant_storage():
    """Create Qdrant storage with alias resolution."""
    storage = QdrantStorage(host="localhost", port=6333)
    yield storage


@pytest.fixture
def neo4j_storage():
    """Create Neo4j storage."""
    storage = Neo4jStorage(
        uri="bolt://localhost:7687", user="neo4j", password="password"
    )
    yield storage
    storage.close()


@pytest.fixture
def query_engine(qdrant_storage, neo4j_storage):
    """Create query engine."""
    engine = QueryEngine(
        qdrant_storage=qdrant_storage,
        neo4j_storage=neo4j_storage,
        embedding_model="sentence-transformers/all-MiniLM-L6-v2",
    )
    yield engine
    engine.close()


def test_semantic_search(query_engine):
    """Test semantic search returns relevant results."""
    results = query_engine.semantic_search("shock pathophysiology", limit=3)

    assert isinstance(results, list)
    assert len(results) <= 3

    if results:
        result = results[0]
        assert "chunk_id" in result
        assert "score" in result
        assert "text" in result
        assert "textbook_id" in result
        assert 0 <= result["score"] <= 1


def test_get_context(query_engine):
    """Test retrieving context for a chunk."""
    search_results = query_engine.semantic_search("shock", limit=1)

    if not search_results:
        pytest.skip("No search results available for testing")

    chunk_id = search_results[0]["chunk_id"]
    context = query_engine.get_context(chunk_id)

    assert context is not None
    assert "text" in context
    assert "hierarchy" in context
    assert "chapter_id" in context
    assert len(context["hierarchy"]) > 0


def test_get_context_nonexistent(query_engine):
    """Test getting context for nonexistent chunk."""
    context = query_engine.get_context("nonexistent:chunk:id")
    assert context is None


def test_hybrid_search(query_engine):
    """Test hybrid search with context expansion."""
    results = query_engine.hybrid_search(
        "fluid resuscitation", limit=2, expand_context=True
    )

    assert isinstance(results, list)
    assert len(results) <= 2

    if results:
        result = results[0]
        assert "chunk_id" in result
        assert "score" in result
        assert "text" in result

        if "full_text" in result:
            assert "hierarchy" in result
            assert isinstance(result["hierarchy"], str)


def test_hybrid_search_no_expansion(query_engine):
    """Test hybrid search without context expansion."""
    results = query_engine.hybrid_search(
        "trauma management", limit=2, expand_context=False
    )

    assert isinstance(results, list)

    if results:
        result = results[0]
        assert "full_text" not in result


def test_get_chapter_structure(query_engine):
    """Test retrieving chapter structure."""
    structure = query_engine.get_chapter_structure("bailey:1")

    if structure is not None:
        assert "chapter_id" in structure
        assert "chapter_title" in structure
        assert "chapter_number" in structure
        assert "sections" in structure
        assert isinstance(structure["sections"], list)


def test_get_chapter_structure_nonexistent(query_engine):
    """Test getting structure for nonexistent chapter."""
    structure = query_engine.get_chapter_structure("nonexistent:999")
    assert structure is None


def test_semantic_search_with_threshold(query_engine):
    """Test semantic search with score threshold."""
    results = query_engine.semantic_search(
        "surgical technique", limit=5, score_threshold=0.7
    )

    assert isinstance(results, list)

    for result in results:
        assert result["score"] >= 0.7
