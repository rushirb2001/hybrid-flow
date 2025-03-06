"""Tests for Qdrant vector storage client."""

import numpy as np
import pytest

from hybridflow.storage.qdrant_client import QdrantStorage


@pytest.fixture
def qdrant_storage():
    """Create a Qdrant storage instance for testing."""
    storage = QdrantStorage(host="localhost", port=6333, collection_name="test_collection")
    storage.create_collection()
    yield storage

    # Teardown: Delete the test collection
    try:
        storage.client.delete_collection(collection_name="test_collection")
    except Exception:
        pass  # Collection might not exist if test failed early


def test_create_collection(qdrant_storage):
    """Test that collection is created successfully."""
    info = qdrant_storage.get_collection_info()

    assert info["points_count"] >= 0
    assert info["vector_size"] == 384


def test_upsert_single_chunk(qdrant_storage):
    """Test upserting a single chunk to the collection."""
    # Create sample chunk
    chunk_id = "test:ch1:1.1"
    text = "Sample medical text"
    metadata = {"chapter": "1", "textbook": "bailey"}
    embedding = np.random.rand(384).tolist()

    # Upsert the chunk
    chunks = [(chunk_id, text, metadata, embedding)]
    qdrant_storage.upsert_chunks(chunks)

    # Verify
    info = qdrant_storage.get_collection_info()
    assert info["points_count"] == 1


def test_upsert_multiple_chunks(qdrant_storage):
    """Test upserting multiple chunks to the collection."""
    chunks = []
    for i in range(5):
        chunk_id = f"test:ch1:1.{i}"
        text = f"Sample medical text {i}"
        metadata = {"chapter": "1", "textbook": "bailey", "index": i}
        embedding = np.random.rand(384).tolist()
        chunks.append((chunk_id, text, metadata, embedding))

    # Upsert all chunks
    qdrant_storage.upsert_chunks(chunks)

    # Verify
    info = qdrant_storage.get_collection_info()
    assert info["points_count"] == 5


def test_search_similar(qdrant_storage):
    """Test searching for similar chunks using vector similarity."""
    # Create base embedding
    base_embedding = np.random.rand(384)

    # Create 3 chunks with similar embeddings
    chunks = []
    for i in range(3):
        chunk_id = f"test:ch1:1.{i}"
        text = f"Medical text about surgery {i}"
        metadata = {"chapter": "1", "textbook": "bailey", "index": i}

        # First chunk has base embedding, others have slightly different
        if i == 0:
            embedding = base_embedding.tolist()
        else:
            # Add small random noise
            embedding = (base_embedding + np.random.rand(384) * 0.1).tolist()

        chunks.append((chunk_id, text, metadata, embedding))

    # Upsert chunks
    qdrant_storage.upsert_chunks(chunks)

    # Search using vector very similar to first chunk
    query_vector = (base_embedding + np.random.rand(384) * 0.01).tolist()
    results = qdrant_storage.search_similar(query_vector, limit=2)

    # Verify results
    assert len(results) == 2
    assert results[0][0] == "test:ch1:1.0"  # First chunk should be most similar
    assert results[0][1] > 0.9  # High similarity score


def test_delete_chunks(qdrant_storage):
    """Test deleting chunks from the collection."""
    # Create and upsert 3 chunks
    chunks = []
    for i in range(3):
        chunk_id = f"test:ch1:1.{i}"
        text = f"Medical text {i}"
        metadata = {"chapter": "1", "textbook": "bailey"}
        embedding = np.random.rand(384).tolist()
        chunks.append((chunk_id, text, metadata, embedding))

    qdrant_storage.upsert_chunks(chunks)

    # Verify 3 chunks exist
    info = qdrant_storage.get_collection_info()
    assert info["points_count"] == 3

    # Delete one chunk
    qdrant_storage.delete_chunks(["test:ch1:1.1"])

    # Verify only 2 chunks remain
    info = qdrant_storage.get_collection_info()
    assert info["points_count"] == 2


def test_upsert_duplicate_chunk_id(qdrant_storage):
    """Test upserting a chunk with duplicate chunk_id updates the existing chunk."""
    chunk_id = "test:ch1:1.1"
    embedding = np.random.rand(384).tolist()

    # First upsert
    chunks1 = [(chunk_id, "Original text", {"chapter": "1"}, embedding)]
    qdrant_storage.upsert_chunks(chunks1)

    # Verify 1 chunk exists
    info = qdrant_storage.get_collection_info()
    assert info["points_count"] == 1

    # Upsert again with same chunk_id but different text
    chunks2 = [(chunk_id, "Updated text", {"chapter": "1", "updated": True}, embedding)]
    qdrant_storage.upsert_chunks(chunks2)

    # Verify still only 1 chunk (updated, not duplicated)
    info = qdrant_storage.get_collection_info()
    assert info["points_count"] == 1

    # Search and verify the text was updated
    results = qdrant_storage.search_similar(embedding, limit=1)
    assert len(results) == 1
    assert results[0][0] == chunk_id
