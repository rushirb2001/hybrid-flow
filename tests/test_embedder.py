"""Tests for embedding generator using sentence transformers."""

import numpy as np
import pytest

from hybridflow.parsing.embedder import EmbeddingGenerator


@pytest.fixture
def embedder():
    """Create an EmbeddingGenerator with default model."""
    return EmbeddingGenerator()


def test_generate_embedding(embedder):
    """Test generating embedding for single text."""
    text = "This is a test medical paragraph"
    embedding = embedder.generate_embedding(text)

    # Should return 384-dimensional vector
    assert len(embedding) == 384

    # All values should be floats
    assert all(isinstance(val, float) for val in embedding)


def test_generate_batch_embeddings(embedder):
    """Test generating embeddings for multiple texts in batch."""
    texts = [
        "First medical paragraph about shock",
        "Second paragraph about blood transfusion",
        "Third paragraph about fluid management",
        "Fourth paragraph about electrolyte balance",
        "Fifth paragraph about wound healing",
    ]

    embeddings = embedder.generate_batch_embeddings(texts)

    # Should return 5 embeddings
    assert len(embeddings) == 5

    # Each embedding should be 384-dimensional
    for embedding in embeddings:
        assert len(embedding) == 384
        assert all(isinstance(val, float) for val in embedding)


def test_embedding_consistency(embedder):
    """Test that same text produces identical embeddings."""
    text = "Medical paragraph about surgical techniques"

    embedding1 = embedder.generate_embedding(text)
    embedding2 = embedder.generate_embedding(text)

    # Should produce identical embeddings
    assert np.allclose(embedding1, embedding2)


def test_different_texts_different_embeddings(embedder):
    """Test that different texts produce different embeddings."""
    text1 = "Blood transfusion protocols in emergency surgery"
    text2 = "Postoperative wound care management strategies"

    embedding1 = embedder.generate_embedding(text1)
    embedding2 = embedder.generate_embedding(text2)

    # Should produce different embeddings
    assert not np.allclose(embedding1, embedding2)
