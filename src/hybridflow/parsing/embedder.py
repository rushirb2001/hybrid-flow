"""Embedding generation for text chunks using sentence transformers."""

from typing import List

import numpy as np
from sentence_transformers import SentenceTransformer


class EmbeddingGenerator:
    """Generates vector embeddings for text chunks using sentence transformers."""

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2") -> None:
        """Initialize the embedding generator.

        Args:
            model_name: Name of the sentence transformer model to use
        """
        self.model = SentenceTransformer(model_name)
        self.model_name = model_name
        self.vector_size = 384

    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for a single text.

        Args:
            text: Input text to encode

        Returns:
            Embedding vector as list of floats
        """
        embedding = self.model.encode(text, convert_to_numpy=True)
        return embedding.tolist()

    def generate_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts in batch.

        Args:
            texts: List of input texts to encode

        Returns:
            List of embedding vectors as lists of floats
        """
        embeddings = self.model.encode(texts, convert_to_numpy=True, batch_size=32)
        return embeddings.tolist()
