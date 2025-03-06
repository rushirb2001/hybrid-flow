"""Qdrant vector database client for semantic search."""

import uuid
from typing import Dict, List, Tuple

import qdrant_client
import qdrant_client.models as qmodels


class QdrantStorage:
    """Manages vector storage and semantic search using Qdrant."""

    def __init__(self, host: str, port: int, collection_name: str):
        """Initialize the Qdrant client.

        Args:
            host: Qdrant server host
            port: Qdrant server port
            collection_name: Name of the vector collection
        """
        self.client = qdrant_client.QdrantClient(host=host, port=port)
        self.collection_name = collection_name

    def create_collection(self) -> None:
        """Create the collection if it doesn't exist.

        Uses 384-dimensional vectors (sentence-transformers/all-MiniLM-L6-v2 default)
        with cosine distance metric for semantic similarity.
        """
        if not self.client.collection_exists(self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=qmodels.VectorParams(
                    size=384, distance=qmodels.Distance.COSINE
                ),
            )

    def upsert_chunks(
        self, chunks: List[Tuple[str, str, dict, List[float]]]
    ) -> None:
        """Insert or update chunks in the vector database.

        Args:
            chunks: List of tuples containing:
                - chunk_id: Unique identifier for the chunk
                - text: Text content of the chunk
                - metadata: Additional metadata (e.g., chapter_id, page, etc.)
                - embedding: 384-dimensional vector embedding
        """
        points = []
        for chunk_id, text, metadata, embedding in chunks:
            # Generate deterministic UUID from chunk_id
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_id))

            point = qmodels.PointStruct(
                id=point_id,
                vector=embedding,
                payload={"chunk_id": chunk_id, "text": text, **metadata},
            )
            points.append(point)

        self.client.upsert(collection_name=self.collection_name, points=points)

    def search_similar(
        self, query_vector: List[float], limit: int = 10
    ) -> List[Tuple[str, float]]:
        """Search for similar chunks using vector similarity.

        Args:
            query_vector: Query embedding vector
            limit: Maximum number of results to return

        Returns:
            List of tuples containing (chunk_id, similarity_score)
        """
        results = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=limit,
        )

        return [(hit.payload["chunk_id"], hit.score) for hit in results.points]

    def delete_chunks(self, chunk_ids: List[str]) -> None:
        """Delete chunks from the vector database.

        Args:
            chunk_ids: List of chunk IDs to delete
        """
        # Convert chunk_ids to point_ids using deterministic UUIDs
        point_ids = [str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_id)) for chunk_id in chunk_ids]

        self.client.delete(
            collection_name=self.collection_name,
            points_selector=qmodels.PointIdsList(points=point_ids),
        )

    def get_collection_info(self) -> Dict:
        """Get collection statistics and information.

        Returns:
            Dictionary containing:
                - points_count: Number of vectors in collection
                - vector_size: Dimensionality of vectors
        """
        collection_info = self.client.get_collection(
            collection_name=self.collection_name
        )

        return {
            "points_count": collection_info.points_count,
            "vector_size": collection_info.config.params.vectors.size,
        }
