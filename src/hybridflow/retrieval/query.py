"""Query engine for semantic search and context retrieval."""

import logging
from typing import Dict, List, Optional

from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class QueryEngine:
    """Performs semantic search and context retrieval across hybrid storage."""

    def __init__(
        self,
        qdrant_client: QdrantClient,
        neo4j_driver,
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
        collection_name: str = "textbook_chunks",
    ):
        """Initialize query engine.

        Args:
            qdrant_client: Qdrant client for vector search
            neo4j_driver: Neo4j driver for graph traversal
            embedding_model: Embedding model name
            collection_name: Qdrant collection name
        """
        self.qdrant = qdrant_client
        self.neo4j = neo4j_driver
        self.collection_name = collection_name
        self.encoder = SentenceTransformer(embedding_model)

    def semantic_search(
        self, query_text: str, limit: int = 5, score_threshold: float = 0.5
    ) -> List[Dict]:
        """Perform semantic search using vector similarity.

        Args:
            query_text: Query string
            limit: Maximum number of results
            score_threshold: Minimum similarity score

        Returns:
            List of results with chunk_id, score, and metadata
        """
        query_vector = self.encoder.encode(query_text).tolist()

        search_results = self.qdrant.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=limit,
            score_threshold=score_threshold,
        ).points

        results = []
        for hit in search_results:
            results.append(
                {
                    "chunk_id": hit.payload.get("chunk_id"),
                    "score": hit.score,
                    "text": hit.payload.get("text", ""),
                    "textbook_id": hit.payload.get("textbook_id"),
                    "chapter_number": hit.payload.get("chapter_number"),
                    "page": hit.payload.get("page"),
                }
            )

        logger.info(f"Semantic search returned {len(results)} results for: {query_text}")
        return results

    def get_context(self, chunk_id: str) -> Optional[Dict]:
        """Retrieve full context for a chunk including hierarchy.

        Args:
            chunk_id: Chunk identifier

        Returns:
            Dictionary with paragraph text and hierarchy path
        """
        query = """
        MATCH (p:Paragraph {chunk_id: $chunk_id})
        OPTIONAL MATCH path = (c:Chapter)-[:HAS_SECTION]->(s:Section)
                      -[:HAS_SUBSECTION*0..1]->(ss)
                      -[:HAS_SUBSUBSECTION*0..1]->(sss)
                      -[:HAS_PARAGRAPH]->(p)
        RETURN p.text as text,
               p.page as page,
               c.id as chapter_id,
               c.title as chapter_title,
               s.title as section_title,
               ss.title as subsection_title,
               sss.title as subsubsection_title
        """

        with self.neo4j.session() as session:
            result = session.run(query, chunk_id=chunk_id)
            record = result.single()

            if not record:
                logger.warning(f"No context found for chunk_id: {chunk_id}")
                return None

            hierarchy_parts = []
            if record["chapter_title"]:
                hierarchy_parts.append(record["chapter_title"])
            if record["section_title"]:
                hierarchy_parts.append(record["section_title"])
            if record["subsection_title"]:
                hierarchy_parts.append(record["subsection_title"])
            if record["subsubsection_title"]:
                hierarchy_parts.append(record["subsubsection_title"])

            return {
                "text": record["text"],
                "page": record["page"],
                "chapter_id": record["chapter_id"],
                "hierarchy": " > ".join(hierarchy_parts),
            }

    def hybrid_search(
        self, query_text: str, limit: int = 5, expand_context: bool = True
    ) -> List[Dict]:
        """Perform hybrid search combining vector search with graph traversal.

        Args:
            query_text: Query string
            limit: Maximum number of results
            expand_context: Whether to expand results with full context

        Returns:
            List of results with text, score, and full hierarchical context
        """
        search_results = self.semantic_search(query_text, limit=limit)

        if expand_context:
            for result in search_results:
                chunk_id = result["chunk_id"]
                context = self.get_context(chunk_id)
                if context:
                    result["full_text"] = context["text"]
                    result["hierarchy"] = context["hierarchy"]
                    result["chapter_id"] = context["chapter_id"]

        return search_results

    def get_chapter_structure(self, chapter_id: str) -> Optional[Dict]:
        """Retrieve complete chapter structure from Neo4j.

        Args:
            chapter_id: Chapter identifier

        Returns:
            Dictionary with complete chapter hierarchy
        """
        query = """
        MATCH (c:Chapter {id: $chapter_id})
        OPTIONAL MATCH (c)-[:HAS_SECTION]->(s:Section)
        RETURN c.title as chapter_title,
               c.number as chapter_number,
               collect(DISTINCT {
                   title: s.title,
                   number: s.number
               }) as sections
        """

        with self.neo4j.session() as session:
            result = session.run(query, chapter_id=chapter_id)
            record = result.single()

            if not record:
                logger.warning(f"No chapter found with id: {chapter_id}")
                return None

            return {
                "chapter_id": chapter_id,
                "chapter_title": record["chapter_title"],
                "chapter_number": record["chapter_number"],
                "sections": record["sections"],
            }

    def close(self):
        """Close connections."""
        if self.neo4j:
            self.neo4j.close()
