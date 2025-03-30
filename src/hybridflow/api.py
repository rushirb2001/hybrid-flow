"""Public API facade for external integration with agentic systems."""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union

from hybridflow.models import ExpansionConfig
from hybridflow.retrieval.query import QueryEngine
from hybridflow.storage.metadata_db import MetadataDatabase
from hybridflow.storage.neo4j_client import Neo4jStorage
from hybridflow.storage.qdrant_client import QdrantStorage

logger = logging.getLogger(__name__)


@dataclass
class HybridFlowConfig:
    """Configuration for HybridFlow API connections."""

    qdrant_host: str = field(default_factory=lambda: os.getenv("QDRANT_HOST", "localhost"))
    qdrant_port: int = field(default_factory=lambda: int(os.getenv("QDRANT_PORT", "6333")))
    qdrant_collection: str = field(
        default_factory=lambda: os.getenv("QDRANT_COLLECTION", "textbook_chunks")
    )
    neo4j_uri: str = field(
        default_factory=lambda: os.getenv("NEO4J_URI", "bolt://localhost:7687")
    )
    neo4j_user: str = field(default_factory=lambda: os.getenv("NEO4J_USER", "neo4j"))
    neo4j_password: str = field(
        default_factory=lambda: os.getenv("NEO4J_PASSWORD", "password")
    )
    metadata_db_path: str = field(
        default_factory=lambda: os.getenv("METADATA_DB_PATH", "metadata.db")
    )
    embedding_model: str = field(
        default_factory=lambda: os.getenv(
            "EMBEDDING_MODEL", "pritamdeka/S-PubMedBert-MS-MARCO-SCIFACT"
        )
    )
    vector_size: int = field(default_factory=lambda: int(os.getenv("VECTOR_SIZE", "768")))

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "HybridFlowConfig":
        """Create config from dictionary."""
        return cls(**{k: v for k, v in config.items() if hasattr(cls, k)})


class HybridFlowAPI:
    """Unified API facade for all HybridFlow operations.

    Provides a clean interface for vector search, graph traversal, metadata queries,
    and hybrid search with context expansion. Designed for integration with LangGraph,
    LangChain, and other agentic frameworks.

    Example:
        >>> api = HybridFlowAPI()
        >>> results = api.vector_search("hemorrhagic shock", limit=5)
        >>> stats = api.get_stats()
    """

    def __init__(
        self,
        config: Optional[Union[HybridFlowConfig, Dict[str, Any]]] = None,
        lazy_init: bool = False,
    ):
        """Initialize the HybridFlow API.

        Args:
            config: Configuration object or dictionary. Uses environment variables if None.
            lazy_init: If True, defer client initialization until first use.
        """
        if config is None:
            self.config = HybridFlowConfig()
        elif isinstance(config, dict):
            self.config = HybridFlowConfig.from_dict(config)
        else:
            self.config = config

        self._qdrant: Optional[QdrantStorage] = None
        self._neo4j: Optional[Neo4jStorage] = None
        self._metadata_db: Optional[MetadataDatabase] = None
        self._query_engine: Optional[QueryEngine] = None
        self._initialized = False

        if not lazy_init:
            self._initialize_clients()

    def _initialize_clients(self) -> None:
        """Initialize all storage clients."""
        if self._initialized:
            return

        logger.info("Initializing HybridFlow API clients...")

        self._qdrant = QdrantStorage(
            host=self.config.qdrant_host,
            port=self.config.qdrant_port,
            collection_name=self.config.qdrant_collection,
            vector_size=self.config.vector_size,
        )

        self._neo4j = Neo4jStorage(
            uri=self.config.neo4j_uri,
            user=self.config.neo4j_user,
            password=self.config.neo4j_password,
        )

        self._metadata_db = MetadataDatabase(database_path=self.config.metadata_db_path)

        self._query_engine = QueryEngine(
            qdrant_storage=self._qdrant,
            neo4j_storage=self._neo4j,
            embedding_model=self.config.embedding_model,
        )

        self._initialized = True
        logger.info("HybridFlow API initialized successfully")

    @property
    def qdrant(self) -> QdrantStorage:
        """Get Qdrant storage client."""
        if not self._initialized:
            self._initialize_clients()
        return self._qdrant

    @property
    def neo4j(self) -> Neo4jStorage:
        """Get Neo4j storage client."""
        if not self._initialized:
            self._initialize_clients()
        return self._neo4j

    @property
    def metadata_db(self) -> MetadataDatabase:
        """Get metadata database client."""
        if not self._initialized:
            self._initialize_clients()
        return self._metadata_db

    @property
    def engine(self) -> QueryEngine:
        """Get query engine."""
        if not self._initialized:
            self._initialize_clients()
        return self._query_engine

    # === Vector Search ===

    def vector_search(
        self,
        query: str,
        limit: int = 5,
        score_threshold: float = 0.5,
        textbook_filter: Optional[str] = None,
    ) -> List[Dict]:
        """Perform pure vector similarity search.

        Args:
            query: Natural language search query
            limit: Maximum number of results
            score_threshold: Minimum similarity score (0.0-1.0)
            textbook_filter: Filter by textbook (bailey, sabiston, schwartz)

        Returns:
            List of results with chunk_id, score, text, citation, and metadata
        """
        raw_results = self.engine.semantic_search(
            query_text=query,
            limit=limit,
            score_threshold=score_threshold,
        )

        results = []
        for r in raw_results:
            if textbook_filter and r.get("textbook_id") != textbook_filter:
                continue

            results.append({
                "chunk_id": r["chunk_id"],
                "score": r["score"],
                "text": r.get("text", ""),
                "textbook_id": r.get("textbook_id", ""),
                "chapter_number": str(r.get("chapter_number", "")),
                "page": r.get("page"),
                "citation": self.engine.format_citation(r),
            })

        return results

    def hybrid_search(
        self,
        query: str,
        limit: int = 5,
        expansion: Literal["none", "minimal", "standard", "comprehensive"] = "standard",
        custom_expansion: Optional[Dict[str, Any]] = None,
        textbook_filter: Optional[str] = None,
    ) -> List[Dict]:
        """Perform hybrid search with configurable context expansion.

        Args:
            query: Natural language search query
            limit: Maximum number of results
            expansion: Preset expansion level (none, minimal, standard, comprehensive)
            custom_expansion: Override preset with custom ExpansionConfig dict
            textbook_filter: Filter by textbook (bailey, sabiston, schwartz)

        Returns:
            List of results with text, score, hierarchy, and context based on expansion
        """
        if custom_expansion:
            config = ExpansionConfig(**custom_expansion)
        else:
            config_map = {
                "none": ExpansionConfig.none,
                "minimal": ExpansionConfig.minimal,
                "standard": ExpansionConfig.standard,
                "comprehensive": ExpansionConfig.comprehensive,
            }
            config = config_map.get(expansion, ExpansionConfig.standard)()

        raw_results = self.engine.hybrid_search(
            query_text=query,
            limit=limit,
            expansion_config=config,
        )

        results = []
        for r in raw_results:
            if textbook_filter and r.get("textbook_id") != textbook_filter:
                continue

            result = {
                "chunk_id": r["chunk_id"],
                "score": r["score"],
                "text": r.get("full_text", r.get("text", "")),
                "textbook_id": r.get("textbook_id", ""),
                "chapter_number": str(r.get("chapter_number", "")),
                "page": r.get("page"),
                "citation": self.engine.format_citation(r),
                "hierarchy": r.get("hierarchy"),
            }

            if r.get("hierarchy_details"):
                result["hierarchy_details"] = r["hierarchy_details"]
            if r.get("expanded_context"):
                result["expanded_context"] = r["expanded_context"]
            if r.get("section_context"):
                result["section_context"] = r["section_context"]
            if r.get("referenced_content"):
                result["referenced_content"] = r["referenced_content"]

            results.append(result)

        return results

    # === Graph Operations ===

    def get_context(self, chunk_id: str) -> Optional[Dict]:
        """Retrieve hierarchical context for a specific paragraph.

        Args:
            chunk_id: Paragraph identifier (e.g., 'bailey:ch60:2.1.1')

        Returns:
            Dictionary with hierarchy path and details, or None if not found
        """
        return self.engine.get_context(chunk_id)

    def get_surrounding(
        self,
        chunk_id: str,
        before: int = 2,
        after: int = 2,
    ) -> Optional[Dict]:
        """Get paragraphs before and after a specific paragraph.

        Args:
            chunk_id: Target paragraph identifier
            before: Number of preceding paragraphs (0-5)
            after: Number of following paragraphs (0-5)

        Returns:
            Dictionary with current, before, and after paragraphs
        """
        return self.engine.get_surrounding_paragraphs(
            chunk_id=chunk_id,
            before_count=before,
            after_count=after,
        )

    def get_references(self, chunk_id: str) -> Dict:
        """Get figures and tables referenced by a paragraph.

        Args:
            chunk_id: Paragraph identifier

        Returns:
            Dictionary with references list and counts
        """
        return self.engine.get_referenced_content(chunk_id) or {
            "chunk_id": chunk_id,
            "references": [],
            "counts": {"figures": 0, "tables": 0, "total": 0},
        }

    def get_chapter_structure(self, chapter_id: str) -> Optional[Dict]:
        """Get complete chapter structure with sections.

        Args:
            chapter_id: Chapter identifier (e.g., 'bailey:ch60')

        Returns:
            Dictionary with chapter title, number, and sections list
        """
        return self.engine.get_chapter_structure(chapter_id)

    def get_siblings(self, chunk_id: str, same_level: bool = True) -> Optional[Dict]:
        """Get all sibling paragraphs at the same hierarchy level.

        Args:
            chunk_id: Target paragraph identifier
            same_level: If True, siblings from immediate parent only

        Returns:
            Dictionary with siblings list and parent info
        """
        return self.engine.get_sibling_paragraphs(
            chunk_id=chunk_id,
            same_level_only=same_level,
        )

    # === Metadata Operations ===

    def get_chapter_metadata(
        self,
        textbook_id: str,
        chapter_number: str,
    ) -> Optional[Dict]:
        """Get metadata for a specific chapter.

        Args:
            textbook_id: Textbook identifier (bailey, sabiston, schwartz)
            chapter_number: Chapter number as string

        Returns:
            Dictionary with version, hash, and ingestion info
        """
        result = self.metadata_db.get_chapter_by_id(textbook_id, chapter_number)
        if not result:
            return None

        return {
            "textbook_id": result.textbook_id,
            "chapter_number": result.chapter_number,
            "title": result.title,
            "version": result.version,
            "content_hash": result.content_hash,
            "chunk_count": result.chunk_count,
            "quality_score": result.quality_score,
            "ingestion_timestamp": str(result.ingestion_timestamp)
            if result.ingestion_timestamp
            else None,
        }

    def get_aggregate_stats(self) -> Dict:
        """Get aggregate statistics from metadata database.

        Returns:
            Dictionary with total chapters, distribution by textbook, etc.
        """
        return self.metadata_db.get_aggregate_stats()

    # === System Stats ===

    def get_stats(self) -> Dict:
        """Get comprehensive system statistics.

        Returns:
            Dictionary with stats from Qdrant, Neo4j, and SQLite
        """
        stats = {
            "vector": {},
            "graph": {},
            "metadata": {},
            "health": {},
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Qdrant stats
        try:
            qdrant_info = self.qdrant.get_collection_info()
            qdrant_stats = self.qdrant.get_collection_stats()
            stats["vector"] = {
                "point_count": qdrant_info.get("points_count", 0),
                "collection_name": qdrant_info.get("collection_name"),
                "vector_size": qdrant_info.get("vector_size"),
                "avg_text_length": qdrant_stats.get("avg_text_length", 0),
                "textbook_distribution": qdrant_stats.get("textbook_distribution", {}),
            }
            stats["health"]["qdrant"] = True
        except Exception as e:
            logger.error(f"Failed to get Qdrant stats: {e}")
            stats["vector"] = {"error": str(e)}
            stats["health"]["qdrant"] = False

        # Neo4j stats
        try:
            stats["graph"] = self._get_neo4j_stats()
            stats["health"]["neo4j"] = True
        except Exception as e:
            logger.error(f"Failed to get Neo4j stats: {e}")
            stats["graph"] = {"error": str(e)}
            stats["health"]["neo4j"] = False

        # Metadata stats
        try:
            stats["metadata"] = self.metadata_db.get_aggregate_stats()
            stats["health"]["sqlite"] = True
        except Exception as e:
            logger.error(f"Failed to get metadata stats: {e}")
            stats["metadata"] = {"error": str(e)}
            stats["health"]["sqlite"] = False

        stats["health"]["all_healthy"] = all(
            v for k, v in stats["health"].items() if k != "all_healthy"
        )

        return stats

    def _get_neo4j_stats(self) -> Dict:
        """Get Neo4j node and relationship counts."""
        stats_query = """
        MATCH (n)
        WITH labels(n) as lbls, count(n) as cnt
        UNWIND lbls as label
        WITH label, sum(cnt) as count
        RETURN label, count
        ORDER BY count DESC
        """

        rel_query = """
        MATCH ()-[r]->()
        WITH type(r) as rel_type, count(r) as count
        RETURN rel_type, count
        ORDER BY count DESC
        """

        with self.neo4j.driver.session() as session:
            node_result = session.run(stats_query)
            node_counts = {record["label"]: record["count"] for record in node_result}

            rel_result = session.run(rel_query)
            rel_counts = {record["rel_type"]: record["count"] for record in rel_result}

        return {
            "nodes": node_counts,
            "relationships": rel_counts,
            "total_nodes": sum(node_counts.values()),
            "total_relationships": sum(rel_counts.values()),
        }

    def health_check(self) -> Dict[str, bool]:
        """Quick health check of all connections.

        Returns:
            Dictionary with boolean status for each storage system
        """
        health = {}

        try:
            self.qdrant.get_collection_info()
            health["qdrant"] = True
        except Exception:
            health["qdrant"] = False

        try:
            with self.neo4j.driver.session() as session:
                session.run("RETURN 1")
            health["neo4j"] = True
        except Exception:
            health["neo4j"] = False

        try:
            self.metadata_db.get_aggregate_stats()
            health["sqlite"] = True
        except Exception:
            health["sqlite"] = False

        health["all_healthy"] = all(v for k, v in health.items() if k != "all_healthy")
        return health

    # === Utility Methods ===

    def format_citation(self, result: Dict) -> str:
        """Format a search result as a citation string.

        Args:
            result: Search result dictionary

        Returns:
            Formatted citation (e.g., "Bailey & Love, Ch 60, Section 2.4, p. 1025")
        """
        return self.engine.format_citation(result)

    def get_expansion_presets(self) -> Dict[str, Dict]:
        """Get available expansion configuration presets.

        Returns:
            Dictionary mapping preset names to their configurations
        """
        return {
            "none": ExpansionConfig.none().model_dump(),
            "minimal": ExpansionConfig.minimal().model_dump(),
            "standard": ExpansionConfig.standard().model_dump(),
            "comprehensive": ExpansionConfig.comprehensive().model_dump(),
        }

    def close(self) -> None:
        """Close all connections and cleanup resources."""
        if self._query_engine:
            self._query_engine.close()
        if self._neo4j:
            self._neo4j.close()
        self._initialized = False
        logger.info("HybridFlow API connections closed")

    def __enter__(self) -> "HybridFlowAPI":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    # === Tool Integration ===

    def as_tool_definitions(self) -> List[Dict]:
        """Generate LangChain-compatible tool definitions.

        Returns:
            List of tool definition dictionaries for LLM binding
        """
        return [
            {
                "name": "vector_search",
                "description": "Fast semantic search for conceptual queries",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query"},
                        "limit": {"type": "integer", "default": 5},
                        "textbook_filter": {
                            "type": "string",
                            "enum": ["bailey", "sabiston", "schwartz"],
                        },
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "hybrid_search",
                "description": "Comprehensive search with graph context expansion",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query"},
                        "limit": {"type": "integer", "default": 5},
                        "expansion": {
                            "type": "string",
                            "enum": ["none", "minimal", "standard", "comprehensive"],
                            "default": "standard",
                        },
                        "textbook_filter": {
                            "type": "string",
                            "enum": ["bailey", "sabiston", "schwartz"],
                        },
                    },
                    "required": ["query"],
                },
            },
            {
                "name": "get_context",
                "description": "Get hierarchical context for a paragraph by chunk_id",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "chunk_id": {"type": "string", "description": "Paragraph ID"},
                    },
                    "required": ["chunk_id"],
                },
            },
            {
                "name": "get_references",
                "description": "Get figures and tables referenced by a paragraph",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "chunk_id": {"type": "string", "description": "Paragraph ID"},
                    },
                    "required": ["chunk_id"],
                },
            },
            {
                "name": "get_chapter_structure",
                "description": "Get the section structure for a chapter",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "chapter_id": {"type": "string", "description": "Chapter ID"},
                    },
                    "required": ["chapter_id"],
                },
            },
        ]

    def invoke_tool(self, tool_name: str, **kwargs) -> Any:
        """Invoke a tool by name with arguments.

        Args:
            tool_name: Name of the tool to invoke
            **kwargs: Tool arguments

        Returns:
            Tool result

        Raises:
            ValueError: If tool name is not recognized
        """
        tool_map = {
            "vector_search": self.vector_search,
            "hybrid_search": self.hybrid_search,
            "get_context": self.get_context,
            "get_references": self.get_references,
            "get_surrounding": self.get_surrounding,
            "get_siblings": self.get_siblings,
            "get_chapter_structure": self.get_chapter_structure,
            "get_chapter_metadata": self.get_chapter_metadata,
            "get_stats": self.get_stats,
            "health_check": self.health_check,
        }

        if tool_name not in tool_map:
            raise ValueError(f"Unknown tool: {tool_name}. Available: {list(tool_map.keys())}")

        return tool_map[tool_name](**kwargs)
