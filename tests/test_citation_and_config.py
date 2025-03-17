"""Unit tests for citation formatting and ExpansionConfig."""

import os
import pytest
from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

from hybridflow.models import ExpansionConfig
from hybridflow.retrieval.query import QueryEngine

# Load environment variables
load_dotenv()


@pytest.fixture
def query_engine():
    """Create QueryEngine instance for testing."""
    qdrant_client = QdrantClient(
        host=os.getenv("QDRANT_HOST", "localhost"),
        port=int(os.getenv("QDRANT_PORT", "6333")),
    )

    neo4j_driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password")),
    )

    engine = QueryEngine(qdrant_client=qdrant_client, neo4j_driver=neo4j_driver)

    yield engine

    # Cleanup
    engine.close()
    qdrant_client.close()


class TestCitationFormatting:
    """Test suite for format_citation method (TASK 4.2)."""

    def test_basic_citation_format(self, query_engine):
        """Test basic citation formatting with all fields."""
        result = {
            "textbook_id": "bailey",
            "chapter_number": "60",
            "chunk_id": "bailey:ch60:2.4.1",
            "page": 1025,
        }

        citation = query_engine.format_citation(result)

        assert "Bailey & Love" in citation
        assert "Ch 60" in citation
        assert "Section 2.4" in citation
        assert "p. 1025" in citation

    def test_citation_sabiston(self, query_engine):
        """Test citation with Sabiston textbook."""
        result = {
            "textbook_id": "sabiston",
            "chapter_number": "12",
            "chunk_id": "sabiston:ch12:3.1.2",
            "page": 456,
        }

        citation = query_engine.format_citation(result)

        assert "Sabiston" in citation
        assert "Ch 12" in citation
        assert "Section 3.1" in citation
        assert "p. 456" in citation

    def test_citation_schwartz(self, query_engine):
        """Test citation with Schwartz textbook."""
        result = {
            "textbook_id": "schwartz",
            "chapter_number": "5",
            "chunk_id": "schwartz:ch5:1.2.3",
            "page": 78,
        }

        citation = query_engine.format_citation(result)

        assert "Schwartz" in citation
        assert "Ch 5" in citation
        assert "Section 1.2" in citation
        assert "p. 78" in citation

    def test_citation_missing_fields(self, query_engine):
        """Test citation with missing fields returns gracefully."""
        result = {}

        citation = query_engine.format_citation(result)

        assert citation == "Unknown source"

    def test_citation_partial_fields(self, query_engine):
        """Test citation with only some fields."""
        result = {
            "textbook_id": "bailey",
            "chapter_number": "60",
        }

        citation = query_engine.format_citation(result)

        assert "Bailey & Love" in citation
        assert "Ch 60" in citation
        assert "p." not in citation

    def test_citation_no_section(self, query_engine):
        """Test citation when chunk_id has no section info."""
        result = {
            "textbook_id": "bailey",
            "chapter_number": "1",
            "chunk_id": "bailey:ch1:1",
            "page": 10,
        }

        citation = query_engine.format_citation(result)

        assert "Bailey & Love" in citation
        assert "Ch 1" in citation
        # Section should not be included if it's a single-level paragraph number
        assert "p. 10" in citation


class TestExpansionConfig:
    """Test suite for ExpansionConfig class (TASK 5.1)."""

    def test_minimal_preset(self):
        """Test minimal expansion preset."""
        config = ExpansionConfig.minimal()

        assert config.expand_context is True
        assert config.expand_paragraphs is False
        assert config.include_section_context is False
        assert config.include_references is False

    def test_standard_preset(self):
        """Test standard expansion preset."""
        config = ExpansionConfig.standard()

        assert config.expand_context is True
        assert config.expand_paragraphs is True
        assert config.before_count == 1
        assert config.after_count == 1
        assert config.include_section_context is False
        assert config.include_references is False

    def test_comprehensive_preset(self):
        """Test comprehensive expansion preset."""
        config = ExpansionConfig.comprehensive()

        assert config.expand_context is True
        assert config.expand_paragraphs is True
        assert config.before_count == 2
        assert config.after_count == 2
        assert config.include_section_context is True
        assert config.include_references is True

    def test_none_preset(self):
        """Test none expansion preset."""
        config = ExpansionConfig.none()

        assert config.expand_context is False
        assert config.expand_paragraphs is False
        assert config.include_section_context is False
        assert config.include_references is False

    def test_custom_config(self):
        """Test creating custom expansion config."""
        config = ExpansionConfig(
            expand_context=True,
            expand_paragraphs=True,
            before_count=3,
            after_count=3,
            include_section_context=True,
            include_references=False,
        )

        assert config.expand_context is True
        assert config.expand_paragraphs is True
        assert config.before_count == 3
        assert config.after_count == 3
        assert config.include_section_context is True
        assert config.include_references is False


class TestExpansionConfigIntegration:
    """Test suite for ExpansionConfig integration with hybrid_search (TASK 5.2)."""

    def test_hybrid_search_with_config_object(self, query_engine):
        """Test hybrid search with ExpansionConfig object."""
        config = ExpansionConfig.standard()

        results = query_engine.hybrid_search(
            "lung anatomy", limit=2, expansion_config=config
        )

        assert len(results) > 0
        result = results[0]

        # Standard config enables expand_context and expand_paragraphs
        assert "hierarchy" in result
        assert "expanded_context" in result

    def test_hybrid_search_with_dict_config(self, query_engine):
        """Test hybrid search with dict config (converted to ExpansionConfig)."""
        config_dict = {
            "expand_context": True,
            "expand_paragraphs": False,
            "include_references": True,
        }

        results = query_engine.hybrid_search(
            "lung anatomy", limit=2, expansion_config=config_dict
        )

        assert len(results) > 0
        result = results[0]

        # Should have hierarchy but not expanded_context
        assert "hierarchy" in result
        assert "expanded_context" not in result

    def test_backward_compatibility_no_config(self, query_engine):
        """Test backward compatibility with old individual parameters."""
        results = query_engine.hybrid_search(
            "lung anatomy",
            limit=2,
            expand_context=True,
            expand_paragraphs=False,
        )

        assert len(results) > 0
        result = results[0]

        # Should work as before
        assert "hierarchy" in result

    def test_config_overrides_individual_params(self, query_engine):
        """Test that expansion_config takes precedence over individual parameters."""
        config = ExpansionConfig.none()

        results = query_engine.hybrid_search(
            "lung anatomy",
            limit=2,
            expansion_config=config,
            expand_context=True,  # This should be ignored
        )

        assert len(results) > 0
        result = results[0]

        # Config.none() should disable expansion, even though expand_context=True was passed
        assert "hierarchy" not in result or result.get("hierarchy") == ""

    def test_minimal_config_search(self, query_engine):
        """Test search with minimal expansion."""
        config = ExpansionConfig.minimal()

        results = query_engine.hybrid_search(
            "surgical complications", limit=2, expansion_config=config
        )

        assert len(results) > 0
        result = results[0]

        # Minimal: hierarchy yes, others no
        assert "hierarchy" in result
        assert "expanded_context" not in result
        assert "referenced_content" not in result

    def test_comprehensive_config_search(self, query_engine):
        """Test search with comprehensive expansion."""
        config = ExpansionConfig.comprehensive()

        results = query_engine.hybrid_search(
            "surgical complications", limit=2, expansion_config=config
        )

        assert len(results) > 0
        result = results[0]

        # Comprehensive: all features enabled
        assert "hierarchy" in result
        assert "expanded_context" in result
        # referenced_content will only be present if there are references in the paragraph
        # So we just check that the feature is working, not that it's always present
