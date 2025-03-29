"""Unit tests for QueryEngine paragraph expansion functionality."""

import os
import pytest
from dotenv import load_dotenv

from hybridflow.retrieval.query import QueryEngine
from hybridflow.storage.qdrant_client import QdrantStorage
from hybridflow.storage.neo4j_client import Neo4jStorage

# Load environment variables
load_dotenv()


@pytest.fixture
def query_engine():
    """Create QueryEngine instance for testing."""
    qdrant_storage = QdrantStorage(
        host=os.getenv("QDRANT_HOST", "localhost"),
        port=int(os.getenv("QDRANT_PORT", "6333")),
    )

    neo4j_storage = Neo4jStorage(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "password"),
    )

    engine = QueryEngine(
        qdrant_storage=qdrant_storage,
        neo4j_storage=neo4j_storage,
    )

    yield engine

    # Cleanup
    engine.close()
    neo4j_storage.close()
    qdrant_storage.client.close()


class TestGetSurroundingParagraphs:
    """Test suite for get_surrounding_paragraphs method."""

    def test_basic_expansion(self, query_engine):
        """Test basic paragraph expansion with before and after paragraphs."""
        # Use a paragraph that has paragraphs before and after
        result = query_engine.get_surrounding_paragraphs(
            "bailey:ch01:1.1.2", before_count=1, after_count=1
        )

        assert result is not None
        assert "current" in result
        assert "before" in result
        assert "after" in result
        assert "metadata" in result

        # Verify current paragraph (supports versioned IDs)
        assert result["current"]["chunk_id"].startswith("bailey:ch01:1.1.2")
        assert result["current"]["position"] == "current"
        assert "number" in result["current"]
        assert "text" in result["current"]
        assert "page" in result["current"]

        # Verify metadata
        assert result["metadata"]["requested_before"] == 1
        assert result["metadata"]["requested_after"] == 1

    def test_edge_case_first_paragraph(self, query_engine):
        """Test retrieval of first paragraph in a section (no before paragraphs)."""
        result = query_engine.get_surrounding_paragraphs(
            "bailey:ch01:1.1", before_count=2, after_count=2
        )

        assert result is not None
        # First paragraph should have 0 before paragraphs
        assert result["metadata"]["returned_before"] == 0
        # Should have some after paragraphs
        assert result["metadata"]["returned_after"] > 0

    def test_edge_case_last_paragraph(self, query_engine):
        """Test retrieval of last paragraph in a section (no after paragraphs)."""
        # bailey:ch60:2.4.4.2 is the last paragraph in its section
        result = query_engine.get_surrounding_paragraphs(
            "bailey:ch60:2.4.4.2", before_count=2, after_count=2
        )

        assert result is not None
        # Last paragraph should have 0 after paragraphs
        assert result["metadata"]["returned_after"] == 0
        # Should have some before paragraphs
        assert result["metadata"]["returned_before"] > 0

    def test_position_marking(self, query_engine):
        """Test that paragraphs are correctly marked with position."""
        result = query_engine.get_surrounding_paragraphs(
            "bailey:ch01:1.1.2", before_count=2, after_count=2
        )

        assert result is not None

        # Check before paragraphs have correct position
        for para in result["before"]:
            assert para["position"] == "before"

        # Check current paragraph has correct position
        assert result["current"]["position"] == "current"

        # Check after paragraphs have correct position
        for para in result["after"]:
            assert para["position"] == "after"

    def test_hierarchy_information(self, query_engine):
        """Test that hierarchy information is included."""
        result = query_engine.get_surrounding_paragraphs("bailey:ch60:2.4.4.2")

        assert result is not None
        assert "hierarchy" in result
        assert "chapter_id" in result
        assert "parent_section" in result

        # Verify hierarchy is a non-empty string
        assert isinstance(result["hierarchy"], str)
        assert len(result["hierarchy"]) > 0

        # Verify chapter ID matches expected format (supports versioned IDs)
        assert result["chapter_id"].startswith("bailey:ch60")

    def test_all_siblings_included(self, query_engine):
        """Test that all siblings in parent section are included."""
        result = query_engine.get_surrounding_paragraphs("bailey:ch60:2.4.4.2")

        assert result is not None
        assert "all_siblings" in result
        assert isinstance(result["all_siblings"], list)
        assert len(result["all_siblings"]) > 0

        # All siblings should have required fields
        for sibling in result["all_siblings"]:
            assert "chunk_id" in sibling
            assert "number" in sibling
            assert "text" in sibling
            assert "page" in sibling

    def test_siblings_ordering(self, query_engine):
        """Test that siblings are ordered by paragraph number."""
        result = query_engine.get_surrounding_paragraphs("bailey:ch01:1.1.2")

        assert result is not None
        assert len(result["all_siblings"]) > 1

        # Extract paragraph numbers and verify ordering
        numbers = [s["number"] for s in result["all_siblings"]]
        # Numbers should be in sorted order
        assert numbers == sorted(numbers)

    def test_invalid_chunk_id(self, query_engine):
        """Test behavior with non-existent chunk_id."""
        result = query_engine.get_surrounding_paragraphs("invalid:chunk:id")

        # Should return None for invalid chunk
        assert result is None

    def test_custom_before_after_counts(self, query_engine):
        """Test with custom before and after counts."""
        result = query_engine.get_surrounding_paragraphs(
            "bailey:ch01:1.1.2", before_count=3, after_count=5
        )

        assert result is not None
        assert result["metadata"]["requested_before"] == 3
        assert result["metadata"]["requested_after"] == 5
        # Actual counts may be less if there aren't enough paragraphs
        assert result["metadata"]["returned_before"] <= 3
        assert result["metadata"]["returned_after"] <= 5

    def test_zero_expansion(self, query_engine):
        """Test with zero before and after counts."""
        result = query_engine.get_surrounding_paragraphs(
            "bailey:ch01:1.1.2", before_count=0, after_count=0
        )

        assert result is not None
        assert result["metadata"]["returned_before"] == 0
        assert result["metadata"]["returned_after"] == 0
        assert "current" in result
        # Should still have all siblings
        assert len(result["all_siblings"]) > 0

    def test_large_expansion(self, query_engine):
        """Test with very large before and after counts."""
        result = query_engine.get_surrounding_paragraphs(
            "bailey:ch01:1.1.2", before_count=100, after_count=100
        )

        assert result is not None
        # Should be limited by actual paragraph count in section
        assert result["metadata"]["returned_before"] <= 100
        assert result["metadata"]["returned_after"] <= 100
        # Total returned should match all siblings
        total_returned = (
            result["metadata"]["returned_before"] + result["metadata"]["returned_after"] + 1
        )
        assert total_returned <= len(result["all_siblings"])

    def test_paragraph_content_completeness(self, query_engine):
        """Test that all required fields are present in paragraph data."""
        result = query_engine.get_surrounding_paragraphs("bailey:ch01:1.1.2")

        assert result is not None

        # Required fields in current paragraph
        required_fields = ["chunk_id", "number", "text", "page", "position"]
        for field in required_fields:
            assert field in result["current"]
            assert result["current"][field] is not None

        # Required fields in before/after paragraphs
        for para in result["before"]:
            for field in required_fields:
                assert field in para
                assert para[field] is not None

        for para in result["after"]:
            for field in required_fields:
                assert field in para
                assert para[field] is not None


class TestHybridSearchWithExpansion:
    """Test suite for hybrid_search with paragraph expansion."""

    def test_backward_compatibility_default(self, query_engine):
        """Test that default behavior has not changed (no expansion)."""
        results = query_engine.hybrid_search("hemorrhage treatment", limit=2)

        assert results is not None
        assert len(results) > 0

        # Default should NOT include expanded_context
        for result in results:
            assert "expanded_context" not in result
            # Should still have basic context expansion (expand_context=True by default)
            assert "hierarchy" in result

    def test_expansion_disabled_explicitly(self, query_engine):
        """Test with expand_paragraphs=False explicitly set."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=2, expand_paragraphs=False
        )

        assert results is not None
        assert len(results) > 0

        for result in results:
            assert "expanded_context" not in result

    def test_expansion_enabled(self, query_engine):
        """Test with expand_paragraphs=True."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=2, expand_paragraphs=True
        )

        assert results is not None
        assert len(results) > 0

        # All results should have expanded_context
        for result in results:
            assert "expanded_context" in result
            exp = result["expanded_context"]

            # Check required keys in expanded_context
            assert "before_paragraphs" in exp
            assert "current_paragraph" in exp
            assert "after_paragraphs" in exp
            assert "parent_section" in exp
            assert "all_siblings" in exp
            assert "expansion_metadata" in exp

    def test_expansion_structure(self, query_engine):
        """Test the structure of expanded_context."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=1, expand_paragraphs=True, before_count=2, after_count=2
        )

        assert len(results) > 0
        exp = results[0]["expanded_context"]

        # Verify metadata structure
        meta = exp["expansion_metadata"]
        assert "requested_before" in meta
        assert "requested_after" in meta
        assert "returned_before" in meta
        assert "returned_after" in meta
        assert meta["requested_before"] == 2
        assert meta["requested_after"] == 2

        # Verify paragraph lists are actually lists
        assert isinstance(exp["before_paragraphs"], list)
        assert isinstance(exp["after_paragraphs"], list)
        assert isinstance(exp["all_siblings"], list)

        # Verify current paragraph structure
        current = exp["current_paragraph"]
        assert "chunk_id" in current
        assert "number" in current
        assert "text" in current
        assert "page" in current
        assert "position" in current
        assert current["position"] == "current"

    def test_custom_counts(self, query_engine):
        """Test with custom before_count and after_count."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_paragraphs=True,
            before_count=5,
            after_count=3,
        )

        assert len(results) > 0
        exp = results[0]["expanded_context"]
        meta = exp["expansion_metadata"]

        assert meta["requested_before"] == 5
        assert meta["requested_after"] == 3
        # Returned counts may be less if not enough paragraphs
        assert meta["returned_before"] <= 5
        assert meta["returned_after"] <= 3

    def test_both_expansions_enabled(self, query_engine):
        """Test with both expand_context and expand_paragraphs enabled."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_context=True,
            expand_paragraphs=True,
            before_count=1,
            after_count=1,
        )

        assert len(results) > 0
        result = results[0]

        # Should have both types of expansion
        assert "hierarchy" in result  # from expand_context
        assert "full_text" in result  # from expand_context
        assert "expanded_context" in result  # from expand_paragraphs

    def test_context_disabled_paragraphs_enabled(self, query_engine):
        """Test with expand_context=False but expand_paragraphs=True."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_context=False,
            expand_paragraphs=True,
        )

        assert len(results) > 0
        result = results[0]

        # Should have expanded_context but hierarchy should come from paragraph expansion
        assert "expanded_context" in result
        # Hierarchy should be added from paragraph expansion fallback
        assert "hierarchy" in result
        assert "chapter_id" in result

    def test_zero_expansion_counts(self, query_engine):
        """Test with zero before and after counts."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_paragraphs=True,
            before_count=0,
            after_count=0,
        )

        assert len(results) > 0
        exp = results[0]["expanded_context"]

        assert len(exp["before_paragraphs"]) == 0
        assert len(exp["after_paragraphs"]) == 0
        # Should still have current paragraph
        assert exp["current_paragraph"] is not None

    def test_expansion_with_multiple_results(self, query_engine):
        """Test that expansion works for all results."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=3, expand_paragraphs=True
        )

        assert len(results) > 0

        # All results should have expansion
        for result in results:
            assert "expanded_context" in result
            exp = result["expanded_context"]
            assert "current_paragraph" in exp
            assert "expansion_metadata" in exp

    def test_expansion_metadata_accuracy(self, query_engine):
        """Test that expansion metadata accurately reflects what was returned."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_paragraphs=True,
            before_count=2,
            after_count=2,
        )

        assert len(results) > 0
        exp = results[0]["expanded_context"]
        meta = exp["expansion_metadata"]

        # Verify counts match actual lengths
        assert meta["returned_before"] == len(exp["before_paragraphs"])
        assert meta["returned_after"] == len(exp["after_paragraphs"])

    def test_parent_section_included(self, query_engine):
        """Test that parent section information is included."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=1, expand_paragraphs=True
        )

        assert len(results) > 0
        exp = results[0]["expanded_context"]

        assert "parent_section" in exp
        # Parent section should be a string
        assert isinstance(exp["parent_section"], str)

    def test_all_siblings_populated(self, query_engine):
        """Test that all_siblings list is populated."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=1, expand_paragraphs=True
        )

        assert len(results) > 0
        exp = results[0]["expanded_context"]

        assert "all_siblings" in exp
        assert len(exp["all_siblings"]) > 0

        # All siblings should have required fields
        for sibling in exp["all_siblings"]:
            assert "chunk_id" in sibling
            assert "number" in sibling
            assert "text" in sibling
            assert "page" in sibling


class TestGetSectionSummary:
    """Test suite for get_section_summary method (TASK 2.1)."""

    def test_section_summary_basic(self, query_engine):
        """Test basic section summary retrieval."""
        # Test with a known section ID
        result = query_engine.get_section_summary("bailey:ch60:s2:ss2.4:sss2.4.4")

        assert result is not None
        assert "chunk_id" in result
        assert "number" in result
        assert "text" in result
        assert "page" in result
        assert "section_title" in result
        assert "section_id" in result
        assert "hierarchy" in result

    def test_section_summary_hierarchy(self, query_engine):
        """Test that hierarchy is correctly returned."""
        result = query_engine.get_section_summary("bailey:ch60:s2:ss2.4:sss2.4.4")

        assert result is not None
        assert isinstance(result["hierarchy"], str)
        assert len(result["hierarchy"]) > 0
        # Hierarchy should contain chapter and section titles
        assert ">" in result["hierarchy"]

    def test_section_summary_first_paragraph(self, query_engine):
        """Test that the summary is indeed the first paragraph."""
        result = query_engine.get_section_summary("bailey:ch60:s2:ss2.4:sss2.4.4")

        assert result is not None
        # First paragraph should have the lowest paragraph number in that section
        assert "number" in result
        # Number should start with the section prefix
        assert result["number"].startswith("2.4.4")

    def test_section_summary_invalid_id(self, query_engine):
        """Test with invalid section ID."""
        result = query_engine.get_section_summary("invalid:section:id")

        # Should return error dict for missing sections
        assert result is not None
        assert "error" in result
        assert result["error"] == "section_not_found"
        assert "message" in result
        assert "section_id" in result

    def test_section_summary_section_level(self, query_engine):
        """Test summary retrieval at section level (not subsection)."""
        result = query_engine.get_section_summary("bailey:ch01:s8")

        assert result is not None
        assert "chunk_id" in result
        assert "section_title" in result

    def test_section_summary_metadata_complete(self, query_engine):
        """Test that all metadata fields are present."""
        result = query_engine.get_section_summary("bailey:ch60:s2:ss2.4:sss2.4.4")

        assert result is not None
        required_fields = [
            "chunk_id",
            "number",
            "text",
            "page",
            "section_title",
            "section_id",
            "chapter_id",
            "hierarchy",
        ]
        for field in required_fields:
            assert field in result
            assert result[field] is not None


class TestGetSiblingParagraphs:
    """Test suite for get_sibling_paragraphs method (TASK 2.2)."""

    def test_siblings_same_level_only(self, query_engine):
        """Test sibling retrieval at same level only."""
        result = query_engine.get_sibling_paragraphs(
            "bailey:ch60:2.4.4.2", same_level_only=True
        )

        assert result is not None
        assert "siblings" in result
        assert "parent_id" in result
        assert "parent_title" in result
        assert "same_level_only" in result
        assert result["same_level_only"] is True

    def test_siblings_section_level(self, query_engine):
        """Test sibling retrieval at section level (same_level_only=False)."""
        result = query_engine.get_sibling_paragraphs(
            "bailey:ch60:2.4.4.2", same_level_only=False
        )

        assert result is not None
        assert "siblings" in result
        # Section level should have more siblings than immediate parent
        assert len(result["siblings"]) > 0

    def test_siblings_list_structure(self, query_engine):
        """Test that siblings list has correct structure."""
        result = query_engine.get_sibling_paragraphs("bailey:ch01:1.1.2")

        assert result is not None
        assert isinstance(result["siblings"], list)
        assert len(result["siblings"]) > 0

        # Each sibling should have required fields
        for sibling in result["siblings"]:
            assert "chunk_id" in sibling
            assert "number" in sibling
            assert "text" in sibling
            assert "page" in sibling
            assert "is_current" in sibling

    def test_siblings_current_marked(self, query_engine):
        """Test that current paragraph is marked in siblings list."""
        result = query_engine.get_sibling_paragraphs("bailey:ch01:1.1.2")

        assert result is not None
        # One and only one sibling should be marked as current
        current_count = sum(1 for s in result["siblings"] if s["is_current"])
        assert current_count == 1

        # The current sibling should match the queried chunk_id (supports versioned IDs)
        current_sibling = next(s for s in result["siblings"] if s["is_current"])
        assert current_sibling["chunk_id"].startswith("bailey:ch01:1.1.2")

    def test_siblings_ordering(self, query_engine):
        """Test that siblings are ordered by paragraph number."""
        result = query_engine.get_sibling_paragraphs("bailey:ch01:1.1.2")

        assert result is not None
        assert len(result["siblings"]) > 1

        # Extract paragraph numbers and verify ordering
        numbers = [s["number"] for s in result["siblings"]]
        assert numbers == sorted(numbers)

    def test_siblings_total_count(self, query_engine):
        """Test that total_siblings count matches list length."""
        result = query_engine.get_sibling_paragraphs("bailey:ch01:1.1.2")

        assert result is not None
        assert "total_siblings" in result
        assert result["total_siblings"] == len(result["siblings"])

    def test_siblings_hierarchy_included(self, query_engine):
        """Test that hierarchy information is included."""
        result = query_engine.get_sibling_paragraphs("bailey:ch60:2.4.4.2")

        assert result is not None
        assert "hierarchy" in result
        assert isinstance(result["hierarchy"], str)
        assert len(result["hierarchy"]) > 0

    def test_siblings_different_levels_comparison(self, query_engine):
        """Test that same_level_only affects number of siblings."""
        result_same = query_engine.get_sibling_paragraphs(
            "bailey:ch60:2.4.4.2", same_level_only=True
        )
        result_section = query_engine.get_sibling_paragraphs(
            "bailey:ch60:2.4.4.2", same_level_only=False
        )

        assert result_same is not None
        assert result_section is not None

        # Section level should typically have more siblings than immediate parent
        # (unless the paragraph is at section level already)
        assert len(result_section["siblings"]) >= len(result_same["siblings"])


class TestHybridSearchWithSectionContext:
    """Test suite for hybrid_search with include_section_context (TASK 2.3)."""

    def test_backward_compatibility_section_context(self, query_engine):
        """Test that default behavior unchanged (no section context)."""
        results = query_engine.hybrid_search("hemorrhage treatment", limit=2)

        assert results is not None
        assert len(results) > 0

        # Default should NOT include section_context
        for result in results:
            assert "section_context" not in result

    def test_section_context_enabled(self, query_engine):
        """Test with include_section_context=True."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=2, include_section_context=True
        )

        assert results is not None
        assert len(results) > 0

        # All results should have section_context
        for result in results:
            assert "section_context" in result

    def test_section_context_structure(self, query_engine):
        """Test the structure of section_context."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=1, include_section_context=True
        )

        assert len(results) > 0
        section_ctx = results[0]["section_context"]

        # Verify structure
        assert "parent_id" in section_ctx
        assert "parent_title" in section_ctx
        assert "summary_paragraph" in section_ctx
        assert "hierarchy" in section_ctx

        # Verify summary paragraph structure
        summary = section_ctx["summary_paragraph"]
        assert "chunk_id" in summary
        assert "number" in summary
        assert "text" in summary
        assert "page" in summary

    def test_section_context_with_expand_context(self, query_engine):
        """Test section context combined with expand_context."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_context=True,
            include_section_context=True,
        )

        assert len(results) > 0
        result = results[0]

        # Should have both context types
        assert "hierarchy" in result
        assert "section_context" in result
        assert "hierarchy_details" in result

    def test_section_context_with_expand_paragraphs(self, query_engine):
        """Test section context combined with expand_paragraphs."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_paragraphs=True,
            include_section_context=True,
        )

        assert len(results) > 0
        result = results[0]

        # Should have both expansion types
        assert "expanded_context" in result
        assert "section_context" in result

    def test_section_context_all_expansions(self, query_engine):
        """Test all expansion modes together."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_context=True,
            expand_paragraphs=True,
            include_section_context=True,
        )

        assert len(results) > 0
        result = results[0]

        # Should have all three expansion types
        assert "hierarchy" in result
        assert "expanded_context" in result
        assert "section_context" in result
        assert "hierarchy_details" in result

    def test_section_context_hierarchy_fallback(self, query_engine):
        """Test that hierarchy is added from section context if not present."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment",
            limit=1,
            expand_context=False,
            include_section_context=True,
        )

        assert len(results) > 0
        result = results[0]

        # Hierarchy should be added from section context
        assert "hierarchy" in result
        assert isinstance(result["hierarchy"], str)

    def test_section_context_parent_info(self, query_engine):
        """Test that parent section information is accurate."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=1, include_section_context=True
        )

        assert len(results) > 0
        section_ctx = results[0]["section_context"]

        # Parent title should be a non-empty string
        assert isinstance(section_ctx["parent_title"], str)
        assert len(section_ctx["parent_title"]) > 0

        # Parent ID should follow the ID format
        assert isinstance(section_ctx["parent_id"], str)
        assert ":" in section_ctx["parent_id"]

    def test_hierarchy_details_added(self, query_engine):
        """Test that hierarchy_details are added with expand_context."""
        results = query_engine.hybrid_search(
            "hemorrhage treatment", limit=1, expand_context=True
        )

        assert len(results) > 0
        result = results[0]

        # Should have hierarchy_details
        assert "hierarchy_details" in result
        details = result["hierarchy_details"]

        # Should have all hierarchy levels (may be None)
        assert "chapter_title" in details
        assert "section_title" in details
        assert "subsection_title" in details
        assert "subsubsection_title" in details
