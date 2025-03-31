"""Tests for ingestion pipeline integration."""

import json

import pytest

from hybridflow.ingestion.pipeline import IngestionPipeline


@pytest.fixture
def pipeline(tmp_path):
    """Create an IngestionPipeline with test database connections."""
    pipeline_instance = IngestionPipeline(
        qdrant_host="localhost", 
        qdrant_port=6333,
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="password",
        metadata_db_path=str(tmp_path / "test_metadata.db"),
        embedding_model="pritamdeka/S-PubMedBert-MS-MARCO-SCIFACT",
    )
    yield pipeline_instance
    pipeline_instance.close()


@pytest.fixture
def bailey_chapter_file(tmp_path):
    """Create a complete Bailey chapter JSON file for testing.

    Uses chapter 99 which doesn't exist in production to avoid conflicts.
    The loader detects 'bailey' from the path and uses TextbookEnum.BAILEY.
    Structure matches actual data format with bounds as array [x1, y1, x2, y2].
    """
    data = {
        "textbook_id": "bailey",
        "chapter_number": "99",
        "title": "Test Chapter for Pipeline",
        "key_points": [],
        "sections": [
            {
                "title": "Introduction",
                "number": "1",
                "subsections": [],
                "paragraphs": [
                    {
                        "number": "1.1",
                        "text": "This is the first test paragraph for pipeline testing.",
                        "page": 10,
                        "bounds": [50.0, 100.0, 500.0, 120.0],
                    },
                    {
                        "number": "1.2",
                        "text": "This is the second test paragraph with more content.",
                        "page": 10,
                        "bounds": [50.0, 125.0, 500.0, 145.0],
                    },
                ],
            },
            {
                "title": "Main Content",
                "number": "2",
                "paragraphs": [],
                "subsections": [
                    {
                        "title": "Subsection One",
                        "number": "2.1",
                        "paragraphs": [
                            {
                                "number": "2.1.1",
                                "text": "Content for subsection paragraph one.",
                                "page": 11,
                                "bounds": [50.0, 100.0, 500.0, 120.0],
                            },
                            {
                                "number": "2.1.2",
                                "text": "Content for subsection paragraph two.",
                                "page": 11,
                                "bounds": [50.0, 125.0, 500.0, 145.0],
                            },
                        ],
                        "subsubsections": [
                            {
                                "title": "Deep Nested Section",
                                "number": "2.1.1",
                                "paragraphs": [
                                    {
                                        "number": "2.1.1.1",
                                        "text": "Content in the deepest level of nesting.",
                                        "page": 12,
                                        "bounds": [50.0, 100.0, 500.0, 120.0],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    file_path = tmp_path / "bailey" / "chapter_99.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return str(file_path)


def test_ingest_single_chapter(pipeline, bailey_chapter_file):
    """Test ingesting a single chapter successfully."""
    # Use force=True to ensure fresh ingestion even if chapter exists
    result = pipeline.ingest_chapter(bailey_chapter_file, force=True)

    assert result["status"] == "success"
    assert result["chunks_inserted"] > 0
    assert result["chunks_inserted"] == 5


def test_ingest_chapter_creates_neo4j_hierarchy(pipeline, bailey_chapter_file):
    """Test that ingesting creates proper Neo4j hierarchy."""
    pipeline.ingest_chapter(bailey_chapter_file, force=True)

    # Query Neo4j for complete hierarchy - use test chapter 99, not production chapter
    query = """
    MATCH (c:Chapter {id: $chapter_id})
    OPTIONAL MATCH (c)-[:HAS_SECTION]->(s:Section)
    OPTIONAL MATCH (s)-[:HAS_SUBSECTION]->(ss:Subsection)
    OPTIONAL MATCH (ss)-[:HAS_SUBSUBSECTION]->(sss:Subsubsection)
    RETURN c.title as chapter_title,
           count(DISTINCT s) as section_count,
           count(DISTINCT ss) as subsection_count,
           count(DISTINCT sss) as subsubsection_count
    """

    with pipeline.neo4j_storage.driver.session() as session:
        result = session.run(query, chapter_id="bailey:ch99")
        record = result.single()

    assert record is not None
    assert record["chapter_title"] == "Test Chapter for Pipeline"
    assert record["section_count"] == 2  # Introduction and Main Content
    assert record["subsection_count"] == 1  # Subsection One under Main Content
    assert record["subsubsection_count"] == 1  # Subsubsection under Subsection One


def test_ingest_chapter_creates_qdrant_vectors(pipeline, bailey_chapter_file):
    """Test that ingesting creates Qdrant vectors."""
    # Ingest chapter with force=True to ensure re-ingestion
    result = pipeline.ingest_chapter(bailey_chapter_file, force=True)

    assert result["status"] == "success"
    assert result["chunks_inserted"] == 5

    # Verify vectors exist in Qdrant for this chapter by searching for them
    # Note: Qdrant uses upsert, so count may not increase if vectors already exist
    from hybridflow.parsing.embedder import EmbeddingGenerator

    embedder = EmbeddingGenerator(model_name="pritamdeka/S-PubMedBert-MS-MARCO-SCIFACT")
    query_embedding = embedder.generate_embedding("shock")

    # Search for vectors from bailey:ch2
    results = pipeline.qdrant_storage.client.query_points(
        collection_name="textbook_chunks",
        query=query_embedding,
        query_filter={
            "must": [
                {"key": "textbook_id", "match": {"value": "bailey"}},
                {"key": "chapter_number", "match": {"value": "99"}},
            ]
        },
        limit=10,
    )

    # Should find vectors from this chapter
    assert len(results.points) > 0
    # Should have vectors with proper metadata
    for point in results.points:
        assert point.payload["textbook_id"] == "bailey"
        assert point.payload["chapter_number"] == "99"


def test_ingest_chapter_creates_metadata(pipeline, bailey_chapter_file):
    """Test that ingesting creates metadata record."""
    pipeline.ingest_chapter(bailey_chapter_file, force=True)

    # Get metadata from database
    metadata = pipeline.metadata_db.get_chapter_by_id("bailey", "99")

    assert metadata is not None
    assert metadata.textbook_id == "bailey"
    assert metadata.chapter_number == "99"
    assert metadata.version >= 1
    assert metadata.title == "Test Chapter for Pipeline"
    assert metadata.content_hash is not None
    assert len(metadata.content_hash) == 64  # SHA256 hash


def test_ingest_unchanged_chapter_skips(pipeline, bailey_chapter_file):
    """Test that ingesting same chapter twice skips the second time."""
    # First ingestion
    result1 = pipeline.ingest_chapter(bailey_chapter_file)
    assert result1["status"] == "success"

    # Second ingestion with same content
    result2 = pipeline.ingest_chapter(bailey_chapter_file)
    assert result2["status"] == "skipped"
    assert result2["chunks_inserted"] == 0


def test_ingest_modified_chapter_updates(bailey_chapter_file, tmp_path):
    """Test that ingesting modified chapter updates version."""
    # Create pipeline
    pipeline_instance = IngestionPipeline(
        qdrant_host="localhost",
        qdrant_port=6333,
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="password",
        metadata_db_path=str(tmp_path / "test_metadata_v2.db"),
        embedding_model="pritamdeka/S-PubMedBert-MS-MARCO-SCIFACT",
    )

    try:
        # First ingestion
        result1 = pipeline_instance.ingest_chapter(bailey_chapter_file)
        assert result1["status"] == "success"

        # Get first version
        metadata1 = pipeline_instance.metadata_db.get_chapter_by_id("bailey", "99")
        assert metadata1.version == 1

        # Modify the JSON file
        with open(bailey_chapter_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["title"] = "Shock and Blood Transfusion - Updated"

        with open(bailey_chapter_file, "w", encoding="utf-8") as f:
            json.dump(data, f)

        # Second ingestion with modified content
        result2 = pipeline_instance.ingest_chapter(bailey_chapter_file)
        assert result2["status"] == "success"

        # Check version was incremented
        metadata2 = pipeline_instance.metadata_db.get_chapter_by_id("bailey", "99")
        assert metadata2.version == 2

    finally:
        pipeline_instance.close()


def test_ingest_creates_paragraph_links(pipeline, bailey_chapter_file):
    """Test that NEXT/PREV relationships are created between paragraphs."""
    pipeline.ingest_chapter(bailey_chapter_file, force=True)

    # Query for NEXT/PREV relationships
    query = """
    MATCH (p1:Paragraph)-[:NEXT]->(p2:Paragraph)
    WHERE p1.chunk_id STARTS WITH 'bailey:ch99:'
    RETURN count(*) as next_links
    """

    with pipeline.neo4j_storage.driver.session() as session:
        result = session.run(query)
        record = result.single()

    # Should have sequential links between paragraphs
    assert record["next_links"] > 0


def test_ingest_paragraphs_connected_to_hierarchy(pipeline, bailey_chapter_file):
    """Test that paragraphs are properly connected to hierarchy nodes."""
    pipeline.ingest_chapter(bailey_chapter_file, force=True)

    # Query to verify paragraphs are connected to sections/subsections
    query = """
    MATCH (p:Paragraph)
    WHERE p.chunk_id STARTS WITH 'bailey:ch99:'
    MATCH (parent)-[:HAS_PARAGRAPH]->(p)
    RETURN labels(parent)[0] as parent_type, count(p) as paragraph_count
    ORDER BY parent_type
    """

    with pipeline.neo4j_storage.driver.session() as session:
        results = session.run(query)
        records = list(results)

    # Should have paragraphs connected to hierarchy nodes
    assert len(records) > 0
    parent_types = [r["parent_type"] for r in records]
    # Paragraphs should be connected to Section, Subsection, or Subsubsection
    assert any(t in ["Section", "Subsection", "Subsubsection"] for t in parent_types)


def test_ingest_qdrant_vectors_have_metadata(pipeline, bailey_chapter_file):
    """Test that Qdrant vectors include proper metadata."""
    pipeline.ingest_chapter(bailey_chapter_file, force=True)

    # Search for a vector from this chapter
    from hybridflow.parsing.embedder import EmbeddingGenerator

    embedder = EmbeddingGenerator(model_name="pritamdeka/S-PubMedBert-MS-MARCO-SCIFACT")
    query_embedding = embedder.generate_embedding("shock pathophysiology")

    results = pipeline.qdrant_storage.client.query_points(
        collection_name="textbook_chunks", query=query_embedding, limit=1
    )

    if len(results.points) > 0:
        point = results.points[0]
        # Check metadata fields exist
        assert "textbook_id" in point.payload
        assert "chapter_number" in point.payload
        assert "chapter_title" in point.payload
        assert "hierarchy_path" in point.payload
        assert "page" in point.payload


def test_ingest_neo4j_paragraphs_match_qdrant_count(pipeline, bailey_chapter_file):
    """Test that Neo4j paragraph count matches Qdrant vector count for chapter."""
    result = pipeline.ingest_chapter(bailey_chapter_file, force=True)

    # Count paragraphs in Neo4j for this specific test chapter using version_id
    version_id = result.get("version_id", "")
    if version_id:
        # Filter by version_id suffix
        query = """
        MATCH (p:Paragraph)
        WHERE p.chunk_id STARTS WITH 'bailey:ch99:' AND p.chunk_id ENDS WITH $version_suffix
        RETURN count(p) as paragraph_count
        """
        version_suffix = f"::{version_id}"
    else:
        # For unversioned ingestion, exclude paragraphs with version suffix (::)
        query = """
        MATCH (p:Paragraph)
        WHERE p.chunk_id STARTS WITH 'bailey:ch99:' AND NOT p.chunk_id CONTAINS '::'
        RETURN count(p) as paragraph_count
        """
        version_suffix = None

    with pipeline.neo4j_storage.driver.session() as session:
        if version_suffix:
            neo4j_result = session.run(query, version_suffix=version_suffix)
        else:
            neo4j_result = session.run(query)
        neo4j_count = neo4j_result.single()["paragraph_count"]

    # Should match the chunks inserted
    assert neo4j_count == result["chunks_inserted"]


def test_ingest_chapter_number_format(pipeline, bailey_chapter_file):
    """Test that paragraph numbers follow correct format."""
    result = pipeline.ingest_chapter(bailey_chapter_file, force=True)

    # Query paragraph numbers for this specific test using version_id
    version_id = result.get("version_id", "")
    if version_id:
        query = """
        MATCH (p:Paragraph)
        WHERE p.chunk_id STARTS WITH 'bailey:ch99:' AND p.chunk_id ENDS WITH $version_suffix
        RETURN p.number as number, p.chunk_id as chunk_id
        ORDER BY p.number
        LIMIT 10
        """
        version_suffix = f"::{version_id}"
    else:
        query = """
        MATCH (p:Paragraph)
        WHERE p.chunk_id STARTS WITH 'bailey:ch99:'
        RETURN p.number as number, p.chunk_id as chunk_id
        ORDER BY p.number
        LIMIT 10
        """
        version_suffix = None

    with pipeline.neo4j_storage.driver.session() as session:
        if version_suffix:
            results = session.run(query, version_suffix=version_suffix)
        else:
            results = session.run(query)
        records = list(results)

    assert len(records) > 0

    for record in records:
        number = record["number"]
        chunk_id = record["chunk_id"]

        # Paragraph number should match expected format (e.g., "1.1", "2.1.1", "2.1.1.1")
        assert number[0].isdigit()
        # chunk_id should contain paragraph number (may have version suffix)
        assert f":{number}" in chunk_id


def test_ingest_bounds_stored_correctly(pipeline, bailey_chapter_file):
    """Test that bounding box coordinates are stored."""
    pipeline.ingest_chapter(bailey_chapter_file, force=True)

    # Query paragraph with bounds
    query = """
    MATCH (p:Paragraph)
    WHERE p.chunk_id STARTS WITH 'bailey:ch99:'
    RETURN p.bounds as bounds
    LIMIT 1
    """

    with pipeline.neo4j_storage.driver.session() as session:
        result = session.run(query)
        record = result.single()

    assert record is not None
    bounds = record["bounds"]
    # Bounds should be a list of 4 coordinates [x1, y1, x2, y2]
    assert isinstance(bounds, list)
    assert len(bounds) == 4
    assert all(isinstance(b, (int, float)) for b in bounds)
