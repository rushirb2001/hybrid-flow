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
        embedding_model="sentence-transformers/all-MiniLM-L6-v2",
    )
    yield pipeline_instance
    pipeline_instance.close()


@pytest.fixture
def bailey_chapter_file(tmp_path):
    """Create a complete Bailey chapter JSON file for testing."""
    data = {
        "chapter_number": "2",
        "title": "Shock and blood transfusion",
        "sections": [
            {
                "title": "Introduction to Shock",
                "number": "2",
                "paragraphs": [
                    {
                        "number": "2.1",
                        "text": "Shock is a life-threatening condition characterized by inadequate tissue perfusion.",
                        "page": 10,
                        "bounds": {"x1": 50.0, "y1": 100.0, "x2": 500.0, "y2": 120.0},
                    },
                    {
                        "number": "2.2",
                        "text": "Early recognition and treatment are critical for patient survival.",
                        "page": 10,
                        "bounds": {"x1": 50.0, "y1": 125.0, "x2": 500.0, "y2": 145.0},
                    },
                ],
                "subsections": [
                    {
                        "title": "Types of Shock",
                        "number": "2.1",
                        "paragraphs": [
                            {
                                "number": "2.1.1",
                                "text": "Hypovolemic shock results from decreased blood volume.",
                                "page": 11,
                                "bounds": {"x1": 50.0, "y1": 100.0, "x2": 500.0, "y2": 120.0},
                            },
                            {
                                "number": "2.1.2",
                                "text": "Cardiogenic shock occurs when the heart cannot pump effectively.",
                                "page": 11,
                                "bounds": {"x1": 50.0, "y1": 125.0, "x2": 500.0, "y2": 145.0},
                            },
                        ],
                        "subsubsections": [
                            {
                                "title": "Hemorrhagic Shock",
                                "number": "2.1.1",
                                "paragraphs": [
                                    {
                                        "number": "2.1.1.1",
                                        "text": "Hemorrhagic shock is the most common type of hypovolemic shock in trauma patients.",
                                        "page": 12,
                                        "bounds": {"x1": 50.0, "y1": 100.0, "x2": 500.0, "y2": 120.0},
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
        "authors": ["Dr. Norman Williams", "Dr. Christopher Bulstrode"],
        "key_points": [],
        "references": [],
    }

    file_path = tmp_path / "bailey" / "chapter_2.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return str(file_path)


def test_ingest_single_chapter(pipeline, bailey_chapter_file):
    """Test ingesting a single chapter successfully."""
    result = pipeline.ingest_chapter(bailey_chapter_file)

    assert result["status"] == "success"
    assert result["chunks_inserted"] > 0
    assert result["chunks_inserted"] == 5


def test_ingest_chapter_creates_neo4j_hierarchy(pipeline, bailey_chapter_file):
    """Test that ingesting creates proper Neo4j hierarchy."""
    pipeline.ingest_chapter(bailey_chapter_file)

    # Query Neo4j for chapter node using driver session
    query = """
    MATCH (c:Chapter {id: $chapter_id})
    RETURN c.title as title
    """

    with pipeline.neo4j_storage.driver.session() as session:
        result = session.run(query, chapter_id="bailey:ch2")
        records = list(result)

    assert len(records) == 1
    assert records[0]["title"] == "Shock and blood transfusion"


def test_ingest_chapter_creates_qdrant_vectors(pipeline, bailey_chapter_file):
    """Test that ingesting creates Qdrant vectors."""
    pipeline.ingest_chapter(bailey_chapter_file)

    # Get collection info from Qdrant
    info = pipeline.qdrant_storage.get_collection_info()

    assert info["points_count"] > 0
    assert info["points_count"] == 5


def test_ingest_chapter_creates_metadata(pipeline, bailey_chapter_file):
    """Test that ingesting creates metadata record."""
    pipeline.ingest_chapter(bailey_chapter_file)

    # Get metadata from database
    metadata = pipeline.metadata_db.get_chapter_by_id("bailey", "2")

    assert metadata is not None
    assert metadata.textbook_id == "bailey"
    assert metadata.chapter_number == "2"
    assert metadata.version == 1


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
        embedding_model="sentence-transformers/all-MiniLM-L6-v2",
    )

    try:
        # First ingestion
        result1 = pipeline_instance.ingest_chapter(bailey_chapter_file)
        assert result1["status"] == "success"

        # Get first version
        metadata1 = pipeline_instance.metadata_db.get_chapter_by_id("bailey", "2")
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
        metadata2 = pipeline_instance.metadata_db.get_chapter_by_id("bailey", "2")
        assert metadata2.version == 2

    finally:
        pipeline_instance.close()
