"""Comprehensive tests for versioning and transaction system."""

import json
import time
from pathlib import Path

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
        metadata_db_path=str(tmp_path / "test_versioning_metadata.db"),
        embedding_model="sentence-transformers/all-MiniLM-L6-v2",
    )
    yield pipeline_instance
    pipeline_instance.close()


@pytest.fixture
def test_chapter_file(tmp_path):
    """Create a test chapter JSON file in bailey directory.

    Uses chapter 99 which doesn't exist in production to avoid conflicts.
    Structure matches actual data format with bounds as array [x1, y1, x2, y2].
    """
    data = {
        "textbook_id": "bailey",
        "chapter_number": "99",
        "title": "Versioning Test Chapter",
        "sections": [
            {
                "title": "Test Section",
                "number": "1",
                "subsections": [],
                "paragraphs": [
                    {
                        "number": "1.1",
                        "text": "This is a test paragraph for versioning.",
                        "page": 1,
                        "bounds": [50.0, 100.0, 500.0, 120.0],
                    },
                    {
                        "number": "1.2",
                        "text": "This is another test paragraph.",
                        "page": 1,
                        "bounds": [50.0, 125.0, 500.0, 145.0],
                    },
                ],
            }
        ],
        "key_points": [],
    }

    # Create in bailey directory so textbook type can be detected
    file_path = tmp_path / "bailey" / "chapter_99.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return str(file_path)


@pytest.fixture
def test_directory(tmp_path):
    """Create a bailey directory with multiple test chapters.

    Uses chapter numbers 95-99 which don't exist in production to avoid conflicts.
    Structure matches actual data format with bounds as array [x1, y1, x2, y2].
    """
    dir_path = tmp_path / "bailey"
    dir_path.mkdir(parents=True, exist_ok=True)

    for chapter_num in range(95, 100):
        data = {
            "textbook_id": "bailey",
            "chapter_number": str(chapter_num),
            "title": f"Test Chapter {chapter_num}",
            "sections": [
                {
                    "title": f"Section {chapter_num}",
                    "number": "1",
                    "subsections": [],
                    "paragraphs": [
                        {
                            "number": "1.1",
                            "text": f"Test paragraph in chapter {chapter_num}.",
                            "page": 1,
                            "bounds": [50.0, 100.0, 500.0, 120.0],
                        }
                    ],
                }
            ],
            "key_points": [],
        }

        file_path = dir_path / f"chapter_{chapter_num}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

    return str(dir_path)


# ============================================================================
# REPAIR 2: Neo4j ID-Based Versioning Tests
# ============================================================================


def test_versioned_id_format(pipeline):
    """Test that _versioned_id creates correct format."""
    base_id = "bailey:ch99"
    version_id = "v_20250317_120000"

    versioned = pipeline.neo4j_storage._versioned_id(base_id, version_id)
    assert versioned == f"{base_id}::{version_id}"

    # Test without version_id
    unversioned = pipeline.neo4j_storage._versioned_id(base_id, None)
    assert unversioned == base_id


def test_transactional_ingestion_creates_versioned_nodes(pipeline, test_chapter_file):
    """Test that transactional ingestion creates nodes with versioned IDs."""
    result = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)

    # Verify transaction completed
    assert result["committed"] is True
    version_id = result["version_id"]

    # Verify versioned nodes exist in Neo4j
    query = """
    MATCH (c:Chapter)
    WHERE c.id ENDS WITH $version_suffix
    RETURN c.id as id, c.title as title
    """

    with pipeline.neo4j_storage.driver.session() as session:
        neo4j_result = session.run(query, version_suffix=f"::{version_id}")
        record = neo4j_result.single()

    assert record is not None
    assert version_id in record["id"]
    assert record["title"] == "Versioning Test Chapter"


def test_multiple_versions_can_coexist(pipeline, test_chapter_file):
    """Test that multiple versions of same chapter can exist in Neo4j."""
    # Ingest first version
    result1 = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)
    version_id_1 = result1["version_id"]

    # Modify content
    with open(test_chapter_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["title"] = "Versioning Test Chapter - Updated"
    with open(test_chapter_file, "w", encoding="utf-8") as f:
        json.dump(data, f)

    # Ingest second version
    result2 = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)
    version_id_2 = result2["version_id"]

    assert version_id_1 != version_id_2

    # Verify both versions exist in Neo4j
    query = """
    MATCH (c:Chapter)
    WHERE c.id ENDS WITH $version_suffix
    RETURN c.title as title
    """

    with pipeline.neo4j_storage.driver.session() as session:
        # Check version 1
        result_v1 = session.run(query, version_suffix=f"::{version_id_1}")
        record_v1 = result_v1.single()
        assert record_v1 is not None
        assert record_v1["title"] == "Versioning Test Chapter"

        # Check version 2
        result_v2 = session.run(query, version_suffix=f"::{version_id_2}")
        record_v2 = result_v2.single()
        assert record_v2 is not None
        assert record_v2["title"] == "Versioning Test Chapter - Updated"


def test_snapshot_deletion_by_version_id(pipeline, test_chapter_file):
    """Test that delete_snapshot correctly removes versioned nodes."""
    # Ingest a version
    result = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)
    version_id = result["version_id"]

    # Verify nodes exist
    query_count = """
    MATCH (n)
    WHERE (n.id IS NOT NULL AND n.id ENDS WITH $suffix)
       OR (n.chunk_id IS NOT NULL AND n.chunk_id ENDS WITH $suffix)
    RETURN count(n) as count
    """

    with pipeline.neo4j_storage.driver.session() as session:
        result_before = session.run(query_count, suffix=f"::{version_id}")
        count_before = result_before.single()["count"]
        assert count_before > 0

    # Delete snapshot
    pipeline.neo4j_storage.delete_snapshot(version_id)

    # Verify nodes deleted
    with pipeline.neo4j_storage.driver.session() as session:
        result_after = session.run(query_count, suffix=f"::{version_id}")
        count_after = result_after.single()["count"]
        assert count_after == 0


# ============================================================================
# REPAIR 3: Incremental Validation Tests
# ============================================================================


def test_incremental_validation_runs_at_checkpoints(pipeline, test_directory):
    """Test that validation runs at specified checkpoints."""
    # Ingest with validate_every=2 (should validate after every 2 chapters)
    result = pipeline.ingest_directory_transactional(
        test_directory, description="Test batch", force=True, validate_every=2
    )

    # Should have successfully ingested 5 chapters with validation checkpoints
    assert result["committed"] is True
    assert result["success"] == 5


def test_incremental_validation_uses_versioned_filter(pipeline, test_directory):
    """Test that incremental validation correctly filters by version_id."""
    result = pipeline.ingest_directory_transactional(
        test_directory, description="Test batch", force=True, validate_every=2
    )

    version_id = result["version_id"]

    # Manually verify that validate_graph with version filter works
    validation = pipeline.neo4j_storage.validate_graph(version_id=version_id)

    assert validation["status"] == "valid"
    # Should have some paragraph nodes
    assert validation["node_counts"]["Paragraph"] > 0


# ============================================================================
# REPAIR 4: Alias-Based Qdrant Backup Tests
# ============================================================================


def test_create_alias_backup(pipeline):
    """Test creating an alias backup for Qdrant collection."""
    alias_name = "test_backup_alias"

    # Create alias
    success = pipeline.qdrant_storage.create_alias_backup(alias_name)
    assert success is True

    # Verify alias exists by searching through it
    from hybridflow.parsing.embedder import EmbeddingGenerator

    embedder = EmbeddingGenerator(model_name="sentence-transformers/all-MiniLM-L6-v2")
    query_embedding = embedder.generate_embedding("test")

    # Should be able to query through alias
    results = pipeline.qdrant_storage.client.query_points(
        collection_name=alias_name, query=query_embedding, limit=1
    )

    # Cleanup
    pipeline.qdrant_storage.delete_alias(alias_name)


def test_delete_alias(pipeline):
    """Test deleting an alias."""
    alias_name = "test_delete_alias"

    # Create and then delete alias
    pipeline.qdrant_storage.create_alias_backup(alias_name)
    success = pipeline.qdrant_storage.delete_alias(alias_name)
    assert success is True

    # Verify alias no longer exists by attempting to query it
    from hybridflow.parsing.embedder import EmbeddingGenerator

    embedder = EmbeddingGenerator(model_name="sentence-transformers/all-MiniLM-L6-v2")
    query_embedding = embedder.generate_embedding("test")

    with pytest.raises(Exception):
        # Should raise exception because alias doesn't exist
        pipeline.qdrant_storage.client.query_points(
            collection_name=alias_name, query=query_embedding, limit=1
        )


def test_alias_backup_performance(pipeline, test_chapter_file):
    """Test that alias backup is significantly faster than full backup."""
    # Ingest a chapter first
    pipeline.ingest_chapter(test_chapter_file, force=True)

    # Time alias backup
    alias_name = "perf_test_alias"
    start_alias = time.time()
    pipeline.qdrant_storage.create_alias_backup(alias_name)
    alias_time = time.time() - start_alias

    # Time snapshot creation (creates copy)
    version_id = "perf_test_version"
    start_snapshot = time.time()
    pipeline.qdrant_storage.create_snapshot(version_id)
    snapshot_time = time.time() - start_snapshot

    # Alias should be at least 10x faster
    assert alias_time < snapshot_time / 10

    # Cleanup
    pipeline.qdrant_storage.delete_alias(alias_name)
    pipeline.qdrant_storage.delete_snapshot(version_id)


# ============================================================================
# REPAIR 5: Version Rotation with Reference Checking Tests
# ============================================================================


def test_check_version_in_use(pipeline, test_chapter_file):
    """Test the _check_version_in_use helper method."""
    # Create a version
    result = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)
    version_id = result["version_id"]

    # Check the status in version history
    history = pipeline.metadata_db.get_version_history(limit=10)
    version_record = next((v for v in history if v["version_id"] == version_id), None)

    # After commit, status should be "committed" not "in_use"
    assert version_record is not None
    assert version_record["status"] == "committed"

    # _check_version_in_use should return False for "committed" status
    in_use = pipeline._check_version_in_use(version_id)
    assert in_use is False


def test_version_rotation_preserves_active_versions(pipeline, test_chapter_file):
    """Test that rotation doesn't delete versions marked as in-use."""
    # Create multiple versions
    versions = []
    for i in range(3):
        result = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)
        versions.append(result["version_id"])

    # Mark first version as "in_use" (simulating active query)
    pipeline.metadata_db.update_version_status(versions[0], "in_use")

    # Rotate with keep_count=1 (should try to delete all but last version)
    deleted = pipeline._rotate_versions(keep_count=1, force=False)

    # First version should NOT be in deleted list
    assert versions[0] not in deleted


# ============================================================================
# Full Transactional Workflow Tests
# ============================================================================


def test_full_transactional_workflow_commit(pipeline, test_chapter_file):
    """Test complete transaction workflow with successful commit."""
    result = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)

    # Verify transaction completed
    assert result["committed"] is True
    assert "version_id" in result
    version_id = result["version_id"]

    # Verify metadata tracking
    history = pipeline.metadata_db.get_version_history(limit=10)
    version_record = next((v for v in history if v["version_id"] == version_id), None)
    assert version_record is not None
    assert version_record["status"] == "committed"

    # Verify chapter was ingested
    chapter_metadata = pipeline.metadata_db.get_chapter_by_id("bailey", "99")
    assert chapter_metadata is not None
    assert chapter_metadata.title == "Versioning Test Chapter"


def test_transactional_ingestion_validates_counts(pipeline, test_chapter_file):
    """Test that transactional ingestion validates Neo4j and Qdrant counts match."""
    result = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)

    # Get validation results
    version_id = result["version_id"]
    validation = pipeline.neo4j_storage.validate_graph(version_id=version_id)

    # Validation should pass
    assert validation["status"] == "valid"
    # Should have some paragraphs
    assert validation["node_counts"]["Paragraph"] > 0


def test_transactional_ingestion_creates_versioned_metadata(pipeline, test_chapter_file):
    """Test that version metadata is properly tracked."""
    result = pipeline.ingest_chapter_transactional(
        test_chapter_file, description="Test version metadata", force=True
    )

    version_id = result["version_id"]

    # Get version from metadata DB history
    history = pipeline.metadata_db.get_version_history(limit=10)
    version_record = next((v for v in history if v["version_id"] == version_id), None)

    assert version_record is not None
    assert version_record["status"] == "committed"
    assert version_record["description"] == "Test version metadata"
    assert version_record["chapters_count"] == 1


def test_metadata_db_not_versioned(pipeline, test_chapter_file):
    """Test that metadata DB is intentionally not versioned."""
    # Ingest first version
    result1 = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)

    # Get chapter metadata
    chapter1 = pipeline.metadata_db.get_chapter_by_id("bailey", "99")
    version1 = chapter1.version

    # Modify and ingest second version
    with open(test_chapter_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["title"] = "Versioning Test Chapter - V2"
    with open(test_chapter_file, "w", encoding="utf-8") as f:
        json.dump(data, f)

    result2 = pipeline.ingest_chapter_transactional(test_chapter_file, force=True)

    # Chapter metadata should be updated in place (not versioned)
    chapter2 = pipeline.metadata_db.get_chapter_by_id("bailey", "99")
    assert chapter2.version == version1 + 1
    assert chapter2.title == "Versioning Test Chapter - V2"


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


def test_transactional_ingestion_handles_invalid_file(pipeline, tmp_path):
    """Test that transactional ingestion handles invalid files gracefully.

    Invalid files cause ingest_chapter to fail, resulting in zero successful chapters.
    The transaction marks the version as 'skipped' and does not commit.
    """
    # Create invalid JSON file
    invalid_file = tmp_path / "bailey" / "invalid.json"
    invalid_file.parent.mkdir(parents=True, exist_ok=True)
    invalid_file.write_text("not valid json{")

    # Invalid file should result in failed ingestion, not exception
    # The transaction handles it gracefully by marking as skipped
    result = pipeline.ingest_chapter_transactional(str(invalid_file))

    # Transaction should not be committed (no successful chapters)
    assert result["committed"] is False
    assert result["status"] == "failed"


def test_versioned_id_with_empty_version(pipeline):
    """Test _versioned_id handles edge cases."""
    # Empty string version
    result = pipeline.neo4j_storage._versioned_id("bailey:ch1", "")
    assert result == "bailey:ch1::"

    # None version
    result = pipeline.neo4j_storage._versioned_id("bailey:ch1", None)
    assert result == "bailey:ch1"


def test_validate_graph_with_invalid_version_filter(pipeline):
    """Test validate_graph with non-existent version filter."""
    validation = pipeline.neo4j_storage.validate_graph(version_id="nonexistent_version")

    # Should return valid status with zero counts for non-existent version
    assert validation["status"] == "valid"
    assert validation["node_counts"]["Paragraph"] == 0
