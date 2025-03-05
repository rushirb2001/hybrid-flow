"""Tests for metadata database functionality."""

import pytest
from sqlalchemy import text

from hybridflow.models import Bounds, Chapter, Section, TextbookEnum
from hybridflow.storage.metadata_db import MetadataDatabase
from hybridflow.storage.models import IngestionLog


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary metadata database for testing."""
    db_path = tmp_path / "test_metadata.db"
    db = MetadataDatabase(str(db_path))
    db.create_tables()
    yield db


def test_create_tables(tmp_db):
    """Test that database tables are created successfully."""
    # Query sqlite_master to verify tables exist
    session = tmp_db.session_factory()
    try:
        result = session.execute(
            text("SELECT name FROM sqlite_master WHERE type='table'")
        ).fetchall()
        table_names = [row[0] for row in result]

        assert "chapter_metadata" in table_names
        assert "ingestion_log" in table_names
    finally:
        session.close()


def test_upsert_chapter_new(tmp_db):
    """Test inserting a new chapter creates version 1."""
    chapter = Chapter(
        chapter_number="1",
        title="Introduction to Surgery",
        sections=[],
        textbook_id=TextbookEnum.BAILEY,
        source_file_path="/data/bailey/chapter_01.json",
    )

    result = tmp_db.upsert_chapter(chapter)

    assert result is not None
    assert result.version == 1
    assert result.textbook_id == "bailey"
    assert result.chapter_number == "1"
    assert result.title == "Introduction to Surgery"


def test_upsert_chapter_unchanged(tmp_db):
    """Test upserting the same chapter twice keeps version at 1."""
    chapter = Chapter(
        chapter_number="1",
        title="Introduction to Surgery",
        sections=[],
        textbook_id=TextbookEnum.BAILEY,
        source_file_path="/data/bailey/chapter_01.json",
    )

    # First upsert
    result1 = tmp_db.upsert_chapter(chapter)
    version1 = result1.version

    # Second upsert with identical content
    result2 = tmp_db.upsert_chapter(chapter)
    version2 = result2.version

    assert version1 == 1
    assert version2 == 1


def test_upsert_chapter_modified(tmp_db):
    """Test upserting a modified chapter increments version to 2."""
    chapter = Chapter(
        chapter_number="1",
        title="Introduction to Surgery",
        sections=[],
        textbook_id=TextbookEnum.BAILEY,
        source_file_path="/data/bailey/chapter_01.json",
    )

    # First upsert
    result1 = tmp_db.upsert_chapter(chapter)
    version1 = result1.version

    # Modify the chapter
    chapter.title = "Introduction to Modern Surgery"

    # Second upsert with modified content
    result2 = tmp_db.upsert_chapter(chapter)
    version2 = result2.version

    assert version1 == 1
    assert version2 == 2


def test_get_chapter_by_id(tmp_db):
    """Test retrieving a chapter by textbook_id and chapter_number."""
    chapter = Chapter(
        chapter_number="5",
        title="Surgical Infections",
        sections=[],
        textbook_id=TextbookEnum.SABISTON,
        source_file_path="/data/sabiston/chapter_05.json",
    )

    # Insert chapter
    tmp_db.upsert_chapter(chapter)

    # Retrieve by ID
    result = tmp_db.get_chapter_by_id("sabiston", "5")

    assert result is not None
    assert result.textbook_id == "sabiston"
    assert result.chapter_number == "5"
    assert result.title == "Surgical Infections"


def test_get_chapter_nonexistent(tmp_db):
    """Test retrieving a non-existent chapter returns None."""
    result = tmp_db.get_chapter_by_id("nonexistent", "999")

    assert result is None


def test_log_ingestion(tmp_db):
    """Test logging an ingestion operation."""
    chapter = Chapter(
        chapter_number="1",
        title="Test Chapter",
        sections=[],
        textbook_id=TextbookEnum.BAILEY,
        source_file_path="/data/bailey/test.json",
    )

    # Create chapter
    chapter_metadata = tmp_db.upsert_chapter(chapter)

    # Log ingestion
    tmp_db.log_ingestion(
        chapter_id=chapter_metadata.id,
        status="success",
        parsing_strategy="multi-tier",
        error_message=None,
        chunks_inserted=50,
    )

    # Query ingestion log
    session = tmp_db.session_factory()
    try:
        log_entry = session.query(IngestionLog).filter_by(
            chapter_id=chapter_metadata.id
        ).first()

        assert log_entry is not None
        assert log_entry.status == "success"
        assert log_entry.parsing_strategy == "multi-tier"
        assert log_entry.chunks_inserted == 50
    finally:
        session.close()


def test_aggregate_stats(tmp_db):
    """Test computing aggregate statistics across multiple chapters."""
    # Create 3 chapters from different textbooks
    chapter1 = Chapter(
        chapter_number="1",
        title="Bailey Chapter 1",
        sections=[],
        textbook_id=TextbookEnum.BAILEY,
        source_file_path="/data/bailey/chapter_01.json",
    )

    chapter2 = Chapter(
        chapter_number="2",
        title="Sabiston Chapter 2",
        sections=[],
        textbook_id=TextbookEnum.SABISTON,
        source_file_path="/data/sabiston/chapter_02.json",
    )

    chapter3 = Chapter(
        chapter_number="3",
        title="Schwartz Chapter 3",
        sections=[],
        textbook_id=TextbookEnum.SCHWARTZ,
        source_file_path="/data/schwartz/chapter_03.json",
    )

    # Insert chapters
    tmp_db.upsert_chapter(chapter1)
    tmp_db.upsert_chapter(chapter2)
    tmp_db.upsert_chapter(chapter3)

    # Get aggregate stats
    stats = tmp_db.get_aggregate_stats()

    assert stats["total_chapters"] == 3
    assert stats["chapters_by_textbook"]["bailey"] == 1
    assert stats["chapters_by_textbook"]["sabiston"] == 1
    assert stats["chapters_by_textbook"]["schwartz"] == 1
