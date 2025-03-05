"""Metadata database management for chapter tracking and versioning."""

import hashlib
from datetime import datetime
from typing import Dict, Optional

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from hybridflow.models import Chapter
from hybridflow.storage.models import Base, ChapterMetadata, IngestionLog


class MetadataDatabase:
    """Manages metadata storage for chapter versioning and quality tracking."""

    def __init__(self, database_path: str):
        """Initialize the metadata database.

        Args:
            database_path: Path to SQLite database file
        """
        self.engine = create_engine(f"sqlite:///{database_path}")
        self.session_factory = sessionmaker(bind=self.engine)

    def create_tables(self) -> None:
        """Create all database tables if they don't exist."""
        Base.metadata.create_all(self.engine)

    def get_chapter_by_id(
        self, textbook_id: str, chapter_number: str
    ) -> Optional[ChapterMetadata]:
        """Retrieve chapter metadata by textbook ID and chapter number.

        Args:
            textbook_id: Textbook identifier (bailey, sabiston, schwartz)
            chapter_number: Chapter number

        Returns:
            ChapterMetadata object if found, None otherwise
        """
        session = self.session_factory()
        try:
            result = (
                session.query(ChapterMetadata)
                .filter_by(textbook_id=textbook_id, chapter_number=chapter_number)
                .first()
            )
            return result
        finally:
            session.close()

    def upsert_chapter(self, chapter: Chapter) -> ChapterMetadata:
        """Insert or update chapter metadata with version tracking.

        Args:
            chapter: Chapter object from Pydantic models

        Returns:
            ChapterMetadata object (newly created or updated)
        """
        session = self.session_factory()
        try:
            # Compute content hash
            content_hash = hashlib.sha256(
                chapter.model_dump_json().encode()
            ).hexdigest()

            # Query existing chapter
            existing = (
                session.query(ChapterMetadata)
                .filter_by(
                    textbook_id=chapter.textbook_id.value,
                    chapter_number=chapter.chapter_number,
                )
                .first()
            )

            if existing:
                # Check if content changed
                if existing.content_hash != content_hash:
                    # Content changed, increment version
                    existing.version += 1
                    existing.content_hash = content_hash
                    existing.title = chapter.title
                    existing.ingestion_timestamp = datetime.utcnow()
                    existing.source_file_path = chapter.source_file_path
                    session.commit()
                    session.refresh(existing)
                    # Make the object accessible outside the session
                    session.expunge(existing)
                    return existing
                else:
                    # Content unchanged, return existing
                    session.expunge(existing)
                    return existing
            else:
                # Create new chapter metadata
                new_chapter = ChapterMetadata(
                    textbook_id=chapter.textbook_id.value,
                    chapter_number=chapter.chapter_number,
                    title=chapter.title,
                    content_hash=content_hash,
                    version=1,
                    source_file_path=chapter.source_file_path,
                )
                session.add(new_chapter)
                session.commit()
                session.refresh(new_chapter)
                session.expunge(new_chapter)
                return new_chapter
        finally:
            session.close()

    def log_ingestion(
        self,
        chapter_id: int,
        status: str,
        parsing_strategy: str,
        error_message: Optional[str],
        chunks_inserted: int,
    ) -> None:
        """Log an ingestion operation.

        Args:
            chapter_id: ID of chapter metadata record
            status: Ingestion status (e.g., "success", "failure")
            parsing_strategy: Strategy used for parsing
            error_message: Error message if failed
            chunks_inserted: Number of chunks successfully inserted
        """
        session = self.session_factory()
        try:
            log_entry = IngestionLog(
                chapter_id=chapter_id,
                status=status,
                parsing_strategy=parsing_strategy,
                error_message=error_message,
                chunks_inserted=chunks_inserted,
            )
            session.add(log_entry)
            session.commit()
        finally:
            session.close()

    def get_aggregate_stats(self) -> Dict:
        """Compute aggregate statistics across all chapters.

        Returns:
            Dictionary containing:
                - total_chapters: Total number of chapters
                - chapters_by_textbook: Dict mapping textbook_id to count
                - average_quality_score: Average quality score (or None)
                - last_ingestion_timestamp: Most recent ingestion timestamp
        """
        session = self.session_factory()
        try:
            # Total chapters
            total_chapters = session.query(func.count(ChapterMetadata.id)).scalar()

            # Chapters by textbook
            textbook_counts = (
                session.query(
                    ChapterMetadata.textbook_id, func.count(ChapterMetadata.id)
                )
                .group_by(ChapterMetadata.textbook_id)
                .all()
            )
            chapters_by_textbook = {tb: count for tb, count in textbook_counts}

            # Average quality score
            avg_quality = session.query(
                func.avg(ChapterMetadata.quality_score)
            ).scalar()

            # Last ingestion timestamp
            last_ingestion = session.query(
                func.max(ChapterMetadata.ingestion_timestamp)
            ).scalar()

            return {
                "total_chapters": total_chapters or 0,
                "chapters_by_textbook": chapters_by_textbook,
                "average_quality_score": avg_quality,
                "last_ingestion_timestamp": last_ingestion,
            }
        finally:
            session.close()
