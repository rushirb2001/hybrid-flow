"""Metadata database management for chapter tracking and versioning."""

import hashlib
from datetime import datetime
from typing import Dict, Optional

from sqlalchemy import create_engine, func, text
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

        # Create versioning infrastructure tables
        with self.engine.connect() as conn:
            # Create version_registry table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS version_registry (
                    version_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    description TEXT,
                    commit_hash TEXT,
                    sqlite_snapshot TEXT,
                    qdrant_snapshot TEXT,
                    neo4j_snapshot TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # Create operation_log table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS operation_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    system TEXT NOT NULL,
                    entity_type TEXT,
                    entity_id TEXT,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    duration_ms INTEGER,
                    diff_json TEXT,
                    metadata_json TEXT,
                    FOREIGN KEY (version_id) REFERENCES version_registry(version_id)
                )
            """))

            # Create indexes for operation_log
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_operation_log_version ON operation_log(version_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_operation_log_timestamp ON operation_log(timestamp)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_operation_log_status ON operation_log(status)"))

            conn.commit()

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

    def register_baseline_version(
        self, description: str = "Initial baseline from existing data"
    ) -> str:
        """Register existing data as baseline version v1.

        Args:
            description: Description of the baseline version

        Returns:
            version_id: The generated baseline version identifier
        """
        # Generate baseline version_id with timestamp
        version_id = f"v1_baseline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        with self.engine.connect() as conn:
            # Insert baseline version into version_registry
            conn.execute(
                text("""
                    INSERT INTO version_registry
                    (version_id, timestamp, status, description, sqlite_snapshot, qdrant_snapshot, neo4j_snapshot)
                    VALUES (:version_id, :timestamp, 'committed', :description, 'baseline', 'baseline', 'baseline')
                """),
                {
                    "version_id": version_id,
                    "timestamp": datetime.now().isoformat(),
                    "description": description,
                },
            )

            # Add baseline_version_id column to chapter_metadata
            conn.execute(
                text("ALTER TABLE chapter_metadata ADD COLUMN baseline_version_id TEXT")
            )

            # Update all existing chapters with baseline version_id
            conn.execute(
                text("UPDATE chapter_metadata SET baseline_version_id = :version_id"),
                {"version_id": version_id},
            )

            # Commit transaction
            conn.commit()

        return version_id
