"""Metadata database management for chapter tracking and versioning."""

import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional

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
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

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
                    chapters_count INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # Add chapters_count column if missing (migration for existing databases)
            try:
                conn.execute(text("ALTER TABLE version_registry ADD COLUMN chapters_count INTEGER DEFAULT 0"))
            except Exception:
                pass  # Column may already exist

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

        # Run ingestion_log schema migration
        self.migrate_ingestion_log_schema()

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
            if result:
                session.expunge(result)
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
                    existing.ingestion_timestamp = datetime.now(timezone.utc)
                    existing.source_file_path = chapter.source_file_path
                    session.commit()
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
        version_id: Optional[str] = None,
        operation_type: Optional[str] = None,
        chunks_before: Optional[int] = None,
        chunks_after: Optional[int] = None,
        duration_ms: Optional[int] = None,
        metadata_json: Optional[str] = None,
        diff_json: Optional[str] = None,
    ) -> None:
        """Log an ingestion operation.

        Args:
            chapter_id: ID of chapter metadata record
            status: Ingestion status (e.g., "success", "failure")
            parsing_strategy: Strategy used for parsing
            error_message: Error message if failed
            chunks_inserted: Number of chunks successfully inserted
            version_id: Optional version identifier for this operation
            operation_type: Optional type of operation (insert, update, delete)
            chunks_before: Optional count of chunks before operation
            chunks_after: Optional count of chunks after operation
            duration_ms: Optional duration in milliseconds
            metadata_json: Optional metadata as JSON string
            diff_json: Optional diff information as JSON string
        """
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO ingestion_log
                    (chapter_id, timestamp, status, parsing_strategy, error_message,
                     chunks_inserted, version_id, operation_type, chunks_before,
                     chunks_after, duration_ms, metadata_json, diff_json)
                    VALUES (:chapter_id, :timestamp, :status, :parsing_strategy,
                            :error_message, :chunks_inserted, :version_id,
                            :operation_type, :chunks_before, :chunks_after,
                            :duration_ms, :metadata_json, :diff_json)
                """),
                {
                    "chapter_id": chapter_id,
                    "timestamp": datetime.now().isoformat(),
                    "status": status,
                    "parsing_strategy": parsing_strategy,
                    "error_message": error_message,
                    "chunks_inserted": chunks_inserted,
                    "version_id": version_id,
                    "operation_type": operation_type,
                    "chunks_before": chunks_before,
                    "chunks_after": chunks_after,
                    "duration_ms": duration_ms,
                    "metadata_json": metadata_json,
                    "diff_json": diff_json,
                },
            )
            conn.commit()

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

    def migrate_ingestion_log_schema(self) -> None:
        """Migrate ingestion_log table to add versioning columns."""
        with self.engine.connect() as conn:
            # Add new columns to ingestion_log table
            try:
                conn.execute(text("ALTER TABLE ingestion_log ADD COLUMN version_id TEXT"))
            except Exception:
                pass  # Column may already exist

            try:
                conn.execute(text("ALTER TABLE ingestion_log ADD COLUMN operation_type TEXT"))
            except Exception:
                pass

            try:
                conn.execute(text("ALTER TABLE ingestion_log ADD COLUMN chunks_before INTEGER"))
            except Exception:
                pass

            try:
                conn.execute(text("ALTER TABLE ingestion_log ADD COLUMN chunks_after INTEGER"))
            except Exception:
                pass

            try:
                conn.execute(text("ALTER TABLE ingestion_log ADD COLUMN duration_ms INTEGER"))
            except Exception:
                pass

            try:
                conn.execute(text("ALTER TABLE ingestion_log ADD COLUMN metadata_json TEXT"))
            except Exception:
                pass

            try:
                conn.execute(text("ALTER TABLE ingestion_log ADD COLUMN diff_json TEXT"))
            except Exception:
                pass

            conn.commit()

    def create_snapshot(self, version_id: str) -> None:
        """Create a snapshot of the current chapter_metadata table.

        Args:
            version_id: Version identifier for this snapshot
        """
        # Baseline uses existing table, no copy needed
        if "baseline" in version_id:
            return

        with self.engine.connect() as conn:
            # Create snapshot table
            snapshot_table = f"chapter_metadata_{version_id}"
            conn.execute(
                text(f"CREATE TABLE IF NOT EXISTS {snapshot_table} AS SELECT * FROM chapter_metadata")
            )

            # Insert snapshot record into version_registry
            conn.execute(
                text("""
                    INSERT OR IGNORE INTO version_registry
                    (version_id, timestamp, status, description, sqlite_snapshot)
                    VALUES (:version_id, :timestamp, 'pending', :description, :sqlite_snapshot)
                """),
                {
                    "version_id": version_id,
                    "timestamp": datetime.now().isoformat(),
                    "description": f"Snapshot created for {version_id}",
                    "sqlite_snapshot": snapshot_table,
                },
            )

            conn.commit()

    def restore_snapshot(self, version_id: str) -> None:
        """Restore chapter_metadata from a snapshot.

        Args:
            version_id: Version identifier to restore from
        """
        snapshot_table = f"chapter_metadata_{version_id}"

        with self.engine.connect() as conn:
            # Check if snapshot table exists
            result = conn.execute(
                text("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name=:table_name
                """),
                {"table_name": snapshot_table},
            ).fetchone()

            if not result:
                raise ValueError(f"Snapshot table {snapshot_table} does not exist")

            # Restore data from snapshot
            conn.execute(text("DELETE FROM chapter_metadata"))
            conn.execute(
                text(f"INSERT INTO chapter_metadata SELECT * FROM {snapshot_table}")
            )

            # Update version_registry status
            conn.execute(
                text("""
                    UPDATE version_registry
                    SET status='restored', updated_at=:updated_at
                    WHERE version_id=:version_id
                """),
                {
                    "updated_at": datetime.now().isoformat(),
                    "version_id": version_id,
                },
            )

            conn.commit()

    def delete_snapshot(self, version_id: str) -> None:
        """Delete a snapshot table.

        Args:
            version_id: Version identifier to delete

        Raises:
            ValueError: If attempting to delete baseline version
        """
        # Prevent deletion of baseline
        if "baseline" in version_id:
            raise ValueError("Cannot delete baseline version")

        snapshot_table = f"chapter_metadata_{version_id}"

        with self.engine.connect() as conn:
            # Drop snapshot table
            conn.execute(text(f"DROP TABLE IF EXISTS {snapshot_table}"))

            # Update version_registry status
            conn.execute(
                text("""
                    UPDATE version_registry
                    SET status='archived', updated_at=:updated_at
                    WHERE version_id=:version_id
                """),
                {
                    "updated_at": datetime.now().isoformat(),
                    "version_id": version_id,
                },
            )

            conn.commit()

    def list_snapshots(self) -> List[str]:
        """List all snapshot version IDs.

        Returns:
            List of version identifiers for existing snapshots
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name LIKE 'chapter_metadata_%'
                      AND name != 'chapter_metadata'
                    ORDER BY name
                """)
            ).fetchall()

            # Extract version_ids from table names
            version_ids = [
                row[0].replace("chapter_metadata_", "") for row in result
            ]

            return version_ids

    def register_version(
        self,
        version_id: str,
        description: str,
        sqlite_snapshot: str = "",
        qdrant_snapshot: str = "",
        neo4j_snapshot: str = "",
        chapters_count: int = 0,
    ) -> None:
        """Register a new version in the version registry.

        Args:
            version_id: Unique version identifier
            description: Description of this version
            sqlite_snapshot: Path or name of SQLite snapshot
            qdrant_snapshot: Path or name of Qdrant snapshot
            neo4j_snapshot: Path or name of Neo4j snapshot
            chapters_count: Number of chapters in this version

        Example:
            >>> db.register_version(
            ...     'v2_minor_20251225_120000',
            ...     'Added new chapter from Schwartz',
            ...     sqlite_snapshot='chapter_metadata_v2_minor_20251225_120000',
            ...     qdrant_snapshot='textbook_chunks_v2',
            ...     neo4j_snapshot='v2_minor_20251225_120000'
            ... )
        """
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO version_registry
                    (version_id, timestamp, status, description, sqlite_snapshot, qdrant_snapshot, neo4j_snapshot, chapters_count)
                    VALUES (:version_id, :timestamp, 'pending', :description, :sqlite_snapshot, :qdrant_snapshot, :neo4j_snapshot, :chapters_count)
                """),
                {
                    "version_id": version_id,
                    "timestamp": datetime.now().isoformat(),
                    "description": description,
                    "sqlite_snapshot": sqlite_snapshot,
                    "qdrant_snapshot": qdrant_snapshot,
                    "neo4j_snapshot": neo4j_snapshot,
                    "chapters_count": chapters_count,
                },
            )
            conn.commit()

    def update_version_status(self, version_id: str, status: str) -> None:
        """Update the status of a version.

        Args:
            version_id: Version identifier to update
            status: New status (pending, staging, validating, committed, rolled_back, archived)

        Example:
            >>> db.update_version_status('v2_minor_20251225_120000', 'committed')
        """
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE version_registry
                    SET status=:status, updated_at=:updated_at
                    WHERE version_id=:version_id
                """),
                {
                    "status": status,
                    "updated_at": datetime.now().isoformat(),
                    "version_id": version_id,
                },
            )
            conn.commit()

    def update_version_chapters_count(self, version_id: str, chapters_count: int) -> None:
        """Update the chapters count for a version.

        Args:
            version_id: Version identifier to update
            chapters_count: Number of chapters in this version

        Example:
            >>> db.update_version_chapters_count('v2_minor_20251225_120000', 5)
        """
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE version_registry
                    SET chapters_count=:chapters_count, updated_at=:updated_at
                    WHERE version_id=:version_id
                """),
                {
                    "chapters_count": chapters_count,
                    "updated_at": datetime.now().isoformat(),
                    "version_id": version_id,
                },
            )
            conn.commit()

    def log_operation(
        self,
        version_id: str,
        operation_type: str,
        system: str,
        entity_type: str,
        entity_id: str,
        status: str,
        error_message: Optional[str] = None,
        duration_ms: Optional[int] = None,
        diff_json: Optional[str] = None,
        metadata_json: Optional[str] = None,
    ) -> None:
        """Log an operation in the operation log.

        Args:
            version_id: Version identifier this operation belongs to
            operation_type: Type of operation (insert, update, delete, snapshot, restore)
            system: System where operation occurred (sqlite, qdrant, neo4j)
            entity_type: Type of entity (chapter, paragraph, collection, label)
            entity_id: Identifier of the entity
            status: Operation status (success, failure, partial)
            error_message: Error message if operation failed
            duration_ms: Duration in milliseconds
            diff_json: JSON string containing diff information
            metadata_json: JSON string containing additional metadata

        Example:
            >>> db.log_operation(
            ...     version_id='v2_minor_20251225_120000',
            ...     operation_type='insert',
            ...     system='qdrant',
            ...     entity_type='paragraph',
            ...     entity_id='bailey:ch60:60.1.1',
            ...     status='success',
            ...     duration_ms=150
            ... )
        """
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO operation_log
                    (version_id, timestamp, operation_type, system, entity_type, entity_id,
                     status, error_message, duration_ms, diff_json, metadata_json)
                    VALUES (:version_id, :timestamp, :operation_type, :system, :entity_type, :entity_id,
                            :status, :error_message, :duration_ms, :diff_json, :metadata_json)
                """),
                {
                    "version_id": version_id,
                    "timestamp": datetime.now().isoformat(),
                    "operation_type": operation_type,
                    "system": system,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "status": status,
                    "error_message": error_message,
                    "duration_ms": duration_ms,
                    "diff_json": diff_json,
                    "metadata_json": metadata_json,
                },
            )
            conn.commit()

    def get_version_history(self, limit: int = 10) -> List[Dict]:
        """Get version history from the registry.

        Args:
            limit: Maximum number of versions to return

        Returns:
            List of dictionaries containing version information

        Example:
            >>> history = db.get_version_history(limit=5)
            >>> for version in history:
            ...     print(f"{version['version_id']}: {version['status']}")
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT * FROM version_registry
                    ORDER BY timestamp DESC
                    LIMIT :limit
                """),
                {"limit": limit},
            ).fetchall()

            # Convert rows to dictionaries
            if result:
                columns = result[0]._fields
                return [dict(zip(columns, row)) for row in result]
            return []

    def get_latest_version(self) -> Optional[str]:
        """Get the latest committed version ID.

        Returns:
            Version ID of the latest committed version, or None if no committed versions exist

        Example:
            >>> latest = db.get_latest_version()
            >>> print(f"Latest committed version: {latest}")
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT version_id FROM version_registry
                    WHERE status='committed'
                    ORDER BY timestamp DESC
                    LIMIT 1
                """)
            ).fetchone()

            return result[0] if result else None
