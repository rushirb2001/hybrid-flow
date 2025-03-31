"""Ingestion pipeline orchestrating all storage and processing components."""

import hashlib
import json
import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from hybridflow.models import Chapter
from hybridflow.parsing.chunk_generator import ChunkGenerator
from hybridflow.parsing.embedder import EmbeddingGenerator
from hybridflow.storage.metadata_db import MetadataDatabase
from hybridflow.storage.neo4j_client import Neo4jStorage
from hybridflow.storage.qdrant_client import QdrantStorage
from hybridflow.storage.version_manager import VersionManager
from hybridflow.validation.loader import JSONLoader


class IngestionTransaction:
    """Transaction context manager for ingestion operations with validation."""

    def __init__(self, pipeline: "IngestionPipeline", description: str = "") -> None:
        """Initialize ingestion transaction.

        Args:
            pipeline: The parent IngestionPipeline instance
            description: Optional description of this transaction
        """
        self.pipeline = pipeline
        self.description = description
        self.version_id = None
        self.safety_backup_id = None
        self.started = False
        self.committed = False
        self.operations = []

    def __enter__(self) -> "IngestionTransaction":
        """Start the transaction and generate version ID.

        Uses copy-on-write approach for incremental updates:
        1. Create safety backup for disaster recovery
        2. Copy current production data to new staging collection
        3. New/modified chapters get upserted to staging
        4. On commit, staging becomes new production

        Returns:
            Self for context manager usage
        """
        # Create safety backup before starting transaction
        self.safety_backup_id = self.pipeline._create_safety_backup()
        self.version_id = self.pipeline._generate_version_id("staging")

        # Copy current production data to staging (for incremental updates)
        # This ensures skipped (unchanged) chapters are preserved
        self.pipeline.logger.info(f"Initializing staging environment: {self.version_id}")
        copied_counts = self.pipeline._copy_production_to_staging(self.version_id)
        self.pipeline.logger.info(
            f"Copied {copied_counts.get('qdrant', 0)} Qdrant points, "
            f"{copied_counts.get('neo4j', 0)} Neo4j nodes to staging"
        )

        # Register the staging version in version_registry
        self.pipeline.metadata_db.register_version(
            version_id=self.version_id,
            description=self.description or f"Staging transaction {self.version_id}",
            sqlite_snapshot=f"chapter_metadata_{self.version_id}",
            qdrant_snapshot=f"textbook_chunks_{self.version_id}",
            neo4j_snapshot=self.version_id,
        )
        # Update status to staging
        self.pipeline.metadata_db.update_version_status(self.version_id, "staging")

        self.started = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit transaction with validation and commit/rollback.

        Args:
            exc_type: Exception type if raised
            exc_val: Exception value if raised
            exc_tb: Exception traceback if raised

        Returns:
            False to propagate exceptions, True to suppress
        """
        # Handle exceptions - rollback and propagate
        if exc_type is not None:
            try:
                self.pipeline._rollback_version(self.version_id, error=str(exc_val))
            except Exception as rollback_error:
                # Catastrophic failure - restore from safety backup
                self.pipeline.logger.error(
                    f"Rollback failed, restoring from safety backup: {rollback_error}"
                )
                self.pipeline._restore_from_safety_backup(self.safety_backup_id)
            return False

        # Count successful chapter ingestions
        successful_chapters = sum(
            1 for op in self.operations
            if op["type"] == "ingest_chapter" and op["status"] == "success"
        )

        # If no chapters were actually ingested (all skipped), skip validation and commit
        if successful_chapters == 0:
            self.pipeline.logger.info(
                f"No chapters ingested in transaction {self.version_id}, skipping commit"
            )
            # Update version status to indicate no-op
            self.pipeline.metadata_db.update_version_status(self.version_id, "skipped")
            # Cleanup safety backup
            self.pipeline._cleanup_safety_backup(self.safety_backup_id)
            return False

        # Validate ingestion before committing
        validation = self.pipeline._validate_ingestion(self.version_id)
        if validation["status"] == "pass":
            # Update chapters count before committing
            self.pipeline.metadata_db.update_version_chapters_count(
                self.version_id, successful_chapters
            )
            self.pipeline._commit_version(self.version_id)
            self.committed = True
            # Cleanup safety backup on successful commit
            self.pipeline._cleanup_safety_backup(self.safety_backup_id)
        else:
            try:
                self.pipeline._rollback_version(self.version_id, error=validation["errors"])
            except Exception as rollback_error:
                # Catastrophic failure - restore from safety backup
                self.pipeline.logger.error(
                    f"Rollback failed, restoring from safety backup: {rollback_error}"
                )
                self.pipeline._restore_from_safety_backup(self.safety_backup_id)
            raise ValueError(f"Validation failed: {validation['errors']}")

        return False

    def track_operation(self, operation_type: str, entity_id: str, status: str) -> None:
        """Track an operation within this transaction.

        Args:
            operation_type: Type of operation (e.g., 'upsert_chapter', 'generate_embedding')
            entity_id: Identifier for the entity being operated on
            status: Status of the operation ('success', 'failed', 'pending')
        """
        self.operations.append(
            {
                "type": operation_type,
                "entity": entity_id,
                "status": status,
                "timestamp": datetime.now().isoformat(),
            }
        )


class IngestionPipeline:
    """Orchestrates ingestion of chapters into hybrid storage backend."""

    def __init__(
        self,
        qdrant_host: str,
        qdrant_port: int,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        metadata_db_path: str,
        embedding_model: str,
        qdrant_collection_name: str = "textbook_chunks",
        vector_size: int = 768,
    ) -> None:
        """Initialize the ingestion pipeline.

        Args:
            qdrant_host: Qdrant server host
            qdrant_port: Qdrant server port
            neo4j_uri: Neo4j database URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            metadata_db_path: Path to SQLite metadata database
            embedding_model: Sentence transformer model name
            qdrant_collection_name: Qdrant collection name (default: textbook_chunks)
            vector_size: Dimensionality of embedding vectors (default: 768)
        """
        # Initialize storage clients
        self.qdrant_storage = QdrantStorage(
            host=qdrant_host, port=qdrant_port, collection_name=qdrant_collection_name,
            vector_size=vector_size
        )
        self.neo4j_storage = Neo4jStorage(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)
        self.metadata_db = MetadataDatabase(database_path=metadata_db_path)

        # Initialize database schemas
        self.qdrant_storage.create_collection()
        self.neo4j_storage.create_constraints()
        self.metadata_db.create_tables()

        # Initialize processing components
        self.loader = JSONLoader()
        self.chunk_generator = ChunkGenerator()
        self.embedder = EmbeddingGenerator(model_name=embedding_model)

        # Set up logging
        self.logger = logging.getLogger(__name__)

    @property
    def version_manager(self) -> VersionManager:
        """Get or create the VersionManager for coordinated version operations.

        Returns:
            VersionManager instance
        """
        if not hasattr(self, "_version_manager"):
            self._version_manager = VersionManager(
                self.metadata_db, self.qdrant_storage, self.neo4j_storage
            )
        return self._version_manager

    def list_versions(self) -> List[Dict]:
        """List all available versions.

        Returns:
            List of version dictionaries
        """
        return self.version_manager.list_versions()

    def validate_system(self) -> Dict:
        """Validate data consistency across all storage systems.

        Returns:
            Dictionary with validation results
        """
        return self.version_manager.validate_all_systems()

    def rotate_old_versions(self, keep_count: int = 5) -> Dict:
        """Rotate old versions by deleting versions beyond keep_count.

        Args:
            keep_count: Number of recent versions to keep

        Returns:
            Dictionary with deleted, skipped, and remaining counts
        """
        return self.version_manager.rotate_versions(keep_count)

    def ingest_chapter(
        self, file_path: str, force: bool = False, version_id: Optional[str] = None
    ) -> Dict:
        """Ingest a single chapter from JSON file into all storage backends.

        Uses batch operations for Neo4j to dramatically improve performance.
        Previously: ~50-100+ individual session operations per chapter
        Now: ~5-6 batch operations per chapter (one session each)

        Args:
            file_path: Path to chapter JSON file
            force: If True, force re-ingestion even if content unchanged
            version_id: Optional version ID for staged ingestion (e.g., 'staging_20250319')
                       Note: Currently limited by Neo4j unique constraints on node IDs.
                       Works best with fresh/non-conflicting data.

        Returns:
            Dict with status, chunks_inserted count, duration_ms, and version_id

        Note:
            When version_id is provided, all Neo4j nodes receive the version label
            and Qdrant points are stored in a versioned collection. This enables
            isolated staging of data before committing to production.
        """
        # Record start time for performance tracking
        start_time = datetime.now()

        try:
            # Load and parse chapter
            chapter = self.loader.parse_chapter(file_path)

            # Check if chapter already exists and is unchanged
            content_hash = hashlib.sha256(chapter.model_dump_json().encode()).hexdigest()
            existing = self.metadata_db.get_chapter_by_id(
                chapter.textbook_id.value, chapter.chapter_number
            )

            if existing and existing.content_hash == content_hash and not force:
                self.logger.info(
                    f"Chapter {chapter.textbook_id.value}:{chapter.chapter_number} unchanged, skipping"
                )
                return {"status": "skipped", "chunks_inserted": 0}

            # Generate chunks from chapter hierarchy
            chunks = self.chunk_generator.generate_chunks(chapter)

            if not chunks:
                self.logger.warning(f"No chunks generated for {file_path}")
                return {"status": "no_chunks", "chunks_inserted": 0}

            # Extract texts for batch embedding generation
            chunk_texts = [paragraph.text for _, paragraph, _ in chunks]
            embeddings = self.embedder.generate_batch_embeddings(chunk_texts)

            # Prepare data for batch Neo4j operations
            textbook_name_map = {
                "bailey": "Bailey & Love's Short Practice of Surgery",
                "sabiston": "Sabiston Textbook of Surgery",
                "schwartz": "Schwartz's Principles of Surgery",
            }
            version = existing.version + 1 if existing else 1
            chapter_id = f"{chapter.textbook_id.value}:ch{chapter.chapter_number}"

            # Collect unique sections, subsections, subsubsections
            sections_data = {}
            subsections_data = {}
            subsubsections_data = {}

            # Collect paragraphs, tables, figures for batch upsert
            paragraphs_data = []
            tables_data = []
            figures_data = []

            # Prepare Qdrant chunks for batch upsert
            qdrant_chunks = []

            # Process each chunk - collect data for batch operations
            for (chunk_id, paragraph, hierarchy_path), embedding in zip(chunks, embeddings):
                # Build full hierarchy path for metadata
                metadata = {
                    "textbook_id": chapter.textbook_id.value,
                    "chapter_number": chapter.chapter_number,
                    "chapter_title": chapter.title,
                    "hierarchy_path": " > ".join(hierarchy_path),
                    "page": paragraph.page,
                    "version_id": version_id if version_id else "production",
                }

                # Determine parent ID based on paragraph number structure
                parts = paragraph.number.split(".")

                # Find matching section/subsection/subsubsection
                section = None
                subsection = None
                subsubsection = None

                for sec in chapter.sections:
                    if sec.number == parts[0]:
                        section = sec
                        break

                if section and len(parts) >= 2:
                    for subsec in section.subsections:
                        if subsec.number == f"{parts[0]}.{parts[1]}":
                            subsection = subsec
                            break

                if subsection and len(parts) >= 3:
                    for subsubsec in subsection.subsubsections:
                        if subsubsec.number == f"{parts[0]}.{parts[1]}.{parts[2]}":
                            subsubsection = subsubsec
                            break

                # Collect hierarchy data (deduplicated by ID)
                if section:
                    section_id = f"{chapter_id}:s{section.number}"
                    if section_id not in sections_data:
                        sections_data[section_id] = {
                            'chapter_id': chapter_id,
                            'number': section.number,
                            'title': section.title,
                        }

                    parent_id = section_id

                    if subsection:
                        subsection_id = f"{section_id}:ss{subsection.number}"
                        if subsection_id not in subsections_data:
                            subsections_data[subsection_id] = {
                                'section_id': section_id,
                                'number': subsection.number,
                                'title': subsection.title,
                            }

                        parent_id = subsection_id

                        if subsubsection:
                            subsubsection_id = f"{subsection_id}:sss{subsubsection.number}"
                            if subsubsection_id not in subsubsections_data:
                                subsubsections_data[subsubsection_id] = {
                                    'subsection_id': subsection_id,
                                    'number': subsubsection.number,
                                    'title': subsubsection.title,
                                }

                            parent_id = subsubsection_id

                    # Extract cross-references from paragraph text
                    cross_references = self.chunk_generator.extract_references(paragraph.text)

                    # Collect paragraph data
                    paragraphs_data.append({
                        'parent_id': parent_id,
                        'paragraph_number': paragraph.number,
                        'text': paragraph.text,
                        'chunk_id': chunk_id,
                        'page': paragraph.page,
                        'bounds': [
                            paragraph.bounds.x1,
                            paragraph.bounds.y1,
                            paragraph.bounds.x2,
                            paragraph.bounds.y2,
                        ],
                        'cross_references': cross_references,
                    })

                    # Collect tables if present
                    if paragraph.tables:
                        for table in paragraph.tables:
                            tables_data.append({
                                'paragraph_chunk_id': chunk_id,
                                'table_number': table.table_number,
                                'description': table.description,
                                'page': table.page,
                                'bounds': [
                                    table.bounds.x1,
                                    table.bounds.y1,
                                    table.bounds.x2,
                                    table.bounds.y2,
                                ],
                            })

                    # Collect figures if present
                    if paragraph.figures:
                        for figure in paragraph.figures:
                            figures_data.append({
                                'paragraph_chunk_id': chunk_id,
                                'figure_number': figure.figure_number,
                                'caption': figure.caption,
                                'page': figure.page,
                                'bounds': [
                                    figure.bounds.x1,
                                    figure.bounds.y1,
                                    figure.bounds.x2,
                                    figure.bounds.y2,
                                ],
                            })

                # Collect for Qdrant batch upsert
                # Use versioned chunk_id to avoid UUID collisions when same content is staged
                versioned_chunk_id = f"{chunk_id}::{version_id}" if version_id else chunk_id
                qdrant_chunks.append((versioned_chunk_id, paragraph.text, metadata, embedding))

            # Execute batch Neo4j operations
            # 1. Batch upsert hierarchy (textbook, chapter, sections, subsections, subsubsections)
            self.neo4j_storage.batch_upsert_hierarchy(
                textbook_id=chapter.textbook_id.value,
                textbook_name=textbook_name_map.get(chapter.textbook_id.value, chapter.textbook_id.value),
                chapter_number=chapter.chapter_number,
                chapter_title=chapter.title,
                chapter_version=version,
                sections=list(sections_data.values()),
                subsections=list(subsections_data.values()),
                subsubsections=list(subsubsections_data.values()),
                version_id=version_id,
            )

            # 2. Batch upsert paragraphs
            self.neo4j_storage.batch_upsert_paragraphs(
                paragraphs=paragraphs_data,
                version_id=version_id,
            )

            # 3. Batch upsert tables
            if tables_data:
                self.neo4j_storage.batch_upsert_tables(
                    tables=tables_data,
                    version_id=version_id,
                )

            # 4. Batch upsert figures
            if figures_data:
                self.neo4j_storage.batch_upsert_figures(
                    figures=figures_data,
                    version_id=version_id,
                )

            # 5. Batch upsert to Qdrant
            if qdrant_chunks:
                self.qdrant_storage.upsert_chunks(qdrant_chunks, version_id=version_id)

            # Upsert chapter metadata (metadata DB doesn't use versioning)
            self.metadata_db.upsert_chapter(chapter)

            # 6. Link sequential paragraphs with NEXT/PREV relationships
            links_created = self.neo4j_storage.link_sequential_paragraphs(
                chapter_id, version_id=version_id
            )

            # Calculate duration
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

            # Log operation if version_id provided
            if version_id:
                self.metadata_db.log_operation(
                    version_id,
                    "ingest_chapter",
                    "pipeline",
                    "chapter",
                    chapter_id,
                    "success",
                    duration_ms=duration_ms,
                )

            self.logger.info(
                f"Successfully ingested {chapter.textbook_id.value}:{chapter.chapter_number} "
                f"with {len(chunks)} chunks and {links_created} sequential links "
                f"in {duration_ms}ms"
                + (f" (version: {version_id})" if version_id else "")
            )

            return {
                "status": "success",
                "chunks_inserted": len(chunks),
                "duration_ms": duration_ms,
                "version_id": version_id,
            }

        except Exception as e:
            self.logger.error(f"Failed to ingest {file_path}: {e}")
            return {"status": "failed", "chunks_inserted": 0, "error": str(e)}

    def ingest_directory(self, directory_path: str, force: bool = False) -> Dict:
        """Ingest all JSON files from a directory.

        Args:
            directory_path: Path to directory containing chapter JSON files
            force: If True, force re-ingestion even if content unchanged

        Returns:
            Summary dict with total_files, successful_count, failed_count, skipped_count
        """
        directory = Path(directory_path)
        json_files = sorted(directory.glob("*.json"))

        results = {
            "total_files": len(json_files),
            "successful_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
        }

        for json_file in json_files:
            result = self.ingest_chapter(str(json_file), force=force)

            if result["status"] == "success":
                results["successful_count"] += 1
            elif result["status"] == "skipped":
                results["skipped_count"] += 1
            else:
                results["failed_count"] += 1

        self.logger.info(
            f"Directory ingestion complete: {results['successful_count']} succeeded, "
            f"{results['skipped_count']} skipped, {results['failed_count']} failed"
        )

        return results

    def ingest_chapter_transactional(
        self, file_path: str, description: str = "", force: bool = False
    ) -> Dict:
        """Ingest a single chapter with full transaction safety.

        Creates a transaction with safety backup, validates after ingestion,
        and commits if validation passes or rolls back if it fails.

        Args:
            file_path: Path to chapter JSON file
            description: Optional description of this transaction
            force: If True, force re-ingestion even if content unchanged

        Returns:
            Dict with ingestion results, version_id, and committed status
        """
        result = None
        with IngestionTransaction(self, description) as txn:
            result = self.ingest_chapter(file_path, force=force, version_id=txn.version_id)
            txn.track_operation("ingest_chapter", file_path, result["status"])
        # Return after __exit__ is called so txn.committed is correctly set
        return {**result, "version_id": txn.version_id, "committed": txn.committed}

    def ingest_directory_transactional(
        self, directory_path: str, description: str = "", force: bool = False, validate_every: int = 0
    ) -> Dict:
        """Ingest all chapters from a directory with full transaction safety.

        Creates a single transaction for the entire directory. If any chapter fails,
        the entire batch is rolled back.

        Args:
            directory_path: Path to directory containing chapter JSON files
            description: Optional description of this transaction
            force: If True, force re-ingestion even if content unchanged
            validate_every: If > 0, run validation every N successful ingestions.
                           Catches errors early in long batches. Default 0 (no interim validation).

        Returns:
            Dict with aggregated results, version_id, and committed status
        """
        import glob
        import os

        results = {"total": 0, "success": 0, "failed": 0, "skipped": 0}
        with IngestionTransaction(self, description) as txn:
            # Get all JSON files
            files = sorted(glob.glob(os.path.join(directory_path, "*.json")))

            # Ingest each file
            for file_path in files:
                try:
                    result = self.ingest_chapter(
                        file_path, force=force, version_id=txn.version_id
                    )
                    results["total"] += 1

                    if result["status"] == "success":
                        results["success"] += 1
                    elif result["status"] == "skipped":
                        results["skipped"] += 1
                    else:
                        results["failed"] += 1

                    txn.track_operation("ingest_chapter", file_path, result["status"])

                    # Incremental validation checkpoint
                    if validate_every > 0 and results["success"] % validate_every == 0:
                        self.logger.info(
                            f"Incremental validation checkpoint at {results['success']} chapters"
                        )
                        interim_validation = self._validate_ingestion(txn.version_id)
                        if interim_validation["status"] != "pass":
                            raise ValueError(
                                f"Interim validation failed at chapter {results['success']}: "
                                f"{interim_validation['errors']}"
                            )

                except Exception as e:
                    results["failed"] += 1
                    txn.track_operation("ingest_chapter", file_path, "failed")
                    self.logger.error(f"Failed to ingest {file_path}: {e}")
                    raise

            self.logger.info(
                f"Directory ingestion complete: {results['success']} succeeded, "
                f"{results['skipped']} skipped, {results['failed']} failed"
            )

        # Return after __exit__ is called so txn.committed is correctly set
        return {**results, "version_id": txn.version_id, "committed": txn.committed}

    def ingest_all_transactional(
        self, data_dir: str = "data", description: str = "", force: bool = False, validate_every: int = 0
    ) -> Dict:
        """Ingest all textbooks with full transaction safety.

        Creates a single transaction for all textbooks. If any chapter fails,
        the entire ingestion is rolled back.

        Args:
            data_dir: Path to data directory containing textbook subdirectories
            description: Optional description of this transaction
            force: If True, force re-ingestion even if content unchanged
            validate_every: If > 0, run validation every N successful ingestions.
                           Catches errors early in long batches. Default 0 (no interim validation).

        Returns:
            Dict with per-textbook results, version_id, and committed status
        """
        import glob
        import os

        results = {"textbooks": {}, "total_chapters": 0, "total_chunks": 0, "successful_chapters": 0}
        with IngestionTransaction(self, description) as txn:
            # Loop through textbook directories
            for textbook in ["bailey", "sabiston", "schwartz"]:
                textbook_path = os.path.join(data_dir, textbook)

                if os.path.exists(textbook_path):
                    files = sorted(glob.glob(os.path.join(textbook_path, "*.json")))
                    results["textbooks"][textbook] = {"files": len(files), "success": 0}

                    # Ingest each file with version_id
                    for file_path in files:
                        result = self.ingest_chapter(
                            file_path, force=force, version_id=txn.version_id
                        )

                        if result["status"] == "success":
                            results["textbooks"][textbook]["success"] += 1
                            results["total_chunks"] += result.get("chunks_inserted", 0)
                            results["successful_chapters"] += 1

                        txn.track_operation("ingest_chapter", file_path, result["status"])
                        results["total_chapters"] += 1

                        # Incremental validation checkpoint
                        if validate_every > 0 and results["successful_chapters"] % validate_every == 0:
                            self.logger.info(
                                f"Incremental validation checkpoint at {results['successful_chapters']} chapters"
                            )
                            interim_validation = self._validate_ingestion(txn.version_id)
                            if interim_validation["status"] != "pass":
                                raise ValueError(
                                    f"Interim validation failed at chapter {results['successful_chapters']}: "
                                    f"{interim_validation['errors']}"
                                )

                    self.logger.info(
                        f"Completed {textbook}: {results['textbooks'][textbook]['success']}/{len(files)}"
                    )

        # Return after __exit__ is called so txn.committed is correctly set
        return {**results, "version_id": txn.version_id, "committed": txn.committed}

    def _generate_version_id(self, prefix: str = "v") -> str:
        """Generate a unique version ID with timestamp.

        Args:
            prefix: Prefix for the version ID (default: 'v')

        Returns:
            Formatted version ID string (e.g., 'v20250318_143025')
        """
        return f"{prefix}{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def _copy_production_to_staging(self, version_id: str) -> Dict[str, int]:
        """Copy current production data to staging for incremental updates.

        This enables incremental ingestion where:
        - Unchanged chapters are preserved (already in staging from copy)
        - New/modified chapters are upserted (overwrites in staging)
        - On commit, staging has complete data set

        Args:
            version_id: Target staging version ID

        Returns:
            Dict with counts of copied items per system
        """
        counts = {"qdrant": 0, "neo4j": 0}

        # Copy Qdrant data from production to staging
        try:
            qdrant_count = self.qdrant_storage.create_snapshot(version_id, show_progress=False)
            counts["qdrant"] = qdrant_count
        except Exception as e:
            # No existing data to copy (first ingestion) - this is OK
            self.logger.debug(f"No Qdrant data to copy: {e}")

        # Copy Neo4j data from current version to staging version
        try:
            neo4j_count = self.neo4j_storage.copy_to_staging(version_id)
            counts["neo4j"] = neo4j_count
        except Exception as e:
            # No existing data to copy (first ingestion) - this is OK
            self.logger.debug(f"No Neo4j data to copy: {e}")

        return counts

    def _create_staging_environment(self, version_id: str) -> Dict[str, bool]:
        """Create staging environment across all storage systems.

        Args:
            version_id: The version ID for this staging environment

        Returns:
            Dict with success status for each storage system

        Raises:
            RuntimeError: If staging creation fails
        """
        results = {"sqlite": False, "qdrant": False, "neo4j": False}

        # Create SQLite staging snapshot
        try:
            self.metadata_db.create_snapshot(version_id)
            results["sqlite"] = True
        except Exception as e:
            raise RuntimeError(f"SQLite staging failed: {e}")

        # Create Qdrant staging collection
        try:
            self.qdrant_storage.create_snapshot(version_id, show_progress=False)
            results["qdrant"] = True
        except Exception as e:
            self.metadata_db.delete_snapshot(version_id)
            raise RuntimeError(f"Qdrant staging failed: {e}")

        # Neo4j staging uses labels during upsert (no pre-creation needed)
        results["neo4j"] = True

        # Update version registry with Qdrant and Neo4j snapshot info
        # (SQLite snapshot was already registered by create_snapshot)
        with self.metadata_db.engine.connect() as conn:
            from sqlalchemy import text

            conn.execute(
                text("""
                    UPDATE version_registry
                    SET qdrant_snapshot = :qdrant_snapshot,
                        neo4j_snapshot = :neo4j_snapshot,
                        description = :description,
                        updated_at = :updated_at
                    WHERE version_id = :version_id
                """),
                {
                    "qdrant_snapshot": f"textbook_chunks_{version_id}",
                    "neo4j_snapshot": f"v{version_id}",
                    "description": f"Staging environment for {version_id}",
                    "updated_at": datetime.now().isoformat(),
                    "version_id": version_id,
                },
            )
            conn.commit()

        return results

    def _cleanup_staging_environment(self, version_id: str) -> None:
        """Cleanup staging environment artifacts.

        Args:
            version_id: The version ID to cleanup
        """
        # Delete all staging artifacts (ignore failures)
        try:
            self.metadata_db.delete_snapshot(version_id)
        except:
            pass

        try:
            self.qdrant_storage.delete_snapshot(version_id)
        except:
            pass

        try:
            self.neo4j_storage.delete_snapshot(version_id)
        except:
            pass

    def _validate_ingestion(self, version_id: str) -> Dict[str, Any]:
        """Validate ingestion for a specific version.

        For staging versions (copy-on-write), handles the case where:
        - Qdrant staging contains copied production data + new data
        - Neo4j staging only has new versioned data (production wasn't versioned)

        In this case, validation compares NEW data only by subtracting
        production count from Qdrant staging count.

        Args:
            version_id: The version ID to validate

        Returns:
            Validation result dict with 'status', 'checks', and 'errors'
        """
        # Initialize validation report
        report = {
            "version_id": version_id,
            "status": "pending",
            "checks": {},
            "errors": [],
        }

        # Get SQLite validation
        sqlite_snapshots = self.metadata_db.list_snapshots()
        report["checks"]["sqlite_snapshot_exists"] = version_id in sqlite_snapshots or any(
            version_id in s for s in sqlite_snapshots
        )

        # Get Qdrant validation
        qdrant_validation = self.qdrant_storage.validate_collection(version_id)
        report["checks"]["qdrant_valid"] = qdrant_validation["status"] == "valid"
        report["checks"]["qdrant_point_count"] = qdrant_validation["point_count"]

        # Get Neo4j validation
        neo4j_validation = self.neo4j_storage.validate_graph(version_id)
        report["checks"]["neo4j_valid"] = neo4j_validation["status"] == "valid"
        report["checks"]["neo4j_paragraph_count"] = neo4j_validation["node_counts"].get(
            "Paragraph", 0
        )

        # Cross-system consistency check
        qdrant_count = report["checks"]["qdrant_point_count"]
        neo4j_count = report["checks"]["neo4j_paragraph_count"]

        # For staging versions, count only points with matching version_id in payload
        # This handles copy-on-write where production points are copied but keep "production" version_id
        if version_id.startswith("staging"):
            # Count points with this specific version_id in their payload
            qdrant_versioned_count = self._count_qdrant_by_version(version_id)
            report["checks"]["qdrant_versioned_count"] = qdrant_versioned_count

            # Compare versioned data counts
            report["checks"]["counts_match"] = qdrant_versioned_count == neo4j_count

            if not report["checks"]["counts_match"]:
                report["errors"].append(
                    f"Count mismatch: Qdrant versioned={qdrant_versioned_count}, Neo4j={neo4j_count} "
                    f"(Qdrant staging total={qdrant_count})"
                )
        else:
            # Non-staging version: direct comparison
            report["checks"]["counts_match"] = qdrant_count == neo4j_count

            if not report["checks"]["counts_match"]:
                report["errors"].append(
                    f"Count mismatch: Qdrant={qdrant_count}, Neo4j={neo4j_count}"
                )

        # Check for orphan paragraphs
        if neo4j_validation.get("orphan_paragraphs", 0) > 0:
            report["errors"].append(
                f"Found {neo4j_validation['orphan_paragraphs']} orphan paragraphs"
            )

        # Check for broken chains
        if neo4j_validation.get("broken_next_chains", 0) > 0:
            report["errors"].append(
                f"Found {neo4j_validation['broken_next_chains']} broken NEXT chains"
            )

        # Set final status
        report["status"] = "pass" if len(report["errors"]) == 0 else "fail"

        # Log validation to operation_log
        self.metadata_db.log_operation(
            version_id,
            "validate",
            "pipeline",
            "version",
            version_id,
            report["status"],
            error_message=str(report["errors"]) if report["errors"] else None,
        )

        return report

    def _count_qdrant_by_version(self, version_id: str) -> int:
        """Count Qdrant points that have a specific version_id in their payload.

        Args:
            version_id: The version ID to filter by

        Returns:
            Count of points matching the version_id
        """
        return self.qdrant_storage.count_by_version(version_id)

    def _commit_version(self, version_id: str) -> None:
        """Commit a validated version to production.

        Args:
            version_id: The version ID to commit
        """
        # Update version status to committing
        self.metadata_db.update_version_status(version_id, "committing")

        # Metadata DB is intentionally unversioned - already updated in place during ingestion
        # No snapshot restore needed

        # Update Qdrant alias to point to new version
        self.qdrant_storage.restore_snapshot(version_id)

        # For Neo4j, nodes already created with versioned IDs during ingestion
        # No additional action needed for commit

        # Rotate old versions
        self._rotate_versions()

        # Update version status to committed
        self.metadata_db.update_version_status(version_id, "committed")

        # Log commit operation
        self.metadata_db.log_operation(
            version_id, "commit", "pipeline", "version", version_id, "success"
        )

        self.logger.info(f"Successfully committed version {version_id}")

    def _check_version_in_use(self, version_id: str) -> bool:
        """Check if a version is currently in use.

        Args:
            version_id: Version identifier to check

        Returns:
            bool: True if version is in use, False if safe to delete
        """
        try:
            history = self.metadata_db.get_version_history(limit=100)
            version_record = next(
                (v for v in history if v["version_id"] == version_id), None
            )

            if not version_record:
                return False

            # Check for active statuses
            active_statuses = ["in_use", "pending", "committing", "staging"]
            if version_record.get("status") in active_statuses:
                return True

            return False

        except Exception as e:
            self.logger.warning(f"Error checking version {version_id}: {e}")
            return True  # Assume in use for safety

    def _rotate_versions(self, keep_count: int = 5, force: bool = False) -> List[str]:
        """Rotate old versions by archiving versions beyond keep_count.

        Args:
            keep_count: Number of committed versions to keep (default: 5)
            force: If True, delete versions even if in use (default: False)

        Returns:
            List[str]: Version IDs that were successfully deleted
        """
        deleted = []
        skipped = []
        # Get version history
        history = self.metadata_db.get_version_history(limit=100)

        # Filter committed versions excluding baseline
        committed = [
            v
            for v in history
            if v["status"] == "committed" and "baseline" not in v["version_id"]
        ]

        # If more than keep_count, delete oldest
        if len(committed) > keep_count:
            to_delete = committed[keep_count:]
            for version in to_delete:
                version_id = version["version_id"]

                # Check if in use before deleting (unless force=True)
                if not force:
                    active_queries = self._check_version_in_use(version["version_id"])
                    if active_queries:
                        skipped.append(version["version_id"])
                        continue

                self.logger.info(f"Archiving old version: {version_id}")

                # Delete snapshots from all storage systems (ignore errors for missing snapshots)
                try:
                    self.metadata_db.delete_snapshot(version_id)
                except Exception as e:
                    self.logger.warning(f"Failed to delete SQLite snapshot {version_id}: {e}")

                try:
                    self.qdrant_storage.delete_snapshot(version_id)
                except Exception as e:
                    self.logger.warning(f"Failed to delete Qdrant snapshot {version_id}: {e}")

                try:
                    self.neo4j_storage.delete_snapshot(version_id)
                except Exception as e:
                    self.logger.warning(f"Failed to delete Neo4j snapshot {version_id}: {e}")

                # Update version status to archived
                self.metadata_db.update_version_status(version_id, "archived")
                deleted.append(version_id)

        # Log summary
        if deleted:
            self.logger.info(f"Deleted {len(deleted)} old versions: {deleted}")
        if skipped:
            self.logger.info(f"Skipped {len(skipped)} versions in use: {skipped}")

        return deleted

    def _rollback_version(self, version_id: str, error: Any = None) -> None:
        """Rollback a failed version and cleanup.

        Args:
            version_id: The version ID to rollback
            error: Error message or details (can be string or list)
        """
        # Convert error to string if it's a list or other type
        error_str = str(error) if error is not None else None

        # Update status to rolling_back
        self.metadata_db.update_version_status(version_id, "rolling_back")

        # Log rollback start
        self.metadata_db.log_operation(
            version_id,
            "rollback_start",
            "pipeline",
            "version",
            version_id,
            "pending",
            error_message=error_str,
        )

        self.logger.warning(f"Rolling back version {version_id}: {error_str}")

        # Clean up SQLite staging
        try:
            self.metadata_db.delete_snapshot(version_id)
            self.logger.info(f"Cleaned up SQLite snapshot for {version_id}")
        except Exception as e:
            self.logger.error(f"Failed to clean up SQLite snapshot: {e}")
            self.metadata_db.log_operation(
                version_id,
                "rollback_sqlite",
                "sqlite",
                "snapshot",
                version_id,
                "failed",
                error_message=str(e),
            )

        # Clean up Qdrant staging
        try:
            self.qdrant_storage.delete_snapshot(version_id)
            self.logger.info(f"Cleaned up Qdrant collection for {version_id}")
        except Exception as e:
            self.logger.error(f"Failed to clean up Qdrant collection: {e}")
            self.metadata_db.log_operation(
                version_id,
                "rollback_qdrant",
                "qdrant",
                "collection",
                version_id,
                "failed",
                error_message=str(e),
            )

        # Clean up Neo4j staging nodes
        try:
            self.neo4j_storage.delete_snapshot(version_id)
            self.logger.info(f"Cleaned up Neo4j nodes for {version_id}")
        except Exception as e:
            self.logger.error(f"Failed to clean up Neo4j nodes: {e}")
            self.metadata_db.log_operation(
                version_id,
                "rollback_neo4j",
                "neo4j",
                "nodes",
                version_id,
                "failed",
                error_message=str(e),
            )

        # Update version status to rolled_back
        self.metadata_db.update_version_status(version_id, "rolled_back")

        # Log rollback complete
        self.metadata_db.log_operation(
            version_id, "rollback_complete", "pipeline", "version", version_id, "success"
        )

        self.logger.info(f"Successfully rolled back version {version_id}")

    def _create_safety_backup(self) -> str:
        """Create a safety backup of the current state before transaction.

        Returns:
            Backup version ID
        """
        # Generate backup version ID
        backup_id = self._generate_version_id("latest_copy")

        self.logger.info(f"Creating safety backup: {backup_id}")

        # Create SQLite backup
        self.metadata_db.create_snapshot(backup_id)

        # Create Qdrant snapshot (full copy for safety)
        self.qdrant_storage.create_snapshot(backup_id, show_progress=False)

        # For Neo4j, add :latest_copy label to current nodes (lightweight operation)
        with self.neo4j_storage.driver.session() as session:
            result = session.run("MATCH (n) WHERE NOT n:latest_copy SET n:latest_copy")
            session.run("RETURN 1")  # Ensure transaction completes

        # Update version registry with Qdrant and Neo4j snapshot info
        # (SQLite snapshot was already registered by create_snapshot)
        with self.metadata_db.engine.connect() as conn:
            from sqlalchemy import text

            conn.execute(
                text("""
                    UPDATE version_registry
                    SET qdrant_snapshot = :qdrant_snapshot,
                        neo4j_snapshot = :neo4j_snapshot,
                        description = :description,
                        updated_at = :updated_at
                    WHERE version_id = :version_id
                """),
                {
                    "qdrant_snapshot": f"textbook_chunks_{backup_id}",
                    "neo4j_snapshot": "latest_copy",
                    "description": "Safety backup before staging",
                    "updated_at": datetime.now().isoformat(),
                    "version_id": backup_id,
                },
            )
            conn.commit()

        self.logger.info(f"Safety backup created: {backup_id}")
        return backup_id

    def _restore_from_safety_backup(self, backup_id: str) -> None:
        """Restore from safety backup in case of catastrophic failure.

        Args:
            backup_id: The safety backup version ID
        """
        self.logger.warning(f"Restoring from safety backup: {backup_id}")

        # Restore SQLite
        self.metadata_db.restore_snapshot(backup_id)

        # Restore Qdrant by updating alias
        self.qdrant_storage.restore_snapshot(backup_id)

        # Neo4j restore: remove non-backup nodes and clear latest_copy label
        with self.neo4j_storage.driver.session() as session:
            # Remove nodes that don't have the latest_copy label
            session.run("MATCH (n) WHERE NOT n:latest_copy DETACH DELETE n")
            # Clear the latest_copy label from remaining nodes
            session.run("MATCH (n:latest_copy) REMOVE n:latest_copy")

        # Log restoration
        self.metadata_db.log_operation(
            backup_id, "restore_safety", "pipeline", "version", backup_id, "success"
        )

        self.logger.info(f"Successfully restored from safety backup: {backup_id}")

    def _cleanup_safety_backup(self, backup_id: str) -> None:
        """Cleanup safety backup after successful transaction.

        Args:
            backup_id: The safety backup version ID
        """
        self.logger.info(f"Cleaning up safety backup: {backup_id}")

        # Remove backup artifacts
        try:
            self.metadata_db.delete_snapshot(backup_id)
        except Exception as e:
            self.logger.warning(f"Failed to delete SQLite backup: {e}")

        try:
            self.qdrant_storage.delete_snapshot(backup_id)
        except Exception as e:
            self.logger.warning(f"Failed to delete Qdrant backup: {e}")

        # Remove latest_copy label from Neo4j nodes
        try:
            with self.neo4j_storage.driver.session() as session:
                session.run("MATCH (n:latest_copy) REMOVE n:latest_copy")
        except Exception as e:
            self.logger.warning(f"Failed to remove Neo4j backup labels: {e}")

        self.logger.info(f"Safety backup cleanup complete: {backup_id}")

    def close(self) -> None:
        """Close all storage client connections."""
        self.neo4j_storage.close()
