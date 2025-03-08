"""Ingestion pipeline orchestrating all storage and processing components."""

import hashlib
import logging
from pathlib import Path
from typing import Dict, List

from hybridflow.models import Chapter
from hybridflow.parsing.chunk_generator import ChunkGenerator
from hybridflow.parsing.embedder import EmbeddingGenerator
from hybridflow.storage.metadata_db import MetadataDatabase
from hybridflow.storage.neo4j_client import Neo4jStorage
from hybridflow.storage.qdrant_client import QdrantStorage
from hybridflow.validation.loader import JSONLoader


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
        """
        # Initialize storage clients
        self.qdrant_storage = QdrantStorage(
            host=qdrant_host, port=qdrant_port, collection_name=qdrant_collection_name
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

    def ingest_chapter(self, file_path: str) -> Dict:
        """Ingest a single chapter from JSON file into all storage backends.

        Args:
            file_path: Path to chapter JSON file

        Returns:
            Dict with status, chunks_inserted count
        """
        try:
            # Load and parse chapter
            chapter = self.loader.parse_chapter(file_path)

            # Check if chapter already exists and is unchanged
            content_hash = hashlib.sha256(chapter.model_dump_json().encode()).hexdigest()
            existing = self.metadata_db.get_chapter_by_id(
                chapter.textbook_id.value, chapter.chapter_number
            )

            if existing and existing.content_hash == content_hash:
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

            # Upsert textbook node in Neo4j
            textbook_name_map = {
                "bailey": "Bailey & Love's Short Practice of Surgery",
                "sabiston": "Sabiston Textbook of Surgery",
                "schwartz": "Schwartz's Principles of Surgery",
            }
            self.neo4j_storage.upsert_textbook(
                textbook_id=chapter.textbook_id.value,
                name=textbook_name_map.get(chapter.textbook_id.value, chapter.textbook_id.value),
            )

            # Upsert chapter node in Neo4j
            version = existing.version + 1 if existing else 1
            self.neo4j_storage.upsert_chapter(
                textbook_id=chapter.textbook_id.value,
                chapter_number=chapter.chapter_number,
                title=chapter.title,
                version=version,
            )

            # Prepare Qdrant chunks for batch upsert
            qdrant_chunks = []

            # Process each chunk
            for (chunk_id, paragraph, hierarchy_path), embedding in zip(chunks, embeddings):
                # Build full hierarchy path for metadata
                metadata = {
                    "textbook_id": chapter.textbook_id.value,
                    "chapter_number": chapter.chapter_number,
                    "chapter_title": chapter.title,
                    "hierarchy_path": " > ".join(hierarchy_path),
                    "page": paragraph.page,
                }

                # Determine parent ID based on paragraph number structure
                # Format: ch2:2.1.1 -> parent could be section, subsection, or subsubsection
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

                # Upsert hierarchy nodes in Neo4j
                if section:
                    chapter_id = f"{chapter.textbook_id.value}:ch{chapter.chapter_number}"
                    section_id = f"{chapter_id}:s{section.number}"
                    self.neo4j_storage.upsert_section(
                        chapter_id=chapter_id,
                        section_number=section.number,
                        title=section.title,
                    )

                    if subsection:
                        subsection_id = f"{chapter_id}:ss{subsection.number}"
                        self.neo4j_storage.upsert_subsection(
                            section_id=section_id,
                            subsection_number=subsection.number,
                            title=subsection.title,
                        )

                        if subsubsection:
                            subsubsection_id = f"{chapter_id}:sss{subsubsection.number}"
                            self.neo4j_storage.upsert_subsubsection(
                                subsection_id=subsection_id,
                                subsubsection_number=subsubsection.number,
                                title=subsubsection.title,
                            )
                            parent_id = subsubsection_id
                        else:
                            parent_id = subsection_id
                    else:
                        parent_id = section_id

                    # Upsert paragraph node
                    self.neo4j_storage.upsert_paragraph(
                        parent_id=parent_id,
                        paragraph_number=paragraph.number,
                        text=paragraph.text,
                        chunk_id=chunk_id,
                        page=paragraph.page,
                        bounds=[
                            paragraph.bounds.x1,
                            paragraph.bounds.y1,
                            paragraph.bounds.x2,
                            paragraph.bounds.y2,
                        ],
                    )

                    # Upsert tables if present
                    if paragraph.tables:
                        for table in paragraph.tables:
                            self.neo4j_storage.upsert_table(
                                paragraph_chunk_id=chunk_id,
                                table_number=table.table_number,
                                description=table.description,
                                page=table.page,
                                bounds=[
                                    table.bounds.x1,
                                    table.bounds.y1,
                                    table.bounds.x2,
                                    table.bounds.y2,
                                ],
                            )

                    # Upsert figures if present
                    if paragraph.figures:
                        for figure in paragraph.figures:
                            self.neo4j_storage.upsert_figure(
                                paragraph_chunk_id=chunk_id,
                                figure_number=figure.figure_number,
                                caption=figure.caption,
                                page=figure.page,
                                bounds=[
                                    figure.bounds.x1,
                                    figure.bounds.y1,
                                    figure.bounds.x2,
                                    figure.bounds.y2,
                                ],
                            )

                # Collect for Qdrant batch upsert
                qdrant_chunks.append((chunk_id, paragraph.text, metadata, embedding))

            # Batch upsert to Qdrant
            if qdrant_chunks:
                self.qdrant_storage.upsert_chunks(qdrant_chunks)

            # Upsert chapter metadata
            self.metadata_db.upsert_chapter(chapter)

            self.logger.info(
                f"Successfully ingested {chapter.textbook_id.value}:{chapter.chapter_number} "
                f"with {len(chunks)} chunks"
            )

            return {"status": "success", "chunks_inserted": len(chunks)}

        except Exception as e:
            self.logger.error(f"Failed to ingest {file_path}: {e}")
            return {"status": "failed", "chunks_inserted": 0, "error": str(e)}

    def ingest_directory(self, directory_path: str) -> Dict:
        """Ingest all JSON files from a directory.

        Args:
            directory_path: Path to directory containing chapter JSON files

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
            result = self.ingest_chapter(str(json_file))

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

    def close(self) -> None:
        """Close all storage client connections."""
        self.neo4j_storage.close()
