"""Qdrant vector database client for semantic search."""

import sqlite3
import uuid
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import qdrant_client
import qdrant_client.models as qmodels


class QdrantStorage:
    """Manages vector storage and semantic search using Qdrant."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6333,
        collection_name: str = "textbook_chunks",
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
        version_suffix: Optional[str] = None,
    ):
        """Initialize the Qdrant client.

        Args:
            host: Qdrant server host (default: localhost)
            port: Qdrant server port (default: 6333)
            collection_name: Base name of the vector collection (default: textbook_chunks)
            embedding_model: Embedding model identifier (default: sentence-transformers/all-MiniLM-L6-v2)
            version_suffix: Optional version suffix to create versioned collection instances.
                           If provided, collection_name becomes '{collection_name}_{version_suffix}'.
                           This allows multiple versions of the same collection to coexist.
                           Example: version_suffix='v2_test' creates collection 'textbook_chunks_v2_test'
        """
        self.client = qdrant_client.QdrantClient(host=host, port=port)
        self.version_suffix = version_suffix
        self.collection_name = (
            f"{collection_name}_{version_suffix}" if version_suffix else collection_name
        )

    def create_collection(self) -> None:
        """Create the collection if it doesn't exist.

        Uses 384-dimensional vectors (sentence-transformers/all-MiniLM-L6-v2 default)
        with cosine distance metric for semantic similarity.
        """
        if not self.client.collection_exists(self.collection_name):
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=qmodels.VectorParams(
                    size=384, distance=qmodels.Distance.COSINE
                ),
            )

    def upsert_chunks(
        self,
        chunks: List[Tuple[str, str, dict, List[float]]],
        version_id: Optional[str] = None,
    ) -> None:
        """Insert or update chunks in the vector database.

        Args:
            chunks: List of tuples containing:
                - chunk_id: Unique identifier for the chunk
                - text: Text content of the chunk
                - metadata: Additional metadata (e.g., chapter_id, page, etc.)
                - embedding: 384-dimensional vector embedding
            version_id: Optional version identifier to upsert into a specific versioned collection.
                       If None, upserts to the current instance's collection.
                       If provided, creates the versioned collection if it doesn't exist.
                       Example: version_id='v2_test' upserts to 'textbook_chunks_v2_test'

        Example:
            >>> # Upsert to main collection (backward compatible)
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> storage.upsert_chunks([chunk1, chunk2])
            >>>
            >>> # Upsert to versioned collection
            >>> storage.upsert_chunks([chunk1, chunk2], version_id='v2_minor')
        """
        # Determine target collection
        target_collection = (
            self._get_versioned_collection_name(version_id)
            if version_id
            else self.collection_name
        )

        # When version_id provided, ensure target collection exists
        if version_id:
            try:
                self.client.get_collection(target_collection)
            except Exception:
                # Create collection with same config as base collection
                self.client.create_collection(
                    collection_name=target_collection,
                    vectors_config=qmodels.VectorParams(
                        size=384, distance=qmodels.Distance.COSINE
                    ),
                )

        # Create points (unchanged logic)
        points = []
        for chunk_id, text, metadata, embedding in chunks:
            # Generate deterministic UUID from chunk_id
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_id))

            point = qmodels.PointStruct(
                id=point_id,
                vector=embedding,
                payload={"chunk_id": chunk_id, "text": text, **metadata},
            )
            points.append(point)

        # Upsert to target collection
        self.client.upsert(collection_name=target_collection, points=points)

    def search_similar(
        self, query_vector: List[float], limit: int = 10
    ) -> List[Tuple[str, float]]:
        """Search for similar chunks using vector similarity.

        Args:
            query_vector: Query embedding vector
            limit: Maximum number of results to return

        Returns:
            List of tuples containing (chunk_id, similarity_score)
        """
        results = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=limit,
        )

        return [(hit.payload["chunk_id"], hit.score) for hit in results.points]

    def delete_chunks(self, chunk_ids: List[str]) -> None:
        """Delete chunks from the vector database.

        Args:
            chunk_ids: List of chunk IDs to delete
        """
        # Convert chunk_ids to point_ids using deterministic UUIDs
        point_ids = [str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_id)) for chunk_id in chunk_ids]

        self.client.delete(
            collection_name=self.collection_name,
            points_selector=qmodels.PointIdsList(points=point_ids),
        )

    def get_collection_info(self) -> Dict:
        """Get collection statistics and information.

        Returns:
            Dictionary containing:
                - points_count: Number of vectors in collection
                - vector_size: Dimensionality of vectors
        """
        collection_info = self.client.get_collection(
            collection_name=self.collection_name
        )

        return {
            "points_count": collection_info.points_count,
            "vector_size": collection_info.config.params.vectors.size,
        }

    def register_baseline_collection(self) -> str:
        """Register the current collection as baseline with an alias.

        Creates an alias '{collection_name}_latest' pointing to the current collection,
        allowing the collection to serve as the baseline for versioning.

        Returns:
            str: "baseline" to indicate current collection serves as baseline

        Raises:
            Exception: If the collection does not exist

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> baseline = storage.register_baseline_collection()
            >>> print(f'Baseline: {baseline}')
            Baseline: baseline
        """
        # Verify current collection exists
        try:
            self.client.get_collection(self.collection_name)
        except Exception as e:
            raise Exception(
                f"Collection '{self.collection_name}' does not exist. "
                f"Cannot register as baseline. Error: {e}"
            )

        # Create alias for baseline collection
        alias_name = f"{self.collection_name}_latest"
        self.client.update_collection_aliases(
            change_aliases_operations=[
                qmodels.CreateAliasOperation(
                    create_alias=qmodels.CreateAlias(
                        collection_name=self.collection_name, alias_name=alias_name
                    )
                )
            ]
        )

        # Verify alias creation
        info = self.client.get_collection(alias_name)
        # If we successfully retrieved the collection via alias, it's working
        if info.points_count is None:
            raise Exception(f"Alias verification failed for '{alias_name}'")

        return "baseline"

    def _get_versioned_collection_name(self, version_id: Optional[str] = None) -> str:
        """Get the collection name for a specific version.

        Args:
            version_id: Optional version identifier (e.g., 'v2_test', 'v3_minor_20251225')
                       If None, returns the current instance's collection_name

        Returns:
            str: Versioned collection name in format 'textbook_chunks_{version_id}'
                or current collection_name if version_id is None

        Example:
            >>> storage = QdrantStorage()
            >>> storage._get_versioned_collection_name('v2_test')
            'textbook_chunks_v2_test'
            >>> storage._get_versioned_collection_name()
            'textbook_chunks'
        """
        if version_id:
            return f"textbook_chunks_{version_id}"
        return self.collection_name

    def _get_base_collection_name(self) -> str:
        """Get the base collection name without any version suffix.

        Returns:
            str: Base collection name 'textbook_chunks' regardless of version suffix

        Example:
            >>> storage = QdrantStorage(version_suffix='v2_test')
            >>> storage._get_base_collection_name()
            'textbook_chunks'
        """
        return "textbook_chunks"

    def create_snapshot(self, version_id: str, show_progress: bool = True) -> int:
        """Create a snapshot of the current collection by copying to a new versioned collection.

        WARNING: This operation can take several minutes for large collections (e.g., 36K vectors).
        The entire collection is copied point-by-point with full vectors and payloads.

        Args:
            version_id: Version identifier for the snapshot (e.g., 'v2_snapshot_test')
            show_progress: If True, print progress updates every 1000 points

        Returns:
            int: Number of points copied to the snapshot collection

        Raises:
            Exception: If source collection doesn't exist or copy verification fails

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> count = storage.create_snapshot('v2_minor_20251225', show_progress=True)
            >>> print(f'Created snapshot with {count} points')
        """
        # Baseline uses existing collection, no copy needed
        if "baseline" in version_id:
            return 0

        # Get source and target collection names
        source_collection = self.collection_name
        target_collection = self._get_versioned_collection_name(version_id)

        # Check if target already exists
        try:
            existing_info = self.client.get_collection(target_collection)
            if show_progress:
                print(
                    f"Snapshot collection '{target_collection}' already exists "
                    f"with {existing_info.points_count} points"
                )
            return existing_info.points_count
        except Exception:
            pass  # Collection doesn't exist, proceed with creation

        # Get source collection info to copy configuration
        source_info = self.client.get_collection(source_collection)

        # Create target collection with same configuration
        self.client.create_collection(
            collection_name=target_collection,
            vectors_config=source_info.config.params.vectors,
        )

        # Initialize progress tracking
        total_points = source_info.points_count
        copied_points = 0
        batch_size = 100

        if show_progress:
            print(
                f"Creating snapshot '{target_collection}' from '{source_collection}' "
                f"({total_points} points)"
            )

        # Use scroll API to iterate through all points
        offset = None
        while True:
            result, offset = self.client.scroll(
                collection_name=source_collection,
                limit=batch_size,
                offset=offset,
                with_payload=True,
                with_vectors=True,
            )

            if not result:
                break

            # Convert Record objects to PointStruct for upsert
            points_to_upsert = []
            for record in result:
                point = qmodels.PointStruct(
                    id=record.id, vector=record.vector, payload=record.payload
                )
                points_to_upsert.append(point)

            # Upsert batch to target collection
            self.client.upsert(collection_name=target_collection, points=points_to_upsert)

            # Update progress
            copied_points += len(result)
            if show_progress and copied_points % 1000 == 0:
                print(f"Progress: {copied_points}/{total_points} points copied")

            # Break when all points processed
            if offset is None:
                break

        # Verify copy completed
        target_info = self.client.get_collection(target_collection)
        if target_info.points_count != source_info.points_count:
            raise Exception(
                f"Snapshot verification failed: "
                f"Expected {source_info.points_count} points, "
                f"got {target_info.points_count} points"
            )

        if show_progress:
            print(
                f"Snapshot complete: {target_info.points_count} points "
                f"copied to '{target_collection}'"
            )

        return target_info.points_count

    def restore_snapshot(self, version_id: str) -> None:
        """Restore a snapshot by updating the 'latest' alias to point to the versioned collection.

        This makes the specified version the active collection without deleting the current one.
        The previous 'latest' collection is preserved and can be restored if needed.

        Args:
            version_id: Version identifier to restore (e.g., 'v2_snapshot_test')
                       Use 'baseline' to restore the original baseline collection

        Raises:
            Exception: If the versioned collection doesn't exist

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> storage.restore_snapshot('v2_minor_20251225')
            >>> # Now 'textbook_chunks_latest' points to 'textbook_chunks_v2_minor_20251225'
        """
        # Handle baseline restoration
        if "baseline" in version_id:
            versioned_collection = self._get_base_collection_name()
        else:
            versioned_collection = self._get_versioned_collection_name(version_id)

        # Verify versioned collection exists
        self.client.get_collection(versioned_collection)

        # Get current latest collection for backup alias
        try:
            current_latest = self.client.get_collection("textbook_chunks_latest")
            # Create backup alias for previous latest
            self.client.update_collection_aliases(
                change_aliases_operations=[
                    qmodels.CreateAliasOperation(
                        create_alias=qmodels.CreateAlias(
                            collection_name=current_latest.collection_name
                            if hasattr(current_latest, "collection_name")
                            else self._get_base_collection_name(),
                            alias_name="textbook_chunks_previous",
                        )
                    )
                ]
            )
        except Exception:
            pass  # No previous latest alias exists

        # Update 'latest' alias to point to versioned collection
        self.client.update_collection_aliases(
            change_aliases_operations=[
                qmodels.CreateAliasOperation(
                    create_alias=qmodels.CreateAlias(
                        collection_name=versioned_collection,
                        alias_name="textbook_chunks_latest",
                    )
                )
            ]
        )

    def delete_snapshot(self, version_id: str) -> None:
        """Delete a snapshot collection.

        WARNING: This permanently deletes the collection and all its data.
        Cannot delete the baseline collection or collections currently in use.

        Args:
            version_id: Version identifier to delete (e.g., 'v2_snapshot_test')

        Raises:
            ValueError: If attempting to delete baseline or collection currently in use
            Exception: If collection doesn't exist

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> storage.delete_snapshot('v2_snapshot_test')
        """
        # Prevent deletion of baseline
        if "baseline" in version_id:
            raise ValueError("Cannot delete baseline collection")

        # Get versioned collection name
        versioned_collection = self._get_versioned_collection_name(version_id)

        # Verify collection exists
        self.client.get_collection(versioned_collection)

        # Check if this collection is currently aliased to latest
        try:
            aliases = self.client.get_collection_aliases(versioned_collection)
            for alias in aliases.aliases:
                if alias.alias_name == "textbook_chunks_latest":
                    raise ValueError(
                        f"Cannot delete collection '{versioned_collection}' "
                        "currently in use (aliased as 'textbook_chunks_latest'). "
                        "Restore a different version first."
                    )
        except Exception as e:
            if "Cannot delete collection" in str(e):
                raise

        # Delete collection
        self.client.delete_collection(collection_name=versioned_collection)

    def list_snapshots(self) -> List[str]:
        """List all available snapshot version IDs.

        Returns:
            List[str]: Sorted list of version IDs for all snapshot collections

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> snapshots = storage.list_snapshots()
            >>> print(f'Available snapshots: {snapshots}')
            ['v2_minor_20251225_120000', 'v2_snapshot_test', 'v3_major_20251226']
        """
        # Get all collections
        collections = self.client.get_collections()

        # Filter for versioned collections
        versioned = [
            c.name
            for c in collections.collections
            if c.name.startswith("textbook_chunks_v")
        ]

        # Extract version IDs by removing prefix
        version_ids = [name.replace("textbook_chunks_", "") for name in versioned]

        # Sort by version ID (which typically contains timestamp)
        version_ids.sort()

        return version_ids

    def create_alias_backup(self, alias_name: str) -> bool:
        """Create an alias backup pointing to the current collection (fast alternative to full copy).

        Creates an alias that points to the current collection without copying data.
        This is much faster than create_snapshot() but provides less isolation.
        Use for quick, temporary backups during short operations.

        Args:
            alias_name: Name for the backup alias (e.g., 'backup_staging_123')

        Returns:
            bool: True if alias created successfully, False otherwise

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> storage.create_alias_backup('backup_quick_test')
            True
            >>> # Alias created instantly, no data copy
        """
        try:
            self.client.update_collection_aliases(
                change_aliases_operations=[
                    qmodels.CreateAliasOperation(
                        create_alias=qmodels.CreateAlias(
                            collection_name=self.collection_name,
                            alias_name=alias_name,
                        )
                    )
                ]
            )
            return True
        except Exception as e:
            print(f"Failed to create alias backup '{alias_name}': {e}")
            return False

    def delete_alias(self, alias_name: str) -> bool:
        """Delete an alias.

        Removes the alias but does not affect the underlying collection.

        Args:
            alias_name: Name of the alias to delete

        Returns:
            bool: True if alias deleted successfully, False otherwise

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> storage.delete_alias('backup_quick_test')
            True
        """
        try:
            self.client.update_collection_aliases(
                change_aliases_operations=[
                    qmodels.DeleteAliasOperation(
                        delete_alias=qmodels.DeleteAlias(alias_name=alias_name)
                    )
                ]
            )
            return True
        except Exception as e:
            print(f"Failed to delete alias '{alias_name}': {e}")
            return False

    def switch_to_alias(self, alias_name: str, target_collection: str) -> bool:
        """Switch an alias to point to a different collection.

        Updates the alias to point to a new target collection.
        Useful for switching between versions without data movement.

        Args:
            alias_name: The alias to update
            target_collection: The collection to point the alias to

        Returns:
            bool: True if alias switched successfully, False otherwise

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> storage.switch_to_alias('textbook_chunks_latest', 'textbook_chunks_v2_test')
            True
        """
        try:
            self.client.update_collection_aliases(
                change_aliases_operations=[
                    qmodels.CreateAliasOperation(
                        create_alias=qmodels.CreateAlias(
                            collection_name=target_collection,
                            alias_name=alias_name,
                        )
                    )
                ]
            )
            return True
        except Exception as e:
            print(f"Failed to switch alias '{alias_name}' to '{target_collection}': {e}")
            return False

    def validate_collection(self, version_id: Optional[str] = None) -> Dict[str, Any]:
        """Validate the health and integrity of a collection.

        Performs the following checks:
        1. Collection exists and is accessible
        2. Points have non-null, non-zero vectors
        3. Required metadata fields are present (chunk_id, text, textbook_id, etc.)
        4. Metadata completeness across sample of points

        Args:
            version_id: Optional version identifier to validate specific version.
                       If None, validates the current instance's collection.

        Returns:
            Dict containing validation report with keys:
            - collection_name: Name of validated collection
            - point_count: Total number of points in collection
            - vector_size: Dimensionality of vectors
            - distance_metric: Distance metric used (COSINE, DOT, EUCLIDEAN)
            - null_vectors: Count of null or zero vectors in sample
            - metadata_completeness: Percentage completeness for each required field
            - status: 'valid' if all checks pass, 'issues_found' otherwise

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> report = storage.validate_collection()
            >>> print(report['status'])
            'valid'
        """
        # Determine collection name
        # Baseline uses the main collection, not a versioned one
        if version_id and "baseline" in version_id:
            collection_name = self.collection_name
        elif version_id:
            collection_name = self._get_versioned_collection_name(version_id)
        else:
            collection_name = self.collection_name

        # Get collection info
        info = self.client.get_collection(collection_name)

        # Extract basic stats
        point_count = info.points_count
        vector_size = info.config.params.vectors.size
        distance_metric = str(info.config.params.vectors.distance)

        # Check for null vectors by scrolling through sample of points
        sample_result, _ = self.client.scroll(
            collection_name=collection_name, limit=100, with_vectors=True
        )
        null_count = sum(
            1
            for p in sample_result
            if p.vector is None or all(v == 0 for v in p.vector)
        )

        # Check metadata completeness by examining payload fields
        metadata_fields = [
            "chunk_id",
            "text",
            "textbook_id",
            "chapter_number",
            "hierarchy_path",
        ]
        missing_fields = {field: 0 for field in metadata_fields}

        for point in sample_result:
            for field in metadata_fields:
                if field not in point.payload:
                    missing_fields[field] += 1

        # Calculate metadata completeness percentage
        completeness = {
            field: 100 * (100 - count) / 100 for field, count in missing_fields.items()
        }

        # Return validation report
        return {
            "collection_name": collection_name,
            "point_count": point_count,
            "vector_size": vector_size,
            "distance_metric": distance_metric,
            "null_vectors": null_count,
            "metadata_completeness": completeness,
            "status": "valid"
            if null_count == 0 and all(v == 100 for v in completeness.values())
            else "issues_found",
        }

    def get_collection_stats(self, version_id: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive statistical analysis of a collection.

        Builds upon validate_collection() to provide additional statistics:
        - Average vector norm (magnitude)
        - Text length statistics (min, max, average)
        - Distribution of chunks across textbooks

        Args:
            version_id: Optional version identifier for specific version stats.
                       If None, analyzes the current instance's collection.

        Returns:
            Dict containing all validation fields plus:
            - avg_vector_norm: Average L2 norm of vectors
            - avg_text_length: Average character length of text payloads
            - min_text_length: Minimum text length found
            - max_text_length: Maximum text length found
            - textbook_distribution: Count of chunks per textbook

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> stats = storage.get_collection_stats()
            >>> print(f"Average text length: {stats['avg_text_length']}")
        """
        # Get validation report as base
        validation = self.validate_collection(version_id)

        # Get sample points for statistical analysis
        collection_name = (
            self._get_versioned_collection_name(version_id)
            if version_id
            else self.collection_name
        )
        sample_result, _ = self.client.scroll(
            collection_name=collection_name, limit=500, with_vectors=True, with_payload=True
        )

        # Calculate average vector norm
        vectors = [p.vector for p in sample_result if p.vector]
        avg_norm = np.mean([np.linalg.norm(v) for v in vectors]) if vectors else 0

        # Calculate text length statistics
        text_lengths = [len(p.payload.get("text", "")) for p in sample_result]
        avg_text_length = np.mean(text_lengths) if text_lengths else 0
        min_text_length = min(text_lengths) if text_lengths else 0
        max_text_length = max(text_lengths) if text_lengths else 0

        # Count chunks by textbook
        textbook_distribution = Counter(
            [
                p.payload.get("textbook_id")
                for p in sample_result
                if "textbook_id" in p.payload
            ]
        )

        # Return comprehensive stats
        return {
            **validation,
            "avg_vector_norm": float(avg_norm),
            "avg_text_length": float(avg_text_length),
            "min_text_length": int(min_text_length),
            "max_text_length": int(max_text_length),
            "textbook_distribution": dict(textbook_distribution),
        }

    def compare_with_metadata(
        self, metadata_db_path: str, version_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Compare Qdrant collection data with metadata database for consistency.

        Performs cross-database validation:
        1. Compares total chunk counts between SQLite and Qdrant
        2. Samples chapters and verifies chunks exist in Qdrant
        3. Reports any discrepancies or missing data

        Args:
            metadata_db_path: Path to SQLite metadata database (e.g., 'metadata.db')
            version_id: Optional version identifier to compare specific version.
                       If None, compares the current instance's collection.

        Returns:
            Dict containing comparison report:
            - sqlite_chapters: Total chapters in metadata database
            - expected_chunks: Sum of chunk_count from metadata
            - actual_chunks: Actual points in Qdrant collection
            - difference: actual_chunks - expected_chunks
            - missing_chunks_in_sample: Count of sampled chapters missing in Qdrant
            - consistency: 'pass' if counts match, 'mismatch' otherwise

        Example:
            >>> storage = QdrantStorage(host='localhost', port=6333)
            >>> comparison = storage.compare_with_metadata('metadata.db')
            >>> if comparison['consistency'] == 'pass':
            ...     print('Databases are consistent')
        """
        # Connect to metadata database
        conn = sqlite3.connect(metadata_db_path)
        cursor = conn.cursor()

        # Get total chapter count
        result = cursor.execute("SELECT COUNT(*) FROM chapter_metadata").fetchone()
        sqlite_count = result[0]

        # Get total expected chunks from metadata
        result = cursor.execute(
            "SELECT SUM(chunk_count) FROM chapter_metadata WHERE chunk_count > 0"
        ).fetchone()
        expected_chunks = result[0] if result[0] else 0

        # Get actual point count from Qdrant
        collection_name = (
            self._get_versioned_collection_name(version_id)
            if version_id
            else self.collection_name
        )
        info = self.client.get_collection(collection_name)
        actual_chunks = info.points_count

        # Sample chunk_ids from metadata DB
        chunk_ids_sample = cursor.execute(
            "SELECT DISTINCT textbook_id, chapter_number FROM chapter_metadata LIMIT 10"
        ).fetchall()

        # For each sample, check if chunks exist in Qdrant
        missing_chunks = 0
        for textbook_id, chapter_number in chunk_ids_sample:
            filter_condition = qmodels.Filter(
                must=[
                    qmodels.FieldCondition(
                        key="textbook_id", match=qmodels.MatchValue(value=textbook_id)
                    ),
                    qmodels.FieldCondition(
                        key="chapter_number",
                        match=qmodels.MatchValue(value=chapter_number),
                    ),
                ]
            )
            result, _ = self.client.scroll(
                collection_name=collection_name, scroll_filter=filter_condition, limit=1
            )
            if not result:
                missing_chunks += 1

        # Close database connection
        conn.close()

        # Return comparison report
        return {
            "sqlite_chapters": sqlite_count,
            "expected_chunks": expected_chunks,
            "actual_chunks": actual_chunks,
            "difference": actual_chunks - expected_chunks,
            "missing_chunks_in_sample": missing_chunks,
            "consistency": "pass" if actual_chunks == expected_chunks else "mismatch",
        }
