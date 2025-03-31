"""Centralized version management coordinator for all storage systems."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .metadata_db import MetadataDatabase
from .neo4j_client import Neo4jStorage
from .qdrant_client import QdrantStorage


class VersionManager:
    """Coordinates version management across metadata DB, Qdrant, and Neo4j."""

    def __init__(
        self,
        metadata_db: MetadataDatabase,
        qdrant_storage: QdrantStorage,
        neo4j_storage: Neo4jStorage,
    ):
        """Initialize VersionManager with storage system references.

        Args:
            metadata_db: MetadataDatabase instance
            qdrant_storage: QdrantStorage instance
            neo4j_storage: Neo4jStorage instance
        """
        self.metadata_db = metadata_db
        self.qdrant = qdrant_storage
        self.neo4j = neo4j_storage
        self.logger = logging.getLogger(__name__)

    def generate_version_id(self, prefix: str = "v") -> str:
        """Generate a unique version ID with timestamp.

        Args:
            prefix: Prefix for version ID (default: "v")

        Returns:
            Version ID in format: {prefix}_YYYYMMDD_HHMMSS
        """
        return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def create_version(self, description: str = "") -> str:
        """Create a new version snapshot across all storage systems.

        Args:
            description: Human-readable description of this version

        Returns:
            The generated version_id

        Raises:
            Exception: If snapshot creation fails in any system
        """
        version_id = self.generate_version_id()
        self.logger.info(f"Creating version: {version_id}")

        try:
            # Create snapshots in all three systems
            self.logger.debug(f"Creating Neo4j snapshot for {version_id}")
            self.neo4j.create_snapshot(version_id)

            self.logger.debug(f"Creating Qdrant snapshot for {version_id}")
            self.qdrant.create_snapshot(version_id)

            self.logger.debug(f"Creating metadata DB snapshot for {version_id}")
            self.metadata_db.create_snapshot(version_id)

            # Register in version registry
            self.logger.debug(f"Registering version {version_id} in registry")
            self.metadata_db.register_version(
                version_id=version_id,
                description=description,
                status="pending",
                chapters_count=0,  # Will be updated during ingestion
            )

            self.logger.info(f"Successfully created version: {version_id}")
            return version_id

        except Exception as e:
            self.logger.error(f"Failed to create version {version_id}: {e}")
            # Attempt rollback of partial creation
            try:
                self.rollback_version(version_id)
            except Exception as rollback_error:
                self.logger.error(f"Rollback failed: {rollback_error}")
            raise

    def commit_version(self, version_id: str) -> bool:
        """Mark a version as committed.

        Args:
            version_id: Version ID to commit

        Returns:
            True if commit successful, False otherwise
        """
        try:
            self.logger.info(f"Committing version: {version_id}")
            self.metadata_db.update_version_status(version_id, "committed")
            return True
        except Exception as e:
            self.logger.error(f"Failed to commit version {version_id}: {e}")
            return False

    def rollback_version(self, version_id: str) -> bool:
        """Rollback a version by deleting all snapshots.

        Args:
            version_id: Version ID to rollback

        Returns:
            True if rollback successful, False otherwise
        """
        try:
            self.logger.info(f"Rolling back version: {version_id}")

            # Delete snapshots from all systems
            self.neo4j.delete_snapshot(version_id)
            self.qdrant.delete_snapshot(version_id)
            self.metadata_db.delete_snapshot(version_id)

            # Update status
            self.metadata_db.update_version_status(version_id, "rolled_back")

            self.logger.info(f"Successfully rolled back version: {version_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to rollback version {version_id}: {e}")
            return False

    def list_versions(self, include_archived: bool = False) -> List[Dict]:
        """List all versions from version registry.

        Args:
            include_archived: If True, include archived and rolled_back versions

        Returns:
            List of version dictionaries
        """
        history = self.metadata_db.get_version_history(limit=100)

        if include_archived:
            return history

        # Filter out archived and rolled_back versions
        return [
            v
            for v in history
            if v["status"] not in ("archived", "rolled_back")
        ]

    def get_version_info(self, version_id: str) -> Optional[Dict]:
        """Get detailed information about a specific version.

        Args:
            version_id: Version ID to query

        Returns:
            Version info dictionary, or None if not found
        """
        history = self.metadata_db.get_version_history(limit=100)
        for version in history:
            if version["version_id"] == version_id:
                return version
        return None

    def get_current_version(self) -> Optional[str]:
        """Get the latest committed version ID.

        Returns:
            Version ID of latest committed version, or None if none exist
        """
        versions = self.list_versions(include_archived=False)

        # Filter to committed versions only
        committed = [v for v in versions if v["status"] == "committed"]

        if not committed:
            return None

        # Return the most recent
        return committed[0]["version_id"]

    def get_version_stats(self, version_id: str) -> Dict[str, Any]:
        """Get aggregated statistics for a version across all systems.

        Args:
            version_id: Version ID to query

        Returns:
            Dictionary with stats from all three systems
        """
        stats = {
            "version_id": version_id,
            "metadata_db": {},
            "qdrant": {},
            "neo4j": {},
        }

        # Get metadata DB stats
        try:
            snapshots = self.metadata_db.list_snapshots()
            snapshot = next((s for s in snapshots if s["version_id"] == version_id), None)
            stats["metadata_db"] = snapshot if snapshot else {"status": "not_found"}
        except Exception as e:
            stats["metadata_db"] = {"error": str(e)}

        # Get Qdrant stats
        try:
            collection_name = f"textbook_chunks_{version_id}"
            validation = self.qdrant.validate_collection(collection_name)
            stats["qdrant"] = validation
        except Exception as e:
            stats["qdrant"] = {"error": str(e)}

        # Get Neo4j stats
        try:
            validation = self.neo4j.validate_graph(version_id=version_id)
            stats["neo4j"] = validation
        except Exception as e:
            stats["neo4j"] = {"error": str(e)}

        return stats

    def compare_versions(self, version_a: str, version_b: str) -> Dict:
        """Compare two versions and show differences.

        Args:
            version_a: First version ID
            version_b: Second version ID

        Returns:
            Dictionary with comparison results
        """
        stats_a = self.get_version_stats(version_a)
        stats_b = self.get_version_stats(version_b)

        comparison = {
            "version_a": version_a,
            "version_b": version_b,
            "qdrant_diff": {},
            "neo4j_diff": {},
        }

        # Compare Qdrant point counts
        try:
            count_a = stats_a["qdrant"].get("point_count", 0)
            count_b = stats_b["qdrant"].get("point_count", 0)
            comparison["qdrant_diff"] = {
                "version_a_count": count_a,
                "version_b_count": count_b,
                "difference": count_b - count_a,
            }
        except Exception as e:
            comparison["qdrant_diff"] = {"error": str(e)}

        # Compare Neo4j node counts
        try:
            nodes_a = stats_a["neo4j"].get("node_counts", {})
            nodes_b = stats_b["neo4j"].get("node_counts", {})

            all_labels = set(nodes_a.keys()) | set(nodes_b.keys())
            node_diff = {}
            for label in all_labels:
                count_a = nodes_a.get(label, 0)
                count_b = nodes_b.get(label, 0)
                node_diff[label] = {
                    "version_a": count_a,
                    "version_b": count_b,
                    "difference": count_b - count_a,
                }

            comparison["neo4j_diff"] = node_diff
        except Exception as e:
            comparison["neo4j_diff"] = {"error": str(e)}

        return comparison

    def delete_version(self, version_id: str, force: bool = False) -> bool:
        """Delete a version from all storage systems.

        Args:
            version_id: Version ID to delete
            force: If True, allow deletion of baseline versions

        Returns:
            True if deletion successful

        Raises:
            ValueError: If attempting to delete baseline without force flag
        """
        # Check if this is a baseline version
        if "baseline" in version_id and not force:
            raise ValueError(
                f"Cannot delete baseline version {version_id} without force=True"
            )

        self.logger.info(f"Deleting version: {version_id}")

        try:
            # Delete from all three systems
            self.metadata_db.delete_snapshot(version_id)
            self.qdrant.delete_snapshot(version_id)
            self.neo4j.delete_snapshot(version_id)

            # Update status to archived
            self.metadata_db.update_version_status(version_id, "archived")

            self.logger.info(f"Successfully deleted version: {version_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to delete version {version_id}: {e}")
            raise

    def rotate_versions(
        self, keep_count: int = 5, protect_baseline: bool = True
    ) -> Dict[str, List[str]]:
        """Rotate versions by deleting old versions beyond keep_count.

        Args:
            keep_count: Number of recent versions to keep
            protect_baseline: If True, never delete baseline versions

        Returns:
            Dictionary with 'deleted', 'skipped', and 'remaining' lists
        """
        self.logger.info(f"Rotating versions: keeping {keep_count}")

        # Get all committed versions sorted by timestamp (newest first)
        all_versions = self.list_versions(include_archived=False)
        committed = [v for v in all_versions if v["status"] == "committed"]

        # Sort by version_id (which includes timestamp)
        committed.sort(key=lambda v: v["version_id"], reverse=True)

        # Identify versions to delete (beyond keep_count)
        to_keep = committed[:keep_count]
        to_delete = committed[keep_count:]

        # Filter out baseline if protected
        if protect_baseline:
            to_delete = [v for v in to_delete if "baseline" not in v["version_id"]]

        deleted = []
        skipped = []

        # Delete each version
        for version in to_delete:
            version_id = version["version_id"]
            try:
                self.delete_version(version_id, force=False)
                deleted.append(version_id)
            except Exception as e:
                self.logger.warning(f"Skipped {version_id}: {e}")
                skipped.append(version_id)

        result = {
            "deleted": deleted,
            "skipped": skipped,
            "remaining": keep_count,
        }

        self.logger.info(
            f"Rotation complete: {len(deleted)} deleted, {len(skipped)} skipped"
        )
        return result

    def is_baseline_registered(self) -> bool:
        """Check if baseline migration has been run.

        Returns:
            True if baseline version exists in registry
        """
        versions = self.list_versions(include_archived=True)
        for version in versions:
            if "baseline" in version["version_id"]:
                return True
        return False

    def run_baseline_migration(
        self, description: str = "Initial baseline from existing data"
    ) -> str:
        """Run baseline migration to register existing data as v1_baseline.

        This creates a baseline version that references the current state of all
        three storage systems without copying data. Uses aliases in Qdrant and
        labels in Neo4j to mark existing data as baseline.

        Args:
            description: Description for baseline version

        Returns:
            The baseline version_id

        Raises:
            Exception: If baseline migration fails
        """
        # Check if baseline already exists
        if self.is_baseline_registered():
            self.logger.info("Baseline already registered, returning existing")
            versions = self.list_versions(include_archived=True)
            for version in versions:
                if "baseline" in version["version_id"]:
                    return version["version_id"]

        # Generate baseline version ID
        baseline_id = f"v1_baseline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        baseline_alias = f"textbook_chunks_{baseline_id}"
        self.logger.info(f"Running baseline migration: {baseline_id}")

        try:
            # Register baseline in SQLite metadata DB
            self.logger.debug("Registering baseline in metadata DB")
            self.metadata_db.register_version(
                version_id=baseline_id,
                description=description,
                sqlite_snapshot="baseline",
                qdrant_snapshot=baseline_alias,
                neo4j_snapshot="baseline_labels",
            )
            # Update status to committed
            self.metadata_db.update_version_status(baseline_id, "committed")

            # Register baseline in Qdrant (create alias to existing collection)
            self.logger.debug("Registering baseline alias in Qdrant")
            # Create alias pointing to main collection (no data copy)
            self.qdrant.create_alias_backup(baseline_alias)

            # Register baseline in Neo4j (add :Baseline label to existing nodes)
            self.logger.debug("Adding baseline labels in Neo4j")
            with self.neo4j.driver.session() as session:
                # Add :Baseline label to all current nodes
                session.run(
                    """
                    MATCH (n)
                    WHERE n.id IS NOT NULL
                    SET n:Baseline
                    RETURN count(n) as count
                    """
                )

            # Validate all three systems
            self.logger.debug("Validating baseline migration")

            # Check SQLite
            version_info = self.get_version_info(baseline_id)
            if not version_info:
                raise Exception("Baseline not found in metadata DB")

            # Check Qdrant alias exists
            try:
                from hybridflow.parsing.embedder import EmbeddingGenerator

                embedder = EmbeddingGenerator(model_name="pritamdeka/S-PubMedBert-MS-MARCO-SCIFACT")
                query_embedding = embedder.generate_embedding("test")
                self.qdrant.client.query_points(
                    collection_name=baseline_alias, query=query_embedding, limit=1
                )
            except Exception as e:
                raise Exception(f"Qdrant baseline alias validation failed: {e}")

            # Check Neo4j baseline labels
            with self.neo4j.driver.session() as session:
                result = session.run("MATCH (n:Baseline) RETURN count(n) as count")
                count = result.single()["count"]
                if count == 0:
                    raise Exception("No baseline labels found in Neo4j")

            self.logger.info(f"Baseline migration successful: {baseline_id}")
            return baseline_id

        except Exception as e:
            self.logger.error(f"Baseline migration failed: {e}")
            # Attempt cleanup
            try:
                self.logger.debug("Attempting baseline cleanup after failure")
                # Remove from metadata DB
                self.metadata_db.update_version_status(baseline_id, "failed")
                # Remove Qdrant alias
                self.qdrant.delete_alias(baseline_alias)
                # Remove Neo4j labels
                with self.neo4j.driver.session() as session:
                    session.run("MATCH (n:Baseline) REMOVE n:Baseline")
            except Exception as cleanup_error:
                self.logger.error(f"Cleanup failed: {cleanup_error}")
            raise

    def validate_all_systems(
        self, version_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Validate data consistency across all three storage systems.

        Args:
            version_id: Optional version ID to validate. If None, validates current state
                       by resolving to the latest committed version.

        Returns:
            Dictionary with validation results from all systems and cross-system checks
        """
        # Resolve "current" to the latest committed version for versioned data queries
        # Qdrant resolves via _latest alias, Neo4j needs explicit version_id
        resolved_version = version_id
        if resolved_version is None:
            resolved_version = self.metadata_db.get_latest_version()
            self.logger.debug(f"Resolved 'current' to latest version: {resolved_version}")

        self.logger.info(f"Validating all systems for version: {version_id or 'current'}")

        validation_report = {
            "version_id": version_id or "current",
            "resolved_version": resolved_version,
            "sqlite": {},
            "qdrant": {},
            "neo4j": {},
            "cross_system": {},
            "status": "valid",
        }

        try:
            # Get SQLite stats - count chapters
            self.logger.debug("Getting SQLite chapter count")
            from sqlalchemy import func
            from hybridflow.storage.metadata_db import ChapterMetadata
            session = self.metadata_db.session_factory()
            try:
                chapter_count = session.query(func.count(ChapterMetadata.id)).scalar()
                validation_report["sqlite"] = {
                    "chapters": chapter_count,
                }
            finally:
                session.close()

            # Get Qdrant stats - uses _latest alias when version_id is None
            self.logger.debug("Validating Qdrant collection")
            qdrant_stats = self.qdrant.validate_collection(version_id)
            validation_report["qdrant"] = qdrant_stats

            # Get Neo4j stats - pass resolved version for versioned data queries
            self.logger.debug("Validating Neo4j graph")
            neo4j_stats = self.neo4j.validate_graph(version_id=resolved_version)
            validation_report["neo4j"] = neo4j_stats

            # Cross-check counts between Qdrant and Neo4j
            qdrant_count = qdrant_stats.get("point_count", 0)
            neo4j_count = neo4j_stats.get("node_counts", {}).get("Paragraph", 0)
            counts_match = qdrant_count == neo4j_count

            validation_report["cross_system"] = {
                "qdrant_neo4j_match": counts_match,
                "qdrant_count": qdrant_count,
                "neo4j_count": neo4j_count,
            }

            # Set overall status
            if not counts_match:
                validation_report["status"] = "mismatch"
                self.logger.warning(
                    f"Count mismatch: Qdrant={qdrant_count}, Neo4j={neo4j_count}"
                )
            else:
                self.logger.info(
                    f"Validation passed: {qdrant_count} paragraphs in both systems"
                )

        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            validation_report["status"] = "error"
            validation_report["error"] = str(e)

        return validation_report
