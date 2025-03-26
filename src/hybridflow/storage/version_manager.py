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
