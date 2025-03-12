#!/usr/bin/env python3
"""Remove duplicate chapter entries from SQLite, Qdrant, and Neo4j."""

import os
import sqlite3
from collections import defaultdict
from typing import List, Dict, Tuple

from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

# Load environment variables
load_dotenv()


def get_duplicates(db_path: str) -> Dict[Tuple[str, str], List[Dict]]:
    """Find duplicate entries in chapter_metadata.

    Args:
        db_path: Path to SQLite database

    Returns:
        Dictionary mapping (textbook_id, source_file_path) to list of duplicate entries
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all chapter metadata entries
    cursor.execute("""
        SELECT id, textbook_id, chapter_number, title, source_file_path, version, chunk_count
        FROM chapter_metadata
        ORDER BY textbook_id, source_file_path, version DESC
    """)

    entries = [dict(row) for row in cursor.fetchall()]
    conn.close()

    # Group by (textbook_id, source_file_path)
    groups = defaultdict(list)
    for entry in entries:
        key = (entry['textbook_id'], entry['source_file_path'])
        groups[key].append(entry)

    # Filter to only duplicates (more than 1 entry per key)
    duplicates = {k: v for k, v in groups.items() if len(v) > 1}

    return duplicates


def clean_duplicates(db_path: str, qdrant_host: str, qdrant_port: int,
                     neo4j_uri: str, neo4j_user: str, neo4j_password: str,
                     dry_run: bool = False) -> Dict:
    """Remove duplicate entries from all databases.

    Args:
        db_path: Path to SQLite database
        qdrant_host: Qdrant host
        qdrant_port: Qdrant port
        neo4j_uri: Neo4j connection URI
        neo4j_user: Neo4j username
        neo4j_password: Neo4j password
        dry_run: If True, only report what would be deleted

    Returns:
        Dictionary with cleanup statistics
    """
    # Find duplicates
    duplicates = get_duplicates(db_path)

    if not duplicates:
        print("No duplicates found")
        return {'duplicates_found': 0, 'entries_deleted': 0}

    print(f"Found {len(duplicates)} duplicate groups:")
    print()

    # Connect to databases
    if not dry_run:
        qdrant_client = QdrantClient(host=qdrant_host, port=qdrant_port)
        neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        sqlite_conn = sqlite3.connect(db_path)
        sqlite_cursor = sqlite_conn.cursor()

    total_deleted = 0
    chunk_ids_deleted = []
    chapter_ids_deleted = []

    for (textbook_id, source_file_path), entries in duplicates.items():
        print(f"Duplicate: {textbook_id} - {source_file_path}")
        print(f"  Found {len(entries)} entries")

        # Keep the entry with highest version (first in list due to ORDER BY version DESC)
        keep_entry = entries[0]
        delete_entries = entries[1:]

        print(f"  Keeping: ID={keep_entry['id']}, version={keep_entry['version']}, chapter={keep_entry['chapter_number']}")

        for entry in delete_entries:
            print(f"  Deleting: ID={entry['id']}, version={entry['version']}, chapter={entry['chapter_number']}")

            if not dry_run:
                # Construct chapter_id for Neo4j
                chapter_id = f"{entry['textbook_id']}:{entry['chapter_number']}"
                chapter_ids_deleted.append(chapter_id)

                # Delete from SQLite
                sqlite_cursor.execute("DELETE FROM chapter_metadata WHERE id = ?", (entry['id'],))

            total_deleted += 1

        print()

    if dry_run:
        print(f"DRY RUN: Would delete {total_deleted} duplicate entries")
        return {'duplicates_found': len(duplicates), 'entries_deleted': 0, 'dry_run': True}

    # Commit SQLite changes
    sqlite_conn.commit()
    sqlite_conn.close()

    # Delete from Neo4j
    print(f"Deleting {len(chapter_ids_deleted)} chapters from Neo4j...")
    with neo4j_driver.session() as session:
        for chapter_id in chapter_ids_deleted:
            query = "MATCH (c:Chapter {id: $chapter_id}) DETACH DELETE c"
            session.run(query, chapter_id=chapter_id)

    neo4j_driver.close()

    print(f"Cleanup complete:")
    print(f"  - SQLite: Deleted {total_deleted} duplicate entries")
    print(f"  - Neo4j: Deleted {len(chapter_ids_deleted)} chapter nodes")

    return {
        'duplicates_found': len(duplicates),
        'entries_deleted': total_deleted,
        'neo4j_chapters_deleted': len(chapter_ids_deleted),
    }


def main():
    """Main function."""
    db_path = os.getenv('METADATA_DB_PATH', './metadata.db')
    qdrant_host = os.getenv('QDRANT_HOST', 'localhost')
    qdrant_port = int(os.getenv('QDRANT_PORT', '6333'))
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'password')

    print("Duplicate Cleanup Script")
    print("=" * 50)
    print()

    result = clean_duplicates(
        db_path=db_path,
        qdrant_host=qdrant_host,
        qdrant_port=qdrant_port,
        neo4j_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password,
        dry_run=False
    )

    print()
    print("Summary:")
    for key, value in result.items():
        print(f"  {key}: {value}")

    return 0


if __name__ == '__main__':
    exit(main())
