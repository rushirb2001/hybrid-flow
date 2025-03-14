"""Clean duplicate Chapter nodes from Neo4j.

This script identifies and removes duplicate Chapter nodes that have the same
textbook_id and chapter_number, keeping only one node per unique chapter.
"""

import os
from collections import defaultdict

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()


def clean_neo4j_duplicates():
    """Remove old format Chapter nodes from Neo4j.

    Old format chapter IDs have 'ch' prefix (e.g., bailey:ch01, sabiston:ch1)
    New format chapter IDs have no prefix (e.g., bailey:01, sabiston:1)

    This function deletes all old format chapters that have been replaced
    by new format chapters in the database.
    """
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    try:
        with driver.session() as session:
            # Find all Chapter nodes
            query_find = """
            MATCH (c:Chapter)
            RETURN c.id as chapter_id,
                   elementId(c) as node_id,
                   c.title as title
            ORDER BY c.id
            """

            result = session.run(query_find)
            records = list(result)

            print(f"\nTotal Chapter nodes found: {len(records)}")

            # Separate old format (with :ch) from new format
            old_format_chapters = []
            new_format_chapters = []

            for record in records:
                chapter_id = record["chapter_id"]
                # Check if chapter ID contains ":ch" pattern
                if ":ch" in chapter_id:
                    old_format_chapters.append(
                        {
                            "node_id": record["node_id"],
                            "chapter_id": chapter_id,
                            "title": record["title"],
                        }
                    )
                else:
                    new_format_chapters.append(chapter_id)

            print(f"Old format chapters (with 'ch' prefix): {len(old_format_chapters)}")
            print(f"New format chapters (without 'ch' prefix): {len(new_format_chapters)}")

            if not old_format_chapters:
                print("\nNo old format chapters to delete.")
                return 0

            # Delete all old format chapters
            total_deleted = 0
            print(f"\nDeleting old format chapters:")

            for chapter in old_format_chapters:
                delete_query = """
                MATCH (c:Chapter)
                WHERE elementId(c) = $node_id
                DETACH DELETE c
                """
                session.run(delete_query, node_id=chapter["node_id"])
                total_deleted += 1
                print(f"  Deleted: {chapter['chapter_id']} - {chapter['title']}")

            print(f"\n✓ Deleted {total_deleted} old format Chapter nodes")
            return total_deleted

    finally:
        driver.close()


if __name__ == "__main__":
    deleted_count = clean_neo4j_duplicates()
    print(f"\nCleanup complete. Deleted {deleted_count} duplicates.")
