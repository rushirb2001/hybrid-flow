#!/usr/bin/env python3
"""Migration script to add NEXT/PREV relationships to existing paragraphs."""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from hybridflow.storage.neo4j_client import Neo4jStorage

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Run migration to add sequential paragraph relationships."""
    # Connect to Neo4j
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

    logger.info("Connecting to Neo4j...")
    neo4j_storage = Neo4jStorage(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)

    # Get all chapter IDs
    logger.info("Fetching all chapters from database...")
    query = "MATCH (c:Chapter) RETURN c.id as chapter_id ORDER BY c.id"

    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        chapter_ids = [record["chapter_id"] for record in result]

    logger.info(f"Found {len(chapter_ids)} chapters to process")

    # Process each chapter
    total_links = 0
    successful_chapters = 0
    failed_chapters = 0

    for i, chapter_id in enumerate(chapter_ids, 1):
        try:
            logger.info(f"[{i}/{len(chapter_ids)}] Processing {chapter_id}...")
            links_created = neo4j_storage.link_sequential_paragraphs(chapter_id)
            total_links += links_created
            successful_chapters += 1
            logger.info(f"  Created {links_created} sequential links")
        except Exception as e:
            logger.error(f"  Failed to process {chapter_id}: {e}")
            failed_chapters += 1

    # Close connection
    neo4j_storage.close()

    # Summary
    logger.info("=" * 60)
    logger.info("Migration Complete!")
    logger.info(f"Total chapters processed: {len(chapter_ids)}")
    logger.info(f"Successful: {successful_chapters}")
    logger.info(f"Failed: {failed_chapters}")
    logger.info(f"Total sequential links created: {total_links}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
