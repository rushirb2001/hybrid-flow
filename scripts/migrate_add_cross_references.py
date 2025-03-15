#!/usr/bin/env python3
"""Migration script to add cross_references property to existing paragraphs."""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from hybridflow.parsing.chunk_generator import ChunkGenerator
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
    """Run migration to extract and store cross-references for existing paragraphs."""
    # Connect to Neo4j
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

    logger.info("Connecting to Neo4j...")
    neo4j_storage = Neo4jStorage(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)

    # Initialize chunk generator for reference extraction
    chunk_generator = ChunkGenerator()

    # Get all paragraphs
    logger.info("Fetching all paragraphs from database...")
    query = """
        MATCH (p:Paragraph)
        RETURN p.chunk_id as chunk_id, p.text as text
        ORDER BY p.chunk_id
    """

    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        paragraphs = [(record["chunk_id"], record["text"]) for record in result]

    logger.info(f"Found {len(paragraphs)} paragraphs to process")

    # Process paragraphs in batches
    total_updated = 0
    total_references_found = 0
    failed_paragraphs = 0
    batch_size = 100

    for i in range(0, len(paragraphs), batch_size):
        batch = paragraphs[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(paragraphs) + batch_size - 1) // batch_size

        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} paragraphs)...")

        for chunk_id, text in batch:
            try:
                # Extract cross-references from paragraph text
                cross_references = chunk_generator.extract_references(text)

                # Serialize cross_references to JSON string for Neo4j storage
                # Neo4j doesn't support lists of maps, only primitives or arrays of primitives
                cross_references_json = json.dumps(cross_references)

                # Update paragraph with cross_references property
                update_query = """
                    MATCH (p:Paragraph {chunk_id: $chunk_id})
                    SET p.cross_references = $cross_references
                    RETURN p.chunk_id as updated_id
                """

                with neo4j_storage.driver.session() as session:
                    result = session.run(
                        update_query,
                        chunk_id=chunk_id,
                        cross_references=cross_references_json
                    )

                    if result.single():
                        total_updated += 1
                        if cross_references:
                            total_references_found += len(cross_references)

            except Exception as e:
                logger.error(f"  Failed to update {chunk_id}: {e}")
                failed_paragraphs += 1

    # Close connection
    neo4j_storage.close()

    # Summary
    logger.info("=" * 60)
    logger.info("Migration Complete!")
    logger.info(f"Total paragraphs processed: {len(paragraphs)}")
    logger.info(f"Successfully updated: {total_updated}")
    logger.info(f"Failed: {failed_paragraphs}")
    logger.info(f"Total cross-references found: {total_references_found}")
    logger.info(f"Average references per paragraph: {total_references_found / total_updated if total_updated > 0 else 0:.2f}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
