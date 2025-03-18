"""Verify chunk_id consistency across Qdrant and Neo4j databases."""

import sys
from qdrant_client import QdrantClient
from neo4j import GraphDatabase

def get_qdrant_chunk_ids():
    """Retrieve all chunk_ids from Qdrant."""
    client = QdrantClient(host="localhost", port=6333)

    # Scroll through all points
    chunk_ids = set()
    offset = None

    while True:
        records, next_offset = client.scroll(
            collection_name="textbook_chunks",
            limit=1000,
            offset=offset,
            with_payload=True,
            with_vectors=False
        )

        for record in records:
            chunk_id = record.payload.get("chunk_id")
            if chunk_id:
                chunk_ids.add(chunk_id)

        if next_offset is None:
            break
        offset = next_offset

    return chunk_ids

def get_neo4j_chunk_ids():
    """Retrieve all chunk_ids from Neo4j."""
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    with driver.session() as session:
        result = session.run("MATCH (p:Paragraph) RETURN p.chunk_id as chunk_id")
        chunk_ids = {record["chunk_id"] for record in result}

    driver.close()
    return chunk_ids

def main():
    """Compare chunk_ids between Qdrant and Neo4j."""
    print("Fetching chunk_ids from Qdrant...")
    qdrant_ids = get_qdrant_chunk_ids()
    print(f"Found {len(qdrant_ids)} chunk_ids in Qdrant")

    print("\nFetching chunk_ids from Neo4j...")
    neo4j_ids = get_neo4j_chunk_ids()
    print(f"Found {len(neo4j_ids)} chunk_ids in Neo4j")

    # Compare
    only_in_qdrant = qdrant_ids - neo4j_ids
    only_in_neo4j = neo4j_ids - qdrant_ids
    in_both = qdrant_ids & neo4j_ids

    print(f"\n{'='*60}")
    print(f"Consistency Report:")
    print(f"{'='*60}")
    print(f"Chunk IDs in both databases: {len(in_both)}")
    print(f"Chunk IDs only in Qdrant: {len(only_in_qdrant)}")
    print(f"Chunk IDs only in Neo4j: {len(only_in_neo4j)}")

    if len(only_in_qdrant) > 0:
        print(f"\nSample IDs only in Qdrant (first 10):")
        for chunk_id in list(only_in_qdrant)[:10]:
            print(f"  - {chunk_id}")

    if len(only_in_neo4j) > 0:
        print(f"\nSample IDs only in Neo4j (first 10):")
        for chunk_id in list(only_in_neo4j)[:10]:
            print(f"  - {chunk_id}")

    if len(only_in_qdrant) == 0 and len(only_in_neo4j) == 0:
        print("\n✓ PASS: All chunk_ids are consistent across databases")
        return 0
    else:
        print("\n✗ FAIL: Chunk_id inconsistencies detected")
        return 1

if __name__ == "__main__":
    sys.exit(main())
