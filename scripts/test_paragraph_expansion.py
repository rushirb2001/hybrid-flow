#!/usr/bin/env python3
"""Test script for paragraph expansion functionality."""

import json
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

from hybridflow.retrieval.query import QueryEngine

# Load environment variables
load_dotenv()

# Initialize connections
qdrant_client = QdrantClient(
    host=os.getenv("QDRANT_HOST", "localhost"),
    port=int(os.getenv("QDRANT_PORT", "6333")),
)

neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password")),
)

# Create query engine
engine = QueryEngine(qdrant_client=qdrant_client, neo4j_driver=neo4j_driver)

print("=" * 80)
print("Testing get_surrounding_paragraphs with bailey:ch60:2.4.4.2")
print("=" * 80)

# Test the specific paragraph
chunk_id = "bailey:ch60:2.4.4.2"
result = engine.get_surrounding_paragraphs(chunk_id, before_count=2, after_count=2)

if result:
    print(f"\n✓ Successfully retrieved surrounding paragraphs for {chunk_id}\n")

    print(f"Hierarchy: {result['hierarchy']}")
    print(f"Parent Section: {result['parent_section']}")
    print(f"Chapter ID: {result['chapter_id']}\n")

    # Print metadata
    meta = result["metadata"]
    print(
        f"Metadata: Requested {meta['requested_before']} before, "
        f"{meta['requested_after']} after"
    )
    print(
        f"          Returned {meta['returned_before']} before, "
        f"{meta['returned_after']} after\n"
    )

    # Print before paragraphs
    print("BEFORE paragraphs:")
    if result["before"]:
        for i, para in enumerate(result["before"], 1):
            print(f"  [{i}] #{para['number']} - Page {para['page']}")
            print(f"      {para['text'][:100]}...")
            print()
    else:
        print("  (none - at beginning of section)\n")

    # Print current paragraph
    print("CURRENT paragraph:")
    current = result["current"]
    print(f"  #{current['number']} - Page {current['page']}")
    print(f"  Chunk ID: {current['chunk_id']}")
    print(f"  {current['text'][:200]}...")
    print()

    # Print after paragraphs
    print("AFTER paragraphs:")
    if result["after"]:
        for i, para in enumerate(result["after"], 1):
            print(f"  [{i}] #{para['number']} - Page {para['page']}")
            print(f"      {para['text'][:100]}...")
            print()
    else:
        print("  (none - at end of section)\n")

    # Print all siblings count
    print(f"Total siblings in parent section: {len(result['all_siblings'])}")
    print("\nAll sibling paragraph numbers:")
    sibling_numbers = [s["number"] for s in result["all_siblings"]]
    print(f"  {', '.join(sibling_numbers)}")

    # Save full result to JSON for inspection
    with open("/tmp/paragraph_expansion_test.json", "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n✓ Full result saved to /tmp/paragraph_expansion_test.json")

else:
    print(f"\n✗ Failed to retrieve surrounding paragraphs for {chunk_id}")

print("\n" + "=" * 80)
print("Testing edge case: first paragraph in section")
print("=" * 80)

# Test edge case - first paragraph
result2 = engine.get_surrounding_paragraphs("bailey:ch01:1.1", before_count=2, after_count=2)
if result2:
    meta2 = result2["metadata"]
    print(
        f"✓ First paragraph test: {meta2['returned_before']} before, "
        f"{meta2['returned_after']} after"
    )
else:
    print("✗ Failed to retrieve first paragraph")

print("\n" + "=" * 80)

# Close connections
engine.close()
qdrant_client.close()
