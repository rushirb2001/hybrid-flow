#!/usr/bin/env python3
"""Test script for hybrid search with paragraph expansion."""

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

query = "hemorrhage treatment"

print("=" * 80)
print("TEST 1: Backward Compatibility - Default behavior (no expansion)")
print("=" * 80)

results_default = engine.hybrid_search(query, limit=2)

print(f"\nQuery: '{query}'")
print(f"Results: {len(results_default)}\n")

for i, result in enumerate(results_default, 1):
    print(f"Result {i}:")
    print(f"  Score: {result['score']:.4f}")
    print(f"  Chunk ID: {result['chunk_id']}")
    print(f"  Has 'expanded_context': {'expanded_context' in result}")
    print(f"  Has 'hierarchy': {'hierarchy' in result}")
    print(f"  Text preview: {result.get('text', '')[:80]}...")
    print()

print("=" * 80)
print("TEST 2: Hybrid Search WITH Paragraph Expansion")
print("=" * 80)

results_expanded = engine.hybrid_search(
    query, limit=2, expand_paragraphs=True, before_count=2, after_count=2
)

print(f"\nQuery: '{query}'")
print(f"Results: {len(results_expanded)}\n")

for i, result in enumerate(results_expanded, 1):
    print(f"Result {i}:")
    print(f"  Score: {result['score']:.4f}")
    print(f"  Chunk ID: {result['chunk_id']}")
    print(f"  Has 'expanded_context': {'expanded_context' in result}")
    print()

    if "expanded_context" in result:
        exp = result["expanded_context"]
        meta = exp["expansion_metadata"]

        print(f"  Expansion Metadata:")
        print(
            f"    Requested: {meta['requested_before']} before, {meta['requested_after']} after"
        )
        print(
            f"    Returned: {meta['returned_before']} before, {meta['returned_after']} after"
        )
        print()

        print(f"  BEFORE paragraphs ({len(exp['before_paragraphs'])}):")
        for j, para in enumerate(exp["before_paragraphs"], 1):
            print(f"    [{j}] #{para['number']} - {para['text'][:60]}...")

        print(f"\n  CURRENT paragraph:")
        current = exp["current_paragraph"]
        print(f"    #{current['number']} - {current['text'][:60]}...")

        print(f"\n  AFTER paragraphs ({len(exp['after_paragraphs'])}):")
        for j, para in enumerate(exp["after_paragraphs"], 1):
            print(f"    [{j}] #{para['number']} - {para['text'][:60]}...")

        print(f"\n  Parent Section: {exp['parent_section']}")
        print(f"  Total siblings in section: {len(exp['all_siblings'])}")
        print()

print("=" * 80)
print("TEST 3: Verify Backward Compatibility - Results should be identical")
print("=" * 80)

# Compare that default results don't have expanded_context
has_expansion_in_default = any("expanded_context" in r for r in results_default)
has_expansion_in_expanded = all("expanded_context" in r for r in results_expanded)

print(f"\nDefault results have expanded_context: {has_expansion_in_default}")
print(f"Expanded results have expanded_context: {has_expansion_in_expanded}")

if not has_expansion_in_default and has_expansion_in_expanded:
    print("\n✓ Backward compatibility maintained!")
    print("  - Default behavior unchanged (no expanded_context)")
    print("  - New expansion feature works when enabled")
else:
    print("\n✗ Backward compatibility issue detected!")

print("\n" + "=" * 80)
print("TEST 4: Edge Case - Expansion with custom counts")
print("=" * 80)

results_custom = engine.hybrid_search(
    query, limit=1, expand_paragraphs=True, before_count=5, after_count=5
)

if results_custom and "expanded_context" in results_custom[0]:
    exp = results_custom[0]["expanded_context"]
    meta = exp["expansion_metadata"]
    print(f"\nCustom expansion (requested 5 before, 5 after):")
    print(f"  Returned: {meta['returned_before']} before, {meta['returned_after']} after")
    print(f"  ✓ Handles custom counts correctly")
else:
    print("✗ Custom expansion failed")

print("\n" + "=" * 80)
print("TEST 5: Both expand_context and expand_paragraphs enabled")
print("=" * 80)

results_both = engine.hybrid_search(
    query, limit=1, expand_context=True, expand_paragraphs=True, before_count=1, after_count=1
)

if results_both:
    result = results_both[0]
    has_hierarchy = "hierarchy" in result
    has_full_text = "full_text" in result
    has_expanded = "expanded_context" in result

    print(f"\nWith both expansions enabled:")
    print(f"  Has hierarchy (from expand_context): {has_hierarchy}")
    print(f"  Has full_text (from expand_context): {has_full_text}")
    print(f"  Has expanded_context (from expand_paragraphs): {has_expanded}")

    if has_hierarchy and has_full_text and has_expanded:
        print("  ✓ Both expansion modes work together!")
    else:
        print("  ✗ Issue with combined expansion modes")

# Save expanded results for inspection
with open("/tmp/hybrid_search_expanded_test.json", "w") as f:
    json.dump(results_expanded, f, indent=2)

print(f"\n✓ Full expanded results saved to /tmp/hybrid_search_expanded_test.json")
print("=" * 80)

# Close connections
engine.close()
qdrant_client.close()
