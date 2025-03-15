#!/usr/bin/env python3
"""Test script for section context retrieval functionality (TASK 2.1, 2.2, 2.3)."""

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
print("TEST 1: get_section_summary - Section Summary Retrieval (TASK 2.1)")
print("=" * 80)

section_id = "bailey:ch60:s2:ss2.4:sss2.4.4"
summary = engine.get_section_summary(section_id)

if summary:
    print(f"\n✓ Successfully retrieved summary for {section_id}\n")
    print(f"Section Title: {summary['section_title']}")
    print(f"Hierarchy: {summary['hierarchy']}")
    print(f"First Paragraph: #{summary['number']}")
    print(f"Page: {summary['page']}")
    print(f"Text preview: {summary['text'][:150]}...")
    print()
else:
    print(f"\n✗ Failed to retrieve summary for {section_id}\n")

print("=" * 80)
print("TEST 2: get_sibling_paragraphs - Same Level Only (TASK 2.2)")
print("=" * 80)

chunk_id = "bailey:ch60:2.4.4.2"
siblings_same = engine.get_sibling_paragraphs(chunk_id, same_level_only=True)

if siblings_same:
    print(f"\n✓ Successfully retrieved siblings for {chunk_id} (same level only)\n")
    print(f"Parent: {siblings_same['parent_title']}")
    print(f"Parent ID: {siblings_same['parent_id']}")
    print(f"Total siblings: {siblings_same['total_siblings']}")
    print(f"Hierarchy: {siblings_same['hierarchy']}")
    print()

    print(f"Siblings (same level):")
    for i, sibling in enumerate(siblings_same['siblings'], 1):
        marker = "→ CURRENT" if sibling['is_current'] else ""
        print(f"  [{i}] #{sibling['number']} {marker}")
        print(f"      {sibling['text'][:80]}...")
        print()
else:
    print(f"\n✗ Failed to retrieve siblings for {chunk_id}\n")

print("=" * 80)
print("TEST 3: get_sibling_paragraphs - Section Level (TASK 2.2)")
print("=" * 80)

siblings_section = engine.get_sibling_paragraphs(chunk_id, same_level_only=False)

if siblings_section:
    print(f"\n✓ Successfully retrieved siblings for {chunk_id} (section level)\n")
    print(f"Parent: {siblings_section['parent_title']}")
    print(f"Parent ID: {siblings_section['parent_id']}")
    print(f"Total siblings: {siblings_section['total_siblings']}")
    print()

    print(f"Comparison:")
    print(f"  Same level only: {siblings_same['total_siblings']} siblings")
    print(f"  Section level:   {siblings_section['total_siblings']} siblings")
    print()
else:
    print(f"\n✗ Failed to retrieve section-level siblings for {chunk_id}\n")

print("=" * 80)
print("TEST 4: Hybrid Search with include_section_context (TASK 2.3)")
print("=" * 80)

query = "hemorrhage treatment"

# Without section context (default)
results_default = engine.hybrid_search(query, limit=2)

# With section context
results_with_context = engine.hybrid_search(
    query, limit=2, include_section_context=True
)

print(f"\nQuery: '{query}'\n")

print("Default Results (no section context):")
for i, result in enumerate(results_default, 1):
    print(f"  Result {i}:")
    print(f"    Score: {result['score']:.4f}")
    print(f"    Chunk ID: {result['chunk_id']}")
    print(f"    Has 'section_context': {'section_context' in result}")
    print()

print("\nResults WITH Section Context:")
for i, result in enumerate(results_with_context, 1):
    print(f"  Result {i}:")
    print(f"    Score: {result['score']:.4f}")
    print(f"    Chunk ID: {result['chunk_id']}")
    print(f"    Has 'section_context': {'section_context' in result}")
    print()

    if 'section_context' in result:
        ctx = result['section_context']
        print(f"    Section Context:")
        print(f"      Parent: {ctx['parent_title']}")
        print(f"      Summary Paragraph: #{ctx['summary_paragraph']['number']}")
        print(f"      Summary Text: {ctx['summary_paragraph']['text'][:100]}...")
        print()

print("=" * 80)
print("TEST 5: Backward Compatibility Check")
print("=" * 80)

# Test that default behavior hasn't changed
has_section_context_default = any('section_context' in r for r in results_default)
has_section_context_enabled = all('section_context' in r for r in results_with_context)

print(f"\nDefault results have section_context: {has_section_context_default}")
print(f"Enabled results have section_context: {has_section_context_enabled}")

if not has_section_context_default and has_section_context_enabled:
    print("\n✓ Backward compatibility maintained!")
    print("  - Default behavior unchanged (no section_context)")
    print("  - New section context feature works when enabled")
else:
    print("\n✗ Backward compatibility issue detected!")

print("\n" + "=" * 80)
print("TEST 6: All Expansion Modes Together (TASK 2.3)")
print("=" * 80)

results_all = engine.hybrid_search(
    query,
    limit=1,
    expand_context=True,
    expand_paragraphs=True,
    include_section_context=True,
    before_count=2,
    after_count=2,
)

if results_all:
    result = results_all[0]

    has_hierarchy = 'hierarchy' in result
    has_hierarchy_details = 'hierarchy_details' in result
    has_expanded = 'expanded_context' in result
    has_section = 'section_context' in result

    print(f"\nWith all expansion modes enabled:")
    print(f"  Has hierarchy: {has_hierarchy}")
    print(f"  Has hierarchy_details: {has_hierarchy_details}")
    print(f"  Has expanded_context: {has_expanded}")
    print(f"  Has section_context: {has_section}")

    if has_hierarchy_details:
        print(f"\n  Hierarchy Details:")
        details = result['hierarchy_details']
        print(f"    Chapter: {details['chapter_title']}")
        print(f"    Section: {details['section_title']}")
        print(f"    Subsection: {details['subsection_title']}")
        print(f"    Subsubsection: {details['subsubsection_title']}")

    if has_expanded:
        exp = result['expanded_context']
        print(f"\n  Expanded Context:")
        print(f"    Before paragraphs: {len(exp['before_paragraphs'])}")
        print(f"    After paragraphs: {len(exp['after_paragraphs'])}")
        print(f"    All siblings: {len(exp['all_siblings'])}")

    if has_section:
        ctx = result['section_context']
        print(f"\n  Section Context:")
        print(f"    Parent: {ctx['parent_title']}")
        print(f"    Summary: {ctx['summary_paragraph']['text'][:80]}...")

    if has_hierarchy and has_hierarchy_details and has_expanded and has_section:
        print("\n  ✓ All expansion modes working together!")
    else:
        print("\n  ✗ Some expansion modes missing")

# Save results for inspection
with open("/tmp/section_context_test.json", "w") as f:
    json.dump(results_all, f, indent=2)

print(f"\n✓ Full results saved to /tmp/section_context_test.json")
print("=" * 80)

# Close connections
engine.close()
qdrant_client.close()
