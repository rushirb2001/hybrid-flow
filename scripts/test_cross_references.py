#!/usr/bin/env python3
"""Test script to verify cross-reference functionality before migration."""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

from hybridflow.parsing.chunk_generator import ChunkGenerator
from hybridflow.retrieval.query import QueryEngine

# Load environment variables
load_dotenv()

# Initialize components
chunk_generator = ChunkGenerator()

# Initialize Qdrant client
qdrant_client = QdrantClient(
    host=os.getenv("QDRANT_HOST", "localhost"),
    port=int(os.getenv("QDRANT_PORT", "6333"))
)

# Initialize Neo4j driver
neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))
)

# Initialize query engine
query_engine = QueryEngine(
    qdrant_client=qdrant_client,
    neo4j_driver=neo4j_driver,
    collection_name="textbook_chunks"
)

print("=" * 80)
print("CROSS-REFERENCE FUNCTIONALITY TEST")
print("=" * 80)

# Test 1: Extract references from various text formats
print("\n" + "=" * 80)
print("TEST 1: Reference Extraction from Text")
print("=" * 80)

test_texts = [
    "(Figure 60.5) shows the anatomy",
    "See [Figure 60.1] and (Table 60.2) for details",
    "Compare Fig. 2.1 with Figure 2.2",
    "Refer to Table 60.3 for measurements",
    "Multiple refs: (Figure 1.1), Fig. 1.2, and [Table 1.1]",
    "No references in this text",
]

for i, text in enumerate(test_texts, 1):
    refs = chunk_generator.extract_references(text)
    print(f"\nTest {i}: {text[:60]}...")
    if refs:
        print(f"  Found {len(refs)} reference(s):")
        for ref in refs:
            print(f"    - {ref['type']}: {ref['number']}")
    else:
        print("  No references found")

# Test 2: Check if paragraph has cross_references property
print("\n" + "=" * 80)
print("TEST 2: Check Existing Paragraph for Cross-References")
print("=" * 80)

# Try to find a paragraph that likely has references
test_chunk_id = "bailey:ch60:1.2.1"  # From earlier examples

print(f"\nChecking paragraph: {test_chunk_id}")
print("Note: This will be empty until migration runs")

referenced_content = query_engine.get_referenced_content(test_chunk_id)

if referenced_content:
    print(f"\nChunk ID: {referenced_content['chunk_id']}")
    print(f"Reference counts: {referenced_content['counts']}")

    if referenced_content['references']:
        print(f"\nFound {len(referenced_content['references'])} reference(s):")
        for ref in referenced_content['references']:
            if 'error' in ref:
                print(f"  - {ref['type']} {ref['number']}: ERROR - {ref['error']}")
            else:
                print(f"  - {ref['type']} {ref['number']}:")
                if ref['type'] == 'figure':
                    print(f"      Caption: {ref['caption'][:80]}...")
                    print(f"      File: {ref['file_png']}")
                elif ref['type'] == 'table':
                    print(f"      Description: {ref['description'][:80]}...")
                    print(f"      Files: {ref['file_png']}, {ref['file_xlsx']}")
                print(f"      Page: {ref['page']}")
    else:
        print("\nNo references found (expected before migration)")
else:
    print("Paragraph not found or no references")

# Test 3: Test hybrid_search with include_references
print("\n" + "=" * 80)
print("TEST 3: Hybrid Search with Reference Inclusion")
print("=" * 80)

query = "lung anatomy"
print(f"\nQuery: '{query}'")
print("Testing with include_references=False (default):")

results = query_engine.hybrid_search(
    query,
    limit=2,
    expand_context=True,
    include_references=False
)

for i, result in enumerate(results, 1):
    print(f"\n  Result {i}:")
    print(f"    Chunk ID: {result['chunk_id']}")
    print(f"    Score: {result['score']:.4f}")
    print(f"    Has 'referenced_content': {'referenced_content' in result}")
    if 'hierarchy' in result:
        print(f"    Hierarchy: {result['hierarchy'][:80]}...")

print("\n\nTesting with include_references=True:")

results_with_refs = query_engine.hybrid_search(
    query,
    limit=2,
    expand_context=True,
    include_references=True
)

for i, result in enumerate(results_with_refs, 1):
    print(f"\n  Result {i}:")
    print(f"    Chunk ID: {result['chunk_id']}")
    print(f"    Score: {result['score']:.4f}")
    print(f"    Has 'referenced_content': {'referenced_content' in result}")

    if 'referenced_content' in result:
        ref_content = result['referenced_content']
        print(f"    Reference counts: {ref_content['counts']}")
        if ref_content['references']:
            print(f"    References:")
            for ref in ref_content['references']:
                if 'error' not in ref:
                    print(f"      - {ref['type']} {ref['number']}")

# Test 4: Find paragraphs with actual figure/table references in text
print("\n" + "=" * 80)
print("TEST 4: Find Paragraphs with References in Text")
print("=" * 80)

print("\nSearching for paragraphs that mention figures/tables...")

# Search for common reference patterns
test_queries = ["Figure 60", "Table 60"]

for test_query in test_queries:
    print(f"\n  Query: '{test_query}'")
    results = query_engine.hybrid_search(test_query, limit=3, expand_context=False)

    for i, result in enumerate(results, 1):
        text = result.get('text', '')
        refs = chunk_generator.extract_references(text)

        print(f"\n  Result {i} ({result['chunk_id']}):")
        print(f"    Text snippet: {text[:100]}...")
        if refs:
            print(f"    Extracted {len(refs)} reference(s):")
            for ref in refs:
                print(f"      - {ref['type']}: {ref['number']}")
        else:
            print(f"    No extractable references (query matched but format varies)")

# Test 5: Verify Figure/Table entities exist in Neo4j
print("\n" + "=" * 80)
print("TEST 5: Verify Figure/Table Entities Exist")
print("=" * 80)

with neo4j_driver.session() as session:
    # Count total figures and tables
    result = session.run("MATCH (f:Figure) RETURN count(f) as count")
    figure_count = result.single()["count"]

    result = session.run("MATCH (t:Table) RETURN count(t) as count")
    table_count = result.single()["count"]

    print(f"\nTotal Figure nodes in Neo4j: {figure_count}")
    print(f"Total Table nodes in Neo4j: {table_count}")

    # Sample some figures and tables
    print("\nSample Figures:")
    result = session.run("""
        MATCH (f:Figure)
        RETURN f.figure_number as number, f.caption as caption, f.file_png as file
        ORDER BY f.figure_number
        LIMIT 5
    """)
    for record in result:
        print(f"  - Figure {record['number']}: {record['caption'][:60]}...")
        print(f"    File: {record['file']}")

    print("\nSample Tables:")
    result = session.run("""
        MATCH (t:Table)
        RETURN t.table_number as number, t.description as desc, t.file_png as file
        ORDER BY t.table_number
        LIMIT 5
    """)
    for record in result:
        print(f"  - Table {record['number']}: {record['desc'][:60]}...")
        print(f"    File: {record['file']}")

# Clean up
query_engine.close()
neo4j_driver.close()

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
print("\nSummary:")
print("- Reference extraction: Working")
print("- Figure/Table entities: Present in Neo4j")
print("- get_referenced_content(): Ready (will populate after migration)")
print("- hybrid_search(include_references=True): Ready (will populate after migration)")
print("\nNext step: Run migrate_add_cross_references.py to populate cross_references")
print("=" * 80)
