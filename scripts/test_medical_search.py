#!/usr/bin/env python3
"""Test hybrid search with medical queries including cross-references."""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

from hybridflow.retrieval.query import QueryEngine

# Load environment variables
load_dotenv()

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
print("MEDICAL SEARCH TEST WITH CROSS-REFERENCES")
print("=" * 80)

# Test queries as requested by user
test_queries = [
    "patient history features",
    "symptoms checklist",
    "lung anatomy figures",
    "surgical complications table",
]

for query in test_queries:
    print(f"\n{'=' * 80}")
    print(f"Query: '{query}'")
    print("=" * 80)

    # Search with references included
    results = query_engine.hybrid_search(
        query,
        limit=3,
        expand_context=True,
        include_references=True
    )

    for i, result in enumerate(results, 1):
        print(f"\nResult {i}:")
        print(f"  Chunk ID: {result['chunk_id']}")
        print(f"  Score: {result['score']:.4f}")

        # Show hierarchy path
        if 'hierarchy' in result:
            hierarchy = result['hierarchy']
            if len(hierarchy) > 80:
                hierarchy = hierarchy[:77] + "..."
            print(f"  Hierarchy: {hierarchy}")

        # Show snippet of text
        text = result.get('text', '')
        snippet = text[:150].replace('\n', ' ')
        if len(text) > 150:
            snippet += "..."
        print(f"  Text: {snippet}")

        # Show cross-references if present
        if 'referenced_content' in result:
            ref_content = result['referenced_content']
            counts = ref_content['counts']
            refs = ref_content['references']

            if refs:
                print(f"  Cross-References Found: {counts['total']} total")
                print(f"    - Figures: {counts['figures']}")
                print(f"    - Tables: {counts['tables']}")

                # Show details of first 2 references
                for j, ref in enumerate(refs[:2], 1):
                    if 'error' not in ref:
                        ref_type = ref['type'].capitalize()
                        ref_num = ref['number']
                        print(f"\n    Reference {j}: {ref_type} {ref_num}")

                        if ref_type == "Figure":
                            caption = ref.get('caption', '')
                            if len(caption) > 60:
                                caption = caption[:57] + "..."
                            print(f"      Caption: {caption}")
                            if ref.get('file_png'):
                                print(f"      File: {ref['file_png']}")

                        elif ref_type == "Table":
                            desc = ref.get('description', '')
                            if len(desc) > 60:
                                desc = desc[:57] + "..."
                            print(f"      Description: {desc}")
                            if ref.get('file_png'):
                                print(f"      File PNG: {ref['file_png']}")
                            if ref.get('file_xlsx'):
                                print(f"      File XLSX: {ref['file_xlsx']}")

                        print(f"      Page: {ref.get('page', 'N/A')}")

                if len(refs) > 2:
                    print(f"\n    ... and {len(refs) - 2} more reference(s)")
            else:
                print(f"  Cross-References: None found in this paragraph")
        else:
            print(f"  Cross-References: Not included (no references in paragraph)")

# Clean up
query_engine.close()
neo4j_driver.close()

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
print("\nSummary:")
print("- Tested hybrid search with medical queries")
print("- Included cross-reference retrieval")
print("- Demonstrated figure and table linking")
print("=" * 80)
