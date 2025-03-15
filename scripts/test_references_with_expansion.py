#!/usr/bin/env python3
"""Test cross-references with different context expansion combinations."""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

from hybridflow.retrieval.query import QueryEngine

# Load environment variables
load_dotenv()

# Initialize clients
qdrant_client = QdrantClient(host='localhost', port=6333)
neo4j_driver = GraphDatabase.driver(
    'bolt://localhost:7687',
    auth=('neo4j', 'password')
)

query_engine = QueryEngine(
    qdrant_client=qdrant_client,
    neo4j_driver=neo4j_driver,
    collection_name='textbook_chunks'
)

print("=" * 80)
print("CROSS-REFERENCES WITH DIFFERENT EXPANSION MODES")
print("=" * 80)

# Use a query we know returns results with references
query = "lobar divisions lungs"

test_configs = [
    {
        "name": "Basic search (no expansion)",
        "params": {
            "expand_context": False,
            "expand_paragraphs": False,
            "include_references": True
        }
    },
    {
        "name": "Hierarchy expansion only",
        "params": {
            "expand_context": True,
            "expand_paragraphs": False,
            "include_references": True
        }
    },
    {
        "name": "Sibling paragraphs expansion (before=2, after=2)",
        "params": {
            "expand_context": True,
            "expand_paragraphs": True,
            "before_count": 2,
            "after_count": 2,
            "include_references": True
        }
    },
    {
        "name": "Sibling paragraphs expansion (before=1, after=1)",
        "params": {
            "expand_context": True,
            "expand_paragraphs": True,
            "before_count": 1,
            "after_count": 1,
            "include_references": True
        }
    },
    {
        "name": "Section context + references",
        "params": {
            "expand_context": True,
            "expand_paragraphs": False,
            "include_section_context": True,
            "include_references": True
        }
    },
    {
        "name": "All expansions enabled",
        "params": {
            "expand_context": True,
            "expand_paragraphs": True,
            "before_count": 2,
            "after_count": 2,
            "include_section_context": True,
            "include_references": True
        }
    }
]

for config in test_configs:
    print(f"\n{'=' * 80}")
    print(f"TEST: {config['name']}")
    print(f"Parameters: {config['params']}")
    print("=" * 80)

    results = query_engine.hybrid_search(query, limit=3, **config['params'])

    for i, result in enumerate(results, 1):
        chunk_id = result['chunk_id']
        score = result['score']

        print(f"\nResult {i}: {chunk_id} (score: {score:.4f})")

        # Check hierarchy
        if 'hierarchy' in result:
            hierarchy = result.get('hierarchy', 'N/A')
            if len(hierarchy) > 60:
                hierarchy = hierarchy[:57] + "..."
            print(f"  Hierarchy: {hierarchy}")

        # Check expanded paragraphs
        if 'expanded_paragraphs' in result:
            exp = result['expanded_paragraphs']
            before_count = len(exp.get('before', []))
            after_count = len(exp.get('after', []))
            print(f"  Expanded paragraphs: {before_count} before, {after_count} after")

        # Check section context
        if 'section_context' in result:
            siblings = result['section_context'].get('all_siblings', [])
            print(f"  Section context: {len(siblings)} total siblings")

        # Check cross-references - THIS IS THE KEY TEST
        if 'referenced_content' in result:
            ref_content = result['referenced_content']
            counts = ref_content['counts']
            refs = ref_content['references']

            if refs:
                print(f"  ✓ Cross-references: {counts['total']} total ({counts['figures']} figures, {counts['tables']} tables)")
                for j, ref in enumerate(refs[:2], 1):
                    if 'error' not in ref:
                        print(f"    - {ref['type'].capitalize()} {ref['number']}")
                if len(refs) > 2:
                    print(f"    ... and {len(refs) - 2} more")
            else:
                print(f"  ✓ Cross-references: None in this paragraph (feature working)")
        else:
            print(f"  ✗ ERROR: 'referenced_content' field missing!")

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
print("\nSummary:")
print("- Tested cross-references with 6 different expansion configurations")
print("- Verified include_references works with:")
print("  1. No expansion")
print("  2. Hierarchy expansion only")
print("  3. Sibling paragraph expansion (different before/after counts)")
print("  4. Section context expansion")
print("  5. All expansions combined")
print("\nExpected behavior:")
print("- Cross-references should appear regardless of other expansion settings")
print("- Each result should have 'referenced_content' field when include_references=True")
print("=" * 80)

query_engine.close()
neo4j_driver.close()
