#!/usr/bin/env python3
"""Verify section counts between JSON files and databases."""

import json
import os
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()

# Local JSON data directories
DATA_DIR = Path("/Users/rushirbhavsar/Main/code/git-commits/hybrid-flow/data")

JSON_DIRS = {
    "bailey": DATA_DIR / "bailey",
    "sabiston": DATA_DIR / "sabiston",
    "schwartz": DATA_DIR / "schwartz",
}


def count_sections_in_json(json_path):
    """Count sections in a single JSON file."""
    with open(json_path, 'r') as f:
        data = json.load(f)

    # Sections are at top level, not nested under 'chapter'
    sections = data.get('sections', [])

    return {
        'chapter_number': data.get('chapter_number'),
        'section_count': len(sections),
        'section_numbers': sorted([s.get('number') for s in sections if s.get('number')]),  # Changed from 'section_number' to 'number'
    }


print("=" * 80)
print("SECTION COUNT VERIFICATION")
print("=" * 80)

# Count in JSON files
json_totals = {}
for textbook_id, json_dir in JSON_DIRS.items():
    json_files = sorted(json_dir.glob("*.json"))
    total_sections = sum(count_sections_in_json(f)['section_count'] for f in json_files)
    json_totals[textbook_id] = {
        'chapters': len(json_files),
        'sections': total_sections
    }
    print(f"\n{textbook_id.upper()} JSON:")
    print(f"  Chapters: {len(json_files)}")
    print(f"  Sections: {total_sections}")

# Count in Neo4j
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))
)

neo4j_totals = {}
with driver.session() as session:
    for textbook_id in JSON_DIRS.keys():
        result = session.run("""
            MATCH (c:Chapter)
            WHERE c.id STARTS WITH $textbook_prefix
            OPTIONAL MATCH (c)-[:HAS_SECTION]->(s:Section)
            RETURN count(DISTINCT c) as chapters, count(DISTINCT s) as sections
        """, textbook_prefix=f"{textbook_id}:")

        record = result.single()
        neo4j_totals[textbook_id] = {
            'chapters': record['chapters'],
            'sections': record['sections']
        }

driver.close()

# Compare
print(f"\n{'='*80}")
print("COMPARISON")
print(f"{'='*80}")

for textbook_id in JSON_DIRS.keys():
    json_data = json_totals[textbook_id]
    neo4j_data = neo4j_totals[textbook_id]

    print(f"\n{textbook_id.upper()}:")
    print(f"  JSON:  {json_data['sections']} sections")
    print(f"  Neo4j: {neo4j_data['sections']} sections")
    print(f"  Diff:  {neo4j_data['sections'] - json_data['sections']}")

    if json_data['sections'] == neo4j_data['sections']:
        print(f"  ✓ MATCH")
    else:
        print(f"  ✗ MISMATCH!")

print(f"\n{'='*80}")
