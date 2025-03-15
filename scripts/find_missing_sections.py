#!/usr/bin/env python3
"""Find which specific sections are missing in Neo4j."""

import json
import os
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

DATA_DIR = Path("/Users/rushirbhavsar/Main/code/git-commits/hybrid-flow/data")

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))
)

def find_missing_sections(textbook_id):
    """Find sections that exist in JSON but not in Neo4j."""
    json_dir = DATA_DIR / textbook_id

    # Collect all section IDs from JSON
    json_section_ids = set()
    for json_file in sorted(json_dir.glob("*.json")):
        with open(json_file) as f:
            data = json.load(f)

        chapter_num = data.get('chapter_number')
        sections = data.get('sections', [])

        for section in sections:
            section_num = section.get('number')  # Changed from 'section_number' to 'number'
            if section_num:
                # Construct section ID
                section_id = f"{textbook_id}:ch{chapter_num}:s{section_num}"
                json_section_ids.add(section_id)

    # Collect all section IDs from Neo4j
    neo4j_section_ids = set()
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Section)
            WHERE s.id STARTS WITH $prefix
            RETURN s.id as section_id
        """, prefix=f"{textbook_id}:")

        for record in result:
            neo4j_section_ids.add(record['section_id'])

    # Find missing sections
    missing = json_section_ids - neo4j_section_ids
    extra = neo4j_section_ids - json_section_ids

    return sorted(missing), sorted(extra)

print("=" * 80)
print("MISSING SECTIONS ANALYSIS")
print("=" * 80)

for textbook_id in ['bailey', 'sabiston', 'schwartz']:
    missing, extra = find_missing_sections(textbook_id)

    print(f"\n{textbook_id.upper()}:")

    if missing:
        print(f"  Missing in Neo4j ({len(missing)} sections):")
        for section_id in missing:
            print(f"    - {section_id}")
    else:
        print(f"  ✓ No missing sections")

    if extra:
        print(f"  Extra in Neo4j ({len(extra)} sections):")
        for section_id in extra:
            print(f"    - {section_id}")

driver.close()
print(f"\n{'='*80}")
