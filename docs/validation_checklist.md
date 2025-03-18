# Validation Checklist

## Overview
This checklist defines the validation procedures required before committing a new version to production. All checks must pass for a version to transition from `validating` to `committed` status.

## Validation Execution Context
- **When**: After ingestion completes, before promotion to production
- **Status**: Version in `validating` state
- **Timeout**: Maximum 15 minutes for full validation
- **Failure Action**: Trigger rollback procedure if any check fails

## Validation Checks

### Check 1: SQLite Chapter Count Matches Expected

**Objective**: Verify that the staging table contains the expected number of chapters.

**Command**:
```bash
sqlite3 metadata.db "SELECT COUNT(*) as chapter_count FROM chapter_metadata_staging;"
```

**Expected Result**:
- For full re-ingestion: `220` (or current total across all textbooks)
- For partial ingestion: Document expected count in version notes

**Pass Criteria**:
```bash
EXPECTED_COUNT=220
ACTUAL_COUNT=$(sqlite3 metadata.db "SELECT COUNT(*) FROM chapter_metadata_staging;")

if [ "$ACTUAL_COUNT" -eq "$EXPECTED_COUNT" ]; then
  echo "✓ PASS: Chapter count matches ($ACTUAL_COUNT)"
  exit 0
else
  echo "✗ FAIL: Expected $EXPECTED_COUNT chapters, found $ACTUAL_COUNT"
  exit 1
fi
```

**Failure Scenarios**:
- Missing chapters: Incomplete ingestion
- Extra chapters: Duplicate processing
- Zero chapters: Complete ingestion failure

**Rollback Trigger**: Yes - Critical failure

### Check 2: Qdrant Point Count Equals Neo4j Paragraph Count

**Objective**: Ensure 1:1 mapping between vector embeddings and graph paragraphs.

**Commands**:
```bash
# Get Qdrant staging collection point count
QDRANT_COUNT=$(curl -X GET "http://localhost:6333/collections/textbook_chunks_staging" 2>/dev/null | \
  python3 -c "import sys, json; print(json.load(sys.stdin)['result']['points_count'])")

# Get Neo4j staging paragraph count
NEO4J_COUNT=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph:staging) RETURN count(p)" --format plain | tail -1)

echo "Qdrant points: $QDRANT_COUNT"
echo "Neo4j paragraphs: $NEO4J_COUNT"
```

**Expected Result**:
- Counts must be EXACTLY equal
- Typical full ingestion: ~36,216 paragraphs

**Pass Criteria**:
```bash
if [ "$QDRANT_COUNT" -eq "$NEO4J_COUNT" ]; then
  echo "✓ PASS: Qdrant ($QDRANT_COUNT) equals Neo4j ($NEO4J_COUNT)"
  exit 0
else
  echo "✗ FAIL: Qdrant ($QDRANT_COUNT) ≠ Neo4j ($NEO4J_COUNT)"
  exit 1
fi
```

**Failure Scenarios**:
- Qdrant > Neo4j: Vector creation succeeded but graph insert failed
- Neo4j > Qdrant: Graph insert succeeded but vector creation failed
- Both zero: Complete pipeline failure

**Rollback Trigger**: Yes - Critical data inconsistency

### Check 3: All chunk_ids in SQLite Exist in Qdrant

**Objective**: Verify that every chapter tracked in metadata has corresponding vectors.

**Script**:
```python
# scripts/validate_sqlite_qdrant.py
import sqlite3
from qdrant_client import QdrantClient

def validate_sqlite_qdrant():
    # Get all chunk_ids from SQLite staging
    conn = sqlite3.connect('metadata.db')
    cursor = conn.execute("""
        SELECT DISTINCT textbook_id || ':ch' || chapter_number as chapter_id
        FROM chapter_metadata_staging
    """)
    sqlite_chapters = {row[0] for row in cursor.fetchall()}
    conn.close()

    # Get all chunk_ids from Qdrant staging
    client = QdrantClient(host="localhost", port=6333)
    qdrant_chapters = set()

    offset = None
    while True:
        records, next_offset = client.scroll(
            collection_name="textbook_chunks_staging",
            limit=1000,
            offset=offset,
            with_payload=["chunk_id"],
            with_vectors=False
        )

        for record in records:
            chunk_id = record.payload.get("chunk_id", "")
            # Extract chapter_id from chunk_id (e.g., bailey:ch60:2.1.1 → bailey:ch60)
            if chunk_id:
                parts = chunk_id.split(":")
                if len(parts) >= 2:
                    chapter_id = ":".join(parts[:2])
                    qdrant_chapters.add(chapter_id)

        if next_offset is None:
            break
        offset = next_offset

    # Compare
    missing_in_qdrant = sqlite_chapters - qdrant_chapters

    if len(missing_in_qdrant) == 0:
        print(f"✓ PASS: All {len(sqlite_chapters)} SQLite chapters exist in Qdrant")
        return 0
    else:
        print(f"✗ FAIL: {len(missing_in_qdrant)} chapters missing in Qdrant:")
        for chapter in list(missing_in_qdrant)[:10]:
            print(f"  - {chapter}")
        return 1

if __name__ == "__main__":
    exit(validate_sqlite_qdrant())
```

**Execution**:
```bash
poetry run python scripts/validate_sqlite_qdrant.py
```

**Expected Result**:
- All chapters in SQLite have at least one paragraph in Qdrant
- No missing chapters

**Rollback Trigger**: Yes - Data completeness issue

### Check 4: All chunk_ids in Qdrant Exist in Neo4j

**Objective**: Verify that every vector has a corresponding graph node.

**Script**:
```python
# scripts/validate_qdrant_neo4j.py
from qdrant_client import QdrantClient
from neo4j import GraphDatabase

def validate_qdrant_neo4j():
    # Get all chunk_ids from Qdrant staging
    client = QdrantClient(host="localhost", port=6333)
    qdrant_chunk_ids = set()

    offset = None
    while True:
        records, next_offset = client.scroll(
            collection_name="textbook_chunks_staging",
            limit=1000,
            offset=offset,
            with_payload=["chunk_id"],
            with_vectors=False
        )

        for record in records:
            chunk_id = record.payload.get("chunk_id")
            if chunk_id:
                qdrant_chunk_ids.add(chunk_id)

        if next_offset is None:
            break
        offset = next_offset

    # Get all chunk_ids from Neo4j staging
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    with driver.session() as session:
        result = session.run("MATCH (p:Paragraph:staging) RETURN p.chunk_id as chunk_id")
        neo4j_chunk_ids = {record["chunk_id"] for record in result}

    driver.close()

    # Compare
    only_in_qdrant = qdrant_chunk_ids - neo4j_chunk_ids
    only_in_neo4j = neo4j_chunk_ids - qdrant_chunk_ids

    if len(only_in_qdrant) == 0 and len(only_in_neo4j) == 0:
        print(f"✓ PASS: Perfect 1:1 mapping ({len(qdrant_chunk_ids)} chunk_ids)")
        return 0
    else:
        print(f"✗ FAIL: Chunk_id mismatch detected")
        if len(only_in_qdrant) > 0:
            print(f"  Only in Qdrant: {len(only_in_qdrant)}")
            for cid in list(only_in_qdrant)[:5]:
                print(f"    - {cid}")
        if len(only_in_neo4j) > 0:
            print(f"  Only in Neo4j: {len(only_in_neo4j)}")
            for cid in list(only_in_neo4j)[:5]:
                print(f"    - {cid}")
        return 1

if __name__ == "__main__":
    exit(validate_qdrant_neo4j())
```

**Execution**:
```bash
poetry run python scripts/validate_qdrant_neo4j.py
```

**Expected Result**:
- Perfect 1:1 mapping between Qdrant and Neo4j
- `only_in_qdrant = 0`
- `only_in_neo4j = 0`

**Rollback Trigger**: Yes - Critical consistency violation

### Check 5: All Neo4j Paragraphs Have Valid Parent Relationships

**Objective**: Ensure hierarchical integrity in the graph database.

**Query**:
```cypher
// Find orphaned paragraphs (no parent relationship)
MATCH (p:Paragraph:staging)
WHERE NOT EXISTS {
  MATCH (p)<-[:HAS_PARAGRAPH|HAS_SUBSUBSECTION|HAS_SUBSECTION|HAS_SECTION]-()
}
RETURN count(p) as orphaned_count
```

**Command**:
```bash
ORPHANED=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph:staging)
   WHERE NOT EXISTS {
     MATCH (p)<-[:HAS_PARAGRAPH|HAS_SUBSUBSECTION|HAS_SUBSECTION|HAS_SECTION]-()
   }
   RETURN count(p) as orphaned_count" --format plain | tail -1)

echo "Orphaned paragraphs: $ORPHANED"
```

**Expected Result**:
- `orphaned_count = 0`
- All paragraphs connected to hierarchy

**Pass Criteria**:
```bash
if [ "$ORPHANED" -eq "0" ]; then
  echo "✓ PASS: All paragraphs have valid parent relationships"
  exit 0
else
  echo "✗ FAIL: Found $ORPHANED orphaned paragraphs"
  exit 1
fi
```

**Additional Check - Verify Full Hierarchy**:
```cypher
// Count paragraphs reachable from chapters
MATCH (c:Chapter:staging)-[:HAS_SECTION|HAS_SUBSECTION|HAS_SUBSUBSECTION|HAS_PARAGRAPH*]->(p:Paragraph:staging)
RETURN count(DISTINCT p) as reachable_paragraphs
```

**Verification**:
- `reachable_paragraphs` should equal total staging paragraph count

**Rollback Trigger**: Yes - Hierarchy corruption

### Check 6: NEXT/PREV Sequential Paragraph Chains Unbroken

**Objective**: Verify sequential navigation relationships are intact.

**Query**:
```cypher
// Find paragraphs with broken NEXT chains
MATCH (p:Paragraph:staging)-[:NEXT]->(next:Paragraph)
WHERE NOT next:staging
RETURN count(p) as broken_next_count

UNION

// Find paragraphs with broken PREV chains
MATCH (p:Paragraph:staging)-[:PREV]->(prev:Paragraph)
WHERE NOT prev:staging
RETURN count(p) as broken_prev_count
```

**Command**:
```bash
# Check for broken NEXT/PREV chains
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph:staging)-[:NEXT]->(next:Paragraph)
   WHERE NOT next:staging
   RETURN count(p) as broken_next_count
   UNION
   MATCH (p:Paragraph:staging)-[:PREV]->(prev:Paragraph)
   WHERE NOT prev:staging
   RETURN count(p) as broken_prev_count" --format plain
```

**Expected Result**:
```
broken_next_count
0
broken_prev_count
0
```

**Additional Checks**:
```cypher
// Verify chain consistency
MATCH (p:Paragraph:staging)-[:NEXT]->(next:Paragraph:staging)
WHERE NOT (next)-[:PREV]->(p)
RETURN count(p) as inconsistent_chains
```

**Pass Criteria**:
- `broken_next_count = 0`
- `broken_prev_count = 0`
- `inconsistent_chains = 0`

**Rollback Trigger**: No - Warning only (navigation feature, not critical)

### Check 7: Cross-References Valid

**Objective**: Verify that cross-reference data is properly formatted and stored.

**Query**:
```cypher
// Find paragraphs with invalid cross-reference JSON
MATCH (p:Paragraph:staging)
WHERE p.cross_references IS NOT NULL
  AND NOT p.cross_references STARTS WITH '['
RETURN count(p) as invalid_json_count
```

**Command**:
```bash
INVALID_JSON=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph:staging)
   WHERE p.cross_references IS NOT NULL
     AND NOT p.cross_references STARTS WITH '['
   RETURN count(p) as invalid_json_count" --format plain | tail -1)

echo "Invalid cross-reference JSON: $INVALID_JSON"
```

**Expected Result**:
- `invalid_json_count = 0`
- All cross_references are valid JSON arrays or NULL

**Additional Validation**:
```python
# scripts/validate_cross_references.py
import json
from neo4j import GraphDatabase

def validate_cross_references():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    with driver.session() as session:
        result = session.run("""
            MATCH (p:Paragraph:staging)
            WHERE p.cross_references IS NOT NULL
            RETURN p.chunk_id as chunk_id, p.cross_references as refs
            LIMIT 100
        """)

        invalid_count = 0
        for record in result:
            try:
                refs = json.loads(record["refs"])
                if not isinstance(refs, list):
                    print(f"✗ Not a list: {record['chunk_id']}")
                    invalid_count += 1
            except json.JSONDecodeError:
                print(f"✗ Invalid JSON: {record['chunk_id']}")
                invalid_count += 1

    driver.close()

    if invalid_count == 0:
        print("✓ PASS: All cross-references are valid JSON arrays")
        return 0
    else:
        print(f"✗ FAIL: Found {invalid_count} invalid cross-references")
        return 1

if __name__ == "__main__":
    exit(validate_cross_references())
```

**Execution**:
```bash
poetry run python scripts/validate_cross_references.py
```

**Rollback Trigger**: No - Warning only (feature enhancement, not critical)

## Validation Script Runner

**Master Validation Script**:
```bash
# scripts/run_full_validation.sh
#!/bin/bash

set -e

echo "=========================================="
echo "Running Full Validation Checklist"
echo "Version: $1"
echo "=========================================="

VERSION_ID="$1"
PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

# Check 1: SQLite chapter count
echo -e "\n[1/7] Validating SQLite chapter count..."
if poetry run python scripts/validate_chapter_count.py; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "CRITICAL FAILURE - Triggering rollback"
  exit 1
fi

# Check 2: Qdrant-Neo4j count equality
echo -e "\n[2/7] Validating Qdrant-Neo4j count equality..."
if poetry run python scripts/validate_count_equality.py; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "CRITICAL FAILURE - Triggering rollback"
  exit 1
fi

# Check 3: SQLite-Qdrant consistency
echo -e "\n[3/7] Validating SQLite-Qdrant consistency..."
if poetry run python scripts/validate_sqlite_qdrant.py; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "CRITICAL FAILURE - Triggering rollback"
  exit 1
fi

# Check 4: Qdrant-Neo4j chunk_id consistency
echo -e "\n[4/7] Validating Qdrant-Neo4j chunk_id consistency..."
if poetry run python scripts/validate_qdrant_neo4j.py; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "CRITICAL FAILURE - Triggering rollback"
  exit 1
fi

# Check 5: Neo4j parent relationships
echo -e "\n[5/7] Validating Neo4j parent relationships..."
if poetry run python scripts/validate_parent_relationships.py; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "CRITICAL FAILURE - Triggering rollback"
  exit 1
fi

# Check 6: NEXT/PREV chains (warning only)
echo -e "\n[6/7] Validating NEXT/PREV chains..."
if poetry run python scripts/validate_next_prev_chains.py; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  WARN_COUNT=$((WARN_COUNT + 1))
  echo "WARNING - Non-critical failure"
fi

# Check 7: Cross-references (warning only)
echo -e "\n[7/7] Validating cross-references..."
if poetry run python scripts/validate_cross_references.py; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  WARN_COUNT=$((WARN_COUNT + 1))
  echo "WARNING - Non-critical failure"
fi

echo -e "\n=========================================="
echo "Validation Summary"
echo "=========================================="
echo "Passed: $PASS_COUNT"
echo "Failed: $FAIL_COUNT"
echo "Warnings: $WARN_COUNT"

if [ "$FAIL_COUNT" -eq "0" ]; then
  echo -e "\n✓ ALL CRITICAL CHECKS PASSED"
  echo "Version $VERSION_ID ready for commit"
  exit 0
else
  echo -e "\n✗ VALIDATION FAILED"
  echo "Version $VERSION_ID will be rolled back"
  exit 1
fi
```

**Execution**:
```bash
chmod +x scripts/run_full_validation.sh
./scripts/run_full_validation.sh v2_minor_20251226_143022
```

## Validation Reporting

After validation completes, update version_registry:

```sql
-- On success
UPDATE version_registry
SET status = 'committed',
    validation_passed = 1,
    status_message = 'All validation checks passed',
    updated_at = CURRENT_TIMESTAMP
WHERE version_id = 'v2_minor_20251226_143022';

-- On failure
UPDATE version_registry
SET status = 'validating_failed',
    validation_passed = 0,
    status_message = 'Check 4 failed: Qdrant-Neo4j mismatch (35800 vs 36216)',
    updated_at = CURRENT_TIMESTAMP
WHERE version_id = 'v2_minor_20251226_143022';
```

## Related Documentation
- `rollback_procedure.md` - Steps to take on validation failure
- `versioning_spec.md` - Version state transitions
- `migration_monitoring.md` - Real-time monitoring during validation
