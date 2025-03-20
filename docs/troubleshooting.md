# Neo4j Validation Troubleshooting Guide

This guide covers common validation failures detected by the `validate-neo4j` CLI command and the underlying validation methods in `Neo4jStorage`. Each section describes the issue, root causes, and step-by-step remediation procedures.

## Table of Contents

1. [Orphan Paragraphs](#orphan-paragraphs)
2. [Broken NEXT/PREV Chains](#broken-nextprev-chains)
3. [Duplicate Chunk IDs](#duplicate-chunk-ids)
4. [Invalid Hierarchies](#invalid-hierarchies)
5. [Cross-Database Consistency Issues](#cross-database-consistency-issues)
6. [Prevention Strategies](#prevention-strategies)
7. [Emergency Recovery Procedures](#emergency-recovery-procedures)

---

## Orphan Paragraphs

### Symptoms

```
Orphan Paragraphs: 150
Status: ISSUES_FOUND
```

**What it means**: Paragraph nodes exist in Neo4j without parent `HAS_PARAGRAPH` relationships, making them unreachable through hierarchy traversal.

### Root Causes

1. **Parent node creation failure**: Section/Subsection/Subsubsection nodes weren't created before paragraphs
2. **MATCH instead of MERGE**: Using `MATCH` for parent lookup fails silently if parent doesn't exist
3. **Incorrect parent ID**: Paragraph upsert references wrong parent_id
4. **Partial ingestion failure**: Pipeline crashed after creating paragraphs but before linking relationships

### Diagnosis

Identify which paragraphs are orphaned:

```bash
poetry run python -c "
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

with storage.driver.session() as session:
    result = session.run('''
        MATCH (p:Paragraph)
        WHERE NOT EXISTS((p)<-[:HAS_PARAGRAPH]-())
        RETURN p.chunk_id as chunk_id, p.number as number
        LIMIT 20
    ''')
    for record in result:
        print(f\"Orphan: {record['chunk_id']} (paragraph {record['number']})\")

storage.close()
"
```

### Remediation

#### Option 1: Re-ingest Affected Chapters (Recommended)

```bash
# Identify affected chapters from chunk_ids (e.g., bailey:ch60:2.4.1)
# Extract chapter: bailey:ch60

# Re-ingest with --force flag
poetry run hybridflow ingest-file data/bailey/chapter_60.json --force
```

#### Option 2: Manual Link Creation (Advanced)

If you know the correct parent IDs:

```python
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

orphan_chunk_id = "bailey:ch60:2.4.1"
parent_id = "bailey:ch60:s2:ss2.4"  # Derive from paragraph number

with storage.driver.session() as session:
    session.run("""
        MATCH (parent {id: $parent_id})
        MATCH (p:Paragraph {chunk_id: $chunk_id})
        MERGE (parent)-[:HAS_PARAGRAPH]->(p)
    """, parent_id=parent_id, chunk_id=orphan_chunk_id)

storage.close()
```

#### Option 3: Nuclear Option

```bash
# Delete all data and re-ingest
docker compose down -v
docker compose up -d
poetry run hybridflow ingest-all
```

### Prevention

1. **Use MERGE not MATCH** in upsert methods:
   ```python
   # WRONG
   MATCH (parent:Section {id: $parent_id})

   # CORRECT
   MERGE (parent:Section {id: $parent_id})
   ```

2. **Create hierarchy nodes before paragraphs** in ingestion pipeline

3. **Validate parent existence** before upserting paragraphs:
   ```python
   with self.driver.session() as session:
       result = session.run("MATCH (n {id: $parent_id}) RETURN count(n) as count", parent_id=parent_id)
       if result.single()["count"] == 0:
           raise ValueError(f"Parent node {parent_id} does not exist")
   ```

---

## Broken NEXT/PREV Chains

### Symptoms

```
Broken NEXT Chains: 25
Broken PREV Chains: 18
Status: ISSUES_FOUND
```

**What it means**: Sequential paragraph relationships are inconsistent - `(p1)-[:NEXT]->(p2)` exists but `(p2)-[:PREV]->(p1)` is missing (or vice versa).

### Root Causes

1. **Unidirectional relationship creation**: Code creates NEXT but forgets PREV (or vice versa)
2. **Transaction rollback**: One relationship committed but not the other
3. **Duplicate paragraph creation**: Multiple paragraphs with same number create ambiguous chains
4. **Manual deletion**: User deleted a paragraph in the middle of a chain

### Diagnosis

Find broken chains:

```bash
poetry run python -c "
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

with storage.driver.session() as session:
    # Find NEXT without matching PREV
    result = session.run('''
        MATCH (p1:Paragraph)-[:NEXT]->(p2:Paragraph)
        WHERE NOT EXISTS((p2)-[:PREV]->(p1))
        RETURN p1.chunk_id as chunk1, p2.chunk_id as chunk2
        LIMIT 10
    ''')
    print('NEXT without PREV:')
    for record in result:
        print(f\"  {record['chunk1']} -> {record['chunk2']}\")

    # Find PREV without matching NEXT
    result = session.run('''
        MATCH (p1:Paragraph)-[:PREV]->(p2:Paragraph)
        WHERE NOT EXISTS((p2)-[:NEXT]->(p1))
        RETURN p1.chunk_id as chunk1, p2.chunk_id as chunk2
        LIMIT 10
    ''')
    print('\\nPREV without NEXT:')
    for record in result:
        print(f\"  {record['chunk1']} -> {record['chunk2']}\")

storage.close()
"
```

### Remediation

#### Option 1: Rebuild All Sequential Relationships (Recommended)

```python
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

with storage.driver.session() as session:
    # Delete all NEXT/PREV relationships
    session.run("MATCH ()-[r:NEXT|PREV]->() DELETE r")

    # Rebuild from paragraph ordering within each parent
    session.run("""
        MATCH (parent)-[:HAS_PARAGRAPH]->(p:Paragraph)
        WITH parent, p ORDER BY p.number
        WITH parent, collect(p) as paragraphs
        UNWIND range(0, size(paragraphs)-2) as i
        WITH paragraphs[i] as p1, paragraphs[i+1] as p2
        MERGE (p1)-[:NEXT]->(p2)
        MERGE (p2)-[:PREV]->(p1)
    """)

storage.close()
```

#### Option 2: Re-ingest Affected Chapters

```bash
# Identify affected chapters from broken chunk_ids
poetry run hybridflow ingest-file data/bailey/chapter_60.json --force
```

### Prevention

1. **Always create bidirectional relationships**:
   ```python
   # Create both NEXT and PREV in same transaction
   MERGE (p1)-[:NEXT]->(p2)
   MERGE (p2)-[:PREV]->(p1)
   ```

2. **Use transactions** to ensure atomicity:
   ```python
   with self.driver.session() as session:
       with session.begin_transaction() as tx:
           tx.run("MERGE (p1)-[:NEXT]->(p2)", ...)
           tx.run("MERGE (p2)-[:PREV]->(p1)", ...)
           tx.commit()
   ```

3. **Validate after creation**:
   ```python
   # After creating sequential relationships, verify bidirectionality
   result = session.run("""
       MATCH (p1)-[:NEXT]->(p2)
       WHERE NOT EXISTS((p2)-[:PREV]->(p1))
       RETURN count(p1) as broken
   """)
   assert result.single()["broken"] == 0
   ```

---

## Duplicate Chunk IDs

### Symptoms

```
Duplicate Chunk IDs: 8
Status: ISSUES_FOUND
```

**What it means**: Multiple Paragraph nodes have the same `chunk_id` value, violating uniqueness constraints.

### Root Causes

1. **Ingestion bug**: Same paragraph ingested multiple times without deduplication
2. **Constraint temporarily dropped**: Uniqueness constraint was dropped and duplicates were created
3. **Manual node creation**: User created duplicate nodes via Cypher
4. **Snapshot corruption**: Snapshot restore created duplicates

### Diagnosis

Find duplicate chunk_ids:

```bash
poetry run python -c "
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

with storage.driver.session() as session:
    result = session.run('''
        MATCH (p:Paragraph)
        WITH p.chunk_id as chunk_id, collect(p) as nodes
        WHERE size(nodes) > 1
        RETURN chunk_id, size(nodes) as count
        ORDER BY count DESC
        LIMIT 20
    ''')
    print('Duplicate chunk_ids:')
    for record in result:
        print(f\"  {record['chunk_id']}: {record['count']} copies\")

storage.close()
"
```

### Remediation

#### Option 1: Delete Duplicates (Recommended)

Keep the most recently modified node:

```python
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

with storage.driver.session() as session:
    # For each duplicate chunk_id, delete all but one
    session.run("""
        MATCH (p:Paragraph)
        WITH p.chunk_id as chunk_id, collect(p) as nodes
        WHERE size(nodes) > 1
        WITH chunk_id, nodes[1..] as duplicates
        UNWIND duplicates as dup
        DETACH DELETE dup
    """)

    # Verify no duplicates remain
    result = session.run("""
        MATCH (p:Paragraph)
        WITH p.chunk_id as chunk_id, count(p) as count
        WHERE count > 1
        RETURN count(chunk_id) as remaining_duplicates
    """)
    print(f"Remaining duplicates: {result.single()['remaining_duplicates']}")

storage.close()
```

#### Option 2: Nuclear Option

```bash
# Clear database and re-ingest
docker compose down -v
docker compose up -d
poetry run hybridflow ingest-all
```

### Prevention

1. **Create uniqueness constraint** on chunk_id:
   ```python
   session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paragraph) REQUIRE p.chunk_id IS UNIQUE")
   ```

2. **Use MERGE with unique property**:
   ```python
   # MERGE on chunk_id prevents duplicates
   MERGE (p:Paragraph {chunk_id: $chunk_id})
   SET p.text = $text, p.number = $number, p.page = $page
   ```

3. **Check before inserting**:
   ```python
   result = session.run("MATCH (p:Paragraph {chunk_id: $chunk_id}) RETURN count(p) as count", chunk_id=chunk_id)
   if result.single()["count"] > 0:
       logger.warning(f"Duplicate detected: {chunk_id}")
   ```

---

## Invalid Hierarchies

### Symptoms

```
Invalid Hierarchies: 42
Status: ISSUES_FOUND
```

**What it means**: Parent-child ID relationships don't follow hierarchical naming convention (e.g., Section ID doesn't start with its Chapter ID).

### Root Causes

1. **Incorrect ID construction**: Building IDs from `chapter_id` instead of `parent_id`
2. **Wrong parent assigned**: Subsection linked to wrong Section
3. **ID format inconsistency**: Using different separators or formats

### Diagnosis

Find invalid hierarchies:

```bash
poetry run python -c "
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

with storage.driver.session() as session:
    # Check Section -> Subsection hierarchies
    result = session.run('''
        MATCH (s:Section)-[:HAS_SUBSECTION]->(ss:Subsection)
        WHERE NOT ss.id STARTS WITH s.id
        RETURN s.id as parent_id, ss.id as child_id
        LIMIT 20
    ''')
    print('Invalid Section -> Subsection hierarchies:')
    for record in result:
        print(f\"  Parent: {record['parent_id']}, Child: {record['child_id']}\")

storage.close()
"
```

### Remediation

#### Option 1: Re-ingest with Correct ID Construction

**Fix the ID construction bug first**:

```python
# WRONG - builds from chapter_id
subsection_id = f"{chapter_id}:ss{subsection_number}"

# CORRECT - builds from parent_id (section_id)
subsection_id = f"{section_id}:ss{subsection_number}"
```

Then re-ingest:

```bash
poetry run hybridflow ingest-all --force
```

#### Option 2: Manual Relationship Fixing (Advanced)

Only if the IDs themselves are correct but relationships are wrong:

```python
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

with storage.driver.session() as session:
    # Delete incorrect relationships
    session.run("""
        MATCH (s:Section)-[r:HAS_SUBSECTION]->(ss:Subsection)
        WHERE NOT ss.id STARTS WITH s.id
        DELETE r
    """)

    # Recreate correct relationships based on ID prefixes
    session.run("""
        MATCH (s:Section), (ss:Subsection)
        WHERE ss.id STARTS WITH s.id + ':'
        MERGE (s)-[:HAS_SUBSECTION]->(ss)
    """)

storage.close()
```

### Prevention

1. **Follow hierarchical ID construction**:
   ```python
   # Correct pattern
   textbook_id = "bailey"
   chapter_id = f"{textbook_id}:ch{chapter_number}"
   section_id = f"{chapter_id}:s{section_number}"
   subsection_id = f"{section_id}:ss{subsection_number}"  # NOT f"{chapter_id}:ss{...}"
   subsubsection_id = f"{subsection_id}:sss{subsubsection_number}"
   ```

2. **Validate IDs before upserting**:
   ```python
   def validate_hierarchy_id(child_id: str, parent_id: str) -> None:
       if not child_id.startswith(parent_id + ":"):
           raise ValueError(f"Invalid hierarchy: {child_id} not under {parent_id}")
   ```

3. **Test ID construction**:
   ```python
   def test_id_construction():
       chapter_id = "bailey:ch60"
       section_id = "bailey:ch60:s2"
       subsection_id = "bailey:ch60:s2:ss2.4"

       assert subsection_id.startswith(section_id + ":")
       assert subsection_id.startswith(chapter_id + ":")
   ```

---

## Cross-Database Consistency Issues

### Symptoms

```
Neo4j Count:    36221
Qdrant Count:   36185
Common Count:   36180
Only in Neo4j:  41
Only in Qdrant: 5
Consistency:    MISMATCH
```

**What it means**: Neo4j and Qdrant databases have different sets of chunk_ids, indicating synchronization failure.

### Root Causes

1. **Partial ingestion failure**: Qdrant upsert succeeded but Neo4j failed (or vice versa)
2. **Manual deletion**: User deleted from one database but not the other
3. **Ingestion pipeline bug**: Not upserting to both databases atomically
4. **Network failure**: Connection lost mid-ingestion

### Diagnosis

#### Identify Mismatched Chunks

```bash
poetry run hybridflow validate-neo4j --compare-qdrant --output mismatch_report.json
```

Review the JSON report's `qdrant_comparison.sample_only_neo4j` and `qdrant_comparison.sample_only_qdrant` fields.

#### Extract Chapter Identifiers

```python
import json

with open('mismatch_report.json') as f:
    report = json.load(f)

# Chunks only in Neo4j
neo4j_only = report['qdrant_comparison']['sample_only_neo4j']
affected_chapters = set()
for chunk_id in neo4j_only:
    # Extract chapter from chunk_id like "bailey:ch60:2.4.1"
    parts = chunk_id.split(':')
    if len(parts) >= 2:
        chapter = f"{parts[0]}:ch{parts[1].replace('ch', '')}"
        affected_chapters.add(chapter)

print(f"Chapters with Neo4j-only chunks: {affected_chapters}")

# Repeat for Qdrant-only chunks
```

### Remediation

#### Option 1: Re-ingest Affected Chapters (Recommended)

```bash
# For each affected chapter, re-ingest with --force
poetry run hybridflow ingest-file data/bailey/chapter_60.json --force
poetry run hybridflow ingest-file data/sabiston/chapter_15.json --force
```

#### Option 2: Manual Synchronization

**Add missing chunks to Qdrant**:

```python
from src.hybridflow.storage.neo4j_client import Neo4jStorage
from src.hybridflow.storage.qdrant_client import QdrantStorage
from src.hybridflow.embeddings import EmbeddingService
import os
from dotenv import load_dotenv

load_dotenv()
neo4j = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))
qdrant = QdrantStorage(host=os.getenv('QDRANT_HOST'), port=int(os.getenv('QDRANT_PORT')))
embedder = EmbeddingService(model=os.getenv('EMBEDDING_MODEL'))

# Get chunks only in Neo4j
with neo4j.driver.session() as session:
    result = session.run("MATCH (p:Paragraph {chunk_id: $chunk_id}) RETURN p.text as text", chunk_id="bailey:ch60:2.4.1")
    record = result.single()
    if record:
        text = record['text']
        embedding = embedder.encode(text)
        qdrant.upsert_chunk(chunk_id="bailey:ch60:2.4.1", embedding=embedding, metadata={"text": text})

neo4j.close()
qdrant.close()
```

**Delete orphan chunks from Qdrant**:

```python
from qdrant_client import QdrantClient
import os
from dotenv import load_dotenv

load_dotenv()
client = QdrantClient(host=os.getenv('QDRANT_HOST'), port=int(os.getenv('QDRANT_PORT')))

orphan_chunk_ids = ["bailey:ch60:99.99.99"]  # From validation report

# Delete by filtering on chunk_id
for chunk_id in orphan_chunk_ids:
    client.delete(
        collection_name="textbook_chunks",
        points_selector=models.FilterSelector(
            filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="chunk_id",
                        match=models.MatchValue(value=chunk_id),
                    )
                ]
            )
        ),
    )
```

#### Option 3: Nuclear Option

```bash
# Clear both databases and re-ingest
docker compose down -v
docker compose up -d
rm -f metadata.db
poetry run hybridflow ingest-all
```

### Prevention

1. **Atomic upserts**: Wrap Neo4j and Qdrant upserts in try-except with rollback:
   ```python
   try:
       neo4j_storage.upsert_paragraph(...)
       qdrant_storage.upsert_chunk(...)
   except Exception as e:
       # Rollback Neo4j changes
       logger.error(f"Failed to sync databases for {chunk_id}")
       raise
   ```

2. **Validation after ingestion**:
   ```bash
   poetry run hybridflow ingest-all
   poetry run hybridflow validate-neo4j --compare-qdrant
   ```

3. **Transaction logs**: Log all upserts to detect partial failures:
   ```python
   logger.info(f"Upserting {chunk_id} to Neo4j...")
   neo4j_storage.upsert_paragraph(...)
   logger.info(f"Upserting {chunk_id} to Qdrant...")
   qdrant_storage.upsert_chunk(...)
   logger.info(f"Successfully synced {chunk_id}")
   ```

---

## Prevention Strategies

### 1. Automated Validation in CI/CD

Add validation to your ingestion workflow:

```bash
#!/bin/bash
set -e

# Ingest data
poetry run hybridflow ingest-all --force

# Validate immediately
poetry run hybridflow validate-neo4j --compare-qdrant --output validation_report.json

# Check exit code
if [ $? -ne 0 ]; then
    echo "Validation failed - check validation_report.json"
    exit 1
fi

echo "Ingestion and validation successful"
```

### 2. Pre-Commit Validation

Before creating snapshots or deploying:

```python
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

# Validate before snapshot
report = storage.validate_graph('v1_baseline')
if report['status'] != 'valid':
    raise Exception(f"Cannot snapshot invalid graph: {report}")

# Safe to proceed
storage.create_snapshot('v2_snapshot')
storage.close()
```

### 3. Monitoring and Alerts

Set up periodic validation checks:

```python
import schedule
import time
from src.hybridflow.storage.neo4j_client import Neo4jStorage

def validate_and_alert():
    storage = Neo4jStorage(...)
    report = storage.validate_graph()

    if report['status'] == 'issues_found':
        # Send alert (email, Slack, etc.)
        send_alert(f"Neo4j validation failed: {report}")

    storage.close()

schedule.every().day.at("02:00").do(validate_and_alert)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### 4. Schema Enforcement

Create Neo4j constraints to prevent invalid data:

```python
def create_constraints(self) -> None:
    """Create all necessary constraints for data integrity."""
    with self.driver.session() as session:
        # Uniqueness constraints
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Textbook) REQUIRE t.id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chapter) REQUIRE c.id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:Section) REQUIRE s.id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (ss:Subsection) REQUIRE ss.id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (sss:Subsubsection) REQUIRE sss.id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paragraph) REQUIRE p.chunk_id IS UNIQUE")

        # Existence constraints (Neo4j Enterprise only)
        # session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paragraph) REQUIRE p.text IS NOT NULL")
```

---

## Emergency Recovery Procedures

### Complete Database Wipe and Re-ingestion

When validation shows extensive corruption (>5% failure rate):

```bash
#!/bin/bash
set -e

echo "WARNING: This will delete all data. Press Ctrl+C to cancel."
sleep 5

# Stop containers
docker compose down -v

# Start fresh
docker compose up -d

# Wait for services to be ready
sleep 10

# Delete local metadata
rm -f metadata.db

# Re-ingest all data
poetry run hybridflow ingest-all --force

# Validate
poetry run hybridflow validate-neo4j --compare-qdrant --output final_validation.json

echo "Recovery complete - check final_validation.json"
```

### Snapshot Rollback

If recent changes caused corruption:

```python
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

# List available snapshots
snapshots = storage.list_snapshots()
print("Available snapshots:")
for snapshot in snapshots:
    print(f"  - {snapshot['version_id']}: {snapshot['total_nodes']} nodes")

# Restore last known good snapshot
storage.restore_snapshot('v1_baseline')

# Validate
report = storage.validate_graph('v1_baseline')
assert report['status'] == 'valid', f"Restored snapshot is invalid: {report}"

storage.close()
```

### Selective Chapter Re-ingestion

When only specific chapters are corrupted:

```python
# Identify corrupted chapters from validation report
corrupted_chapters = ["bailey:ch60", "schwartz:ch15"]

# Map to file paths
chapter_files = {
    "bailey:ch60": "data/bailey/chapter_60.json",
    "schwartz:ch15": "data/schwartz/chapter_15.json",
}

# Delete corrupted data
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))

for chapter_id in corrupted_chapters:
    with storage.driver.session() as session:
        # Delete chapter and all descendants
        session.run("""
            MATCH (c:Chapter {id: $chapter_id})
            OPTIONAL MATCH (c)-[*]->(descendant)
            DETACH DELETE c, descendant
        """, chapter_id=chapter_id)

storage.close()

# Re-ingest from files
import subprocess
for chapter_id, file_path in chapter_files.items():
    subprocess.run(["poetry", "run", "hybridflow", "ingest-file", file_path, "--force"], check=True)
```

---

## Getting Help

If you encounter validation failures not covered in this guide:

1. **Generate detailed report**:
   ```bash
   poetry run hybridflow validate-neo4j --compare-qdrant --output debug_report.json -v
   ```

2. **Check logs** for ingestion errors:
   ```bash
   grep ERROR hybridflow.log
   ```

3. **Query Neo4j directly** via Neo4j Browser (http://localhost:7474) to inspect graph structure

4. **Export sample data** for debugging:
   ```python
   with storage.driver.session() as session:
       result = session.run("MATCH (p:Paragraph) RETURN p LIMIT 10")
       for record in result:
           print(record['p'])
   ```

5. **Compare validation methods** to ensure consistency:
   ```python
   report = storage.validate_graph()
   stats = storage.get_graph_stats()

   assert report['node_counts'] == stats['node_counts']
   ```
