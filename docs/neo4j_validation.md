# Neo4j Validation Methods

## Overview

The Neo4jStorage class provides comprehensive validation methods to ensure graph health, integrity, and consistency. These methods help detect data quality issues and synchronization problems between Neo4j and Qdrant.

## Validation Methods

### 1. validate_graph(version_id: Optional[str] = None)

Performs comprehensive validation checks on the Neo4j graph.

**Purpose**: Ensure graph structure and relationships are valid and complete.

**Checks Performed**:

#### Node Count Validation
- **What**: Counts all nodes by type (Textbook, Chapter, Section, Subsection, Subsubsection, Paragraph, Table, Figure)
- **Passing Criteria**: Counts should match expected values based on ingestion
- **Failure Indicator**: Unexpected zeros or significantly different counts than expected

#### Relationship Count Validation
- **What**: Counts all relationships by type (CONTAINS, HAS_SECTION, HAS_SUBSECTION, HAS_SUBSUBSECTION, HAS_PARAGRAPH, NEXT, PREV, CONTAINS_TABLE, CONTAINS_FIGURE)
- **Passing Criteria**:
  - Every Chapter should have exactly 1 CONTAINS relationship from its Textbook
  - Every Section should have exactly 1 HAS_SECTION relationship from its Chapter
  - NEXT and PREV counts should be equal
  - Paragraph count should roughly equal HAS_PARAGRAPH relationship count
- **Failure Indicator**: Mismatched counts or zeros where relationships should exist

#### Orphan Node Detection
- **What**: Finds Paragraph nodes without parent HAS_PARAGRAPH relationships
- **Passing Criteria**: `orphan_paragraphs = 0`
- **Failure Indicator**: `orphan_paragraphs > 0` indicates disconnected paragraphs
- **Impact**: Orphan paragraphs won't appear in hierarchy traversals

#### Broken NEXT/PREV Chain Detection
- **What**: Verifies bidirectional consistency of sequential paragraph relationships
- **Check 1**: For every `(p1)-[:NEXT]->(p2)`, verify `(p2)-[:PREV]->(p1)` exists
- **Check 2**: For every `(p1)-[:PREV]->(p2)`, verify `(p2)-[:NEXT]->(p1)` exists
- **Passing Criteria**: `broken_next_chains = 0` AND `broken_prev_chains = 0`
- **Failure Indicator**: Any broken chains indicate incomplete sequential linking
- **Impact**: Context expansion and sequential navigation will fail

#### Duplicate Chunk ID Detection
- **What**: Identifies Paragraph nodes with duplicate chunk_id values
- **Passing Criteria**: `duplicate_chunk_ids = 0`
- **Failure Indicator**: `duplicate_chunk_ids > 0` indicates data integrity issue
- **Impact**: Unique constraint violations, retrieval ambiguity

#### Invalid Hierarchy Detection
- **What**: Verifies parent-child ID relationships follow hierarchical naming convention
- **Example Check**: Section ID should start with its Chapter ID (e.g., `bailey:ch2:s3` starts with `bailey:ch2`)
- **Passing Criteria**: `invalid_hierarchies = 0`
- **Failure Indicator**: `invalid_hierarchies > 0` indicates mismatched parent-child links
- **Impact**: Hierarchy traversal returns incorrect results

**Overall Status**:
- `"valid"`: All checks pass (all issue counts = 0)
- `"issues_found"`: One or more checks failed

**Return Value**:
```python
{
    "version_id": "v1_baseline",
    "node_counts": {...},
    "relationship_counts": {...},
    "orphan_paragraphs": 0,
    "broken_next_chains": 0,
    "broken_prev_chains": 0,
    "duplicate_chunk_ids": 0,
    "invalid_hierarchies": 0,
    "status": "valid"
}
```

### 2. get_graph_stats(version_id: Optional[str] = None)

Extends validate_graph() with statistical analysis.

**Purpose**: Understand graph content distribution and characteristics.

**Additional Statistics**:

#### Text Length Statistics
- **What**: Analyzes paragraph text length distribution
- **Metrics**:
  - `avg`: Average text length in characters
  - `min`: Shortest paragraph length
  - `max`: Longest paragraph length
- **Passing Criteria**:
  - `avg` should be reasonable (typically 150-500 characters)
  - `min` should be > 0 (no empty paragraphs)
  - `max` should be < 5000 (no excessively long paragraphs)
- **Failure Indicator**:
  - `min = 0` indicates empty text
  - `avg < 50` indicates truncated content
  - `max > 10000` indicates merged paragraphs

#### Top Chapters by Paragraph Count
- **What**: Lists top 10 chapters with most paragraphs
- **Purpose**: Identify content-heavy chapters
- **Passing Criteria**: Distribution should be relatively even
- **Failure Indicator**: One chapter with 10x more paragraphs than others

#### Cross-Reference Statistics
- **What**: Counts paragraphs containing cross-references to figures/tables
- **Purpose**: Measure reference extraction completeness
- **Passing Criteria**: Should be > 0 if source documents contain references
- **Failure Indicator**: `paragraphs_with_cross_references = 0` when references expected

**Return Value**: All validate_graph() fields plus:
```python
{
    ...,
    "text_stats": {
        "avg": 245.3,
        "min": 12,
        "max": 1523
    },
    "top_chapters_by_paragraphs": [
        {"chapter": "60", "paragraphs": 1250},
        {"chapter": "15", "paragraphs": 1100},
        ...
    ],
    "paragraphs_with_cross_references": 3847
}
```

### 3. compare_with_qdrant(qdrant_chunk_ids: Set[str], version_id: Optional[str] = None)

Compares Neo4j paragraphs with Qdrant vectors for consistency.

**Purpose**: Ensure Neo4j and Qdrant databases are synchronized.

**Checks Performed**:

#### Set Intersection Analysis
- **What**: Compares chunk_ids between Neo4j and Qdrant
- **Metrics**:
  - `common_count`: Chunks in both databases
  - `only_in_neo4j`: Chunks missing from Qdrant
  - `only_in_qdrant`: Chunks missing from Neo4j

#### Consistency Status
- **Passing Criteria**: `consistency = "pass"` when:
  - `only_in_neo4j = 0`
  - `only_in_qdrant = 0`
  - `neo4j_count = qdrant_count = common_count`
- **Failure Indicator**: `consistency = "mismatch"` when databases differ

**Common Mismatch Scenarios**:

1. **Chunks only in Neo4j**:
   - Cause: Qdrant upsert failed but Neo4j succeeded
   - Fix: Re-run ingestion for affected chapters with `--force`

2. **Chunks only in Qdrant**:
   - Cause: Neo4j upsert failed but Qdrant succeeded
   - Fix: Re-run ingestion or delete orphan Qdrant vectors

3. **Count mismatch but some overlap**:
   - Cause: Partial ingestion failure
   - Fix: Compare sample chunk_ids to identify affected chapters, re-ingest

**Return Value**:
```python
{
    "neo4j_count": 36221,
    "qdrant_count": 36221,
    "common_count": 36221,
    "only_in_neo4j": 0,
    "only_in_qdrant": 0,
    "consistency": "pass",
    "sample_only_neo4j": [],
    "sample_only_qdrant": []
}
```

## Usage Examples

### Validate Current Graph
```python
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(
    uri=os.getenv('NEO4J_URI'),
    user=os.getenv('NEO4J_USER'),
    password=os.getenv('NEO4J_PASSWORD')
)

# Validate current graph
report = storage.validate_graph()
if report['status'] == 'valid':
    print("Graph validation passed!")
else:
    print(f"Issues found:")
    print(f"  Orphan paragraphs: {report['orphan_paragraphs']}")
    print(f"  Broken chains: {report['broken_next_chains'] + report['broken_prev_chains']}")
```

### Validate Specific Version
```python
# Validate baseline version
report = storage.validate_graph('v1_baseline')
print(f"Baseline status: {report['status']}")
```

### Get Detailed Statistics
```python
stats = storage.get_graph_stats('v1_baseline')
print(f"Average paragraph length: {stats['text_stats']['avg']:.1f} characters")
print(f"Paragraphs with references: {stats['paragraphs_with_cross_references']}")
```

### Compare with Qdrant
```python
from qdrant_client import QdrantClient

# Get all Qdrant chunk_ids
qclient = QdrantClient(host='localhost', port=6333)
all_chunk_ids = set()
offset = None

while True:
    result, offset = qclient.scroll(
        'textbook_chunks',
        limit=1000,
        offset=offset,
        with_payload=True
    )
    if not result:
        break
    all_chunk_ids.update(p.payload.get('chunk_id') for p in result if p.payload.get('chunk_id'))
    if offset is None:
        break

# Compare
comparison = storage.compare_with_qdrant(all_chunk_ids, 'v1_baseline')
if comparison['consistency'] == 'pass':
    print(f"✓ Databases synchronized: {comparison['common_count']} chunks")
else:
    print(f"✗ Mismatch detected:")
    print(f"  Neo4j only: {comparison['only_in_neo4j']}")
    print(f"  Qdrant only: {comparison['only_in_qdrant']}")
    print(f"  Sample mismatches: {comparison['sample_only_neo4j'][:5]}")
```

## Best Practices

1. **Run validation after every ingestion**: Ensure data quality immediately
2. **Validate before creating snapshots**: Don't snapshot corrupted data
3. **Compare with Qdrant regularly**: Catch synchronization issues early
4. **Monitor orphan counts**: Should always be zero in production
5. **Check broken chains**: Critical for context expansion functionality
6. **Review statistics**: Understand content distribution and characteristics
7. **Automate validation**: Include in CI/CD pipeline or scheduled jobs

## Integration with Workflows

### Post-Ingestion Validation
```bash
# After ingestion
poetry run hybridflow ingest-all --force

# Validate immediately
poetry run python -c "
from src.hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv
load_dotenv()
storage = Neo4jStorage(uri=os.getenv('NEO4J_URI'), user=os.getenv('NEO4J_USER'), password=os.getenv('NEO4J_PASSWORD'))
report = storage.validate_graph('v1_baseline')
assert report['status'] == 'valid', f'Validation failed: {report}'
print('✓ Validation passed')
"
```

### Pre-Snapshot Validation
```python
# Before creating snapshot
report = storage.validate_graph('v1_baseline')
if report['status'] != 'valid':
    raise Exception(f"Cannot snapshot invalid graph: {report}")

# Create snapshot only if valid
storage.create_snapshot('v2_snapshot')
```

## Performance Considerations

- **validate_graph()**: ~1-2 seconds for 36K nodes
- **get_graph_stats()**: ~2-3 seconds (includes text analysis)
- **compare_with_qdrant()**: ~5-10 seconds depending on Qdrant collection size

For large graphs (100K+ nodes), consider:
- Running validation during off-peak hours
- Using sampling for statistics
- Caching Qdrant chunk_ids for repeated comparisons
