# Neo4j Versioning Migration Guide

This guide explains how to update existing code to use the new versioning capabilities in Neo4jStorage.

## Overview

All upsert methods in `Neo4jStorage` now support an optional `version_id` parameter that enables multi-version graph support using Neo4j's multi-label capability. This is a **backward-compatible** change - existing code will continue to work without modification.

## What Changed

### Before (Non-Versioned)

```python
storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')

# All nodes created without version labels
storage.upsert_textbook('bailey', 'Bailey & Love')
storage.upsert_chapter('bailey', '60', 'The Thorax', version=1)
storage.upsert_section('bailey:ch60', '2', 'Anatomy')
# ... etc
```

### After (Versioned)

```python
storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')

# All nodes created with :v2_test label
storage.upsert_textbook('bailey', 'Bailey & Love', version_id='v2_test')
storage.upsert_chapter('bailey', '60', 'The Thorax', version=1, version_id='v2_test')
storage.upsert_section('bailey:ch60', '2', 'Anatomy', version_id='v2_test')
# ... etc
```

## Modified Methods

All the following methods now accept an optional `version_id` parameter:

1. `upsert_textbook(textbook_id, name, version_id=None)`
2. `upsert_chapter(textbook_id, chapter_number, title, version, version_id=None)`
3. `upsert_section(chapter_id, section_number, title, version_id=None)`
4. `upsert_subsection(section_id, subsection_number, title, version_id=None)`
5. `upsert_subsubsection(subsection_id, subsubsection_number, title, version_id=None)`
6. `upsert_paragraph(parent_id, paragraph_number, text, chunk_id, page, bounds, cross_references=None, version_id=None)`
7. `upsert_table(paragraph_chunk_id, table_number, description, page, bounds, file_png="", file_xlsx="", version_id=None)`
8. `upsert_figure(paragraph_chunk_id, figure_number, caption, page, bounds, file_png="", version_id=None)`
9. `link_sequential_paragraphs(chapter_id, version_id=None)`

## Migration Scenarios

### Scenario 1: No Changes Required (Continue Using Unversioned Graph)

If you don't need multi-version support, **no code changes are required**. The methods are backward-compatible and will continue creating unversioned nodes.

```python
# This code continues to work exactly as before
storage.upsert_chapter('bailey', '60', 'The Thorax', version=1)
```

### Scenario 2: Migrate Existing Code to Use Versioning

To add versioning support to existing ingestion code:

#### Step 1: Add version_id parameter to all upsert calls

```python
# Before
def ingest_chapter(storage, chapter_data):
    storage.upsert_textbook(chapter_data['textbook_id'], chapter_data['textbook_name'])
    storage.upsert_chapter(
        chapter_data['textbook_id'],
        chapter_data['chapter_number'],
        chapter_data['title'],
        version=1
    )
    # ... more upsert calls

# After
def ingest_chapter(storage, chapter_data, version_id='v1_baseline'):
    storage.upsert_textbook(
        chapter_data['textbook_id'],
        chapter_data['textbook_name'],
        version_id=version_id  # Added
    )
    storage.upsert_chapter(
        chapter_data['textbook_id'],
        chapter_data['chapter_number'],
        chapter_data['title'],
        version=1,
        version_id=version_id  # Added
    )
    # ... more upsert calls with version_id
```

#### Step 2: Update ingestion pipeline

```python
# Before
pipeline = IngestionPipeline(...)
pipeline.ingest_chapter('data/bailey/chapter_60.json')

# After
pipeline = IngestionPipeline(...)
pipeline.ingest_chapter('data/bailey/chapter_60.json', version_id='v2_test')
```

#### Step 3: Register existing graph as baseline (optional)

If you have an existing unversioned graph and want to preserve it as a baseline:

```python
storage = Neo4jStorage(...)

# Register current graph as v1_baseline
baseline_id = storage.register_baseline_graph()
print(f'Baseline: {baseline_id}')  # Outputs: v1_baseline

# Now create a new versioned snapshot
storage.create_snapshot('v2_experiment', source_version='v1_baseline')

# Ingest new data to v2_experiment
storage.upsert_chapter('bailey', '60', 'Updated Title', version=2, version_id='v2_experiment')
```

### Scenario 3: Parallel Versions for A/B Testing

Use versioning to test different ingestion strategies side-by-side:

```python
storage = Neo4jStorage(...)

# Version A: Original processing
for chapter in chapters:
    ingest_chapter(storage, chapter, version_id='v_original_processing')

# Version B: New processing with improvements
for chapter in chapters:
    ingest_chapter_improved(storage, chapter, version_id='v_improved_processing')

# Compare results
stats_original = storage.get_graph_stats('v_original_processing')
stats_improved = storage.get_graph_stats('v_improved_processing')

print(f"Original: {stats_original['node_counts']['Paragraph']} paragraphs")
print(f"Improved: {stats_improved['node_counts']['Paragraph']} paragraphs")

# Choose winner and restore
if stats_improved['status'] == 'valid':
    storage.restore_snapshot('v_improved_processing')
```

### Scenario 4: Incremental Migration

Migrate to versioning gradually by adding version support to one component at a time:

```python
# Phase 1: Add version support to ingestion only
def ingest_chapter(storage, chapter_data, version_id=None):
    storage.upsert_chapter(..., version_id=version_id)
    # Other methods still unversioned

# Phase 2: Add version support to retrieval
def get_chapter_hierarchy(storage, chapter_id, version_id=None):
    # Use version_id in queries
    pass

# Phase 3: Full versioning across all operations
```

## Common Patterns

### Pattern 1: Version Naming Convention

Use descriptive version identifiers:

```python
# Good
version_id = 'v2_cross_ref_fix'
version_id = 'v1_baseline'
version_id = 'v3_hierarchy_improvements'

# Avoid
version_id = 'test'
version_id = 'new'
version_id = 'v1'
```

### Pattern 2: Centralized Version Management

Create a configuration module for version management:

```python
# config/versions.py
CURRENT_VERSION = 'v2_production'
BASELINE_VERSION = 'v1_baseline'
EXPERIMENT_VERSION = 'v3_experiment'

# usage
from config.versions import CURRENT_VERSION

storage.upsert_chapter(..., version_id=CURRENT_VERSION)
```

### Pattern 3: Version Isolation

Ensure all related operations use the same version_id:

```python
def ingest_and_link(storage, chapter_data, version_id):
    # Upsert hierarchy
    storage.upsert_chapter(..., version_id=version_id)
    storage.upsert_section(..., version_id=version_id)
    storage.upsert_paragraph(..., version_id=version_id)

    # Link paragraphs - MUST use same version_id
    storage.link_sequential_paragraphs(chapter_id, version_id=version_id)

    # Validate - MUST use same version_id
    report = storage.validate_graph(version_id)
```

### Pattern 4: Version-Aware Queries

When querying versioned graphs, always specify the version:

```python
# Without versioning (queries all nodes)
storage.get_chapter_hierarchy('bailey:ch60')

# With versioning (queries specific version)
# Note: get_chapter_hierarchy doesn't support version_id yet
# You need to use custom Cypher queries:

with storage.driver.session() as session:
    result = session.run('''
        MATCH (c:Chapter:v2_test {id: $chapter_id})
        MATCH (c)-[:HAS_SECTION]->(s:Section:v2_test)
        RETURN c, collect(s) as sections
    ''', chapter_id='bailey:ch60')
```

## Testing Version Migrations

### Test 1: Verify Backward Compatibility

```python
def test_backward_compatibility():
    """Ensure old code still works without version_id."""
    storage = Neo4jStorage(...)

    # Old-style calls (no version_id)
    storage.upsert_textbook('test', 'Test Book')
    storage.upsert_chapter('test', '1', 'Chapter 1', version=1)

    # Verify unversioned nodes created
    with storage.driver.session() as session:
        result = session.run('MATCH (c:Chapter {id: "test:ch1"}) RETURN c')
        assert result.single() is not None
```

### Test 2: Verify Version Isolation

```python
def test_version_isolation():
    """Ensure versioned nodes don't interfere with each other."""
    storage = Neo4jStorage(...)

    # Create v1 version
    storage.upsert_chapter('test', '1', 'V1 Title', version=1, version_id='v1')

    # Create v2 version
    storage.upsert_chapter('test', '1', 'V2 Title', version=2, version_id='v2')

    # Verify both exist independently
    with storage.driver.session() as session:
        result_v1 = session.run('MATCH (c:Chapter:v1 {id: "test:ch1"}) RETURN c.title as title')
        result_v2 = session.run('MATCH (c:Chapter:v2 {id: "test:ch1"}) RETURN c.title as title')

        assert result_v1.single()['title'] == 'V1 Title'
        assert result_v2.single()['title'] == 'V2 Title'
```

### Test 3: Verify Complete Hierarchy

```python
def test_complete_versioned_hierarchy():
    """Test full hierarchy with versioning."""
    storage = Neo4jStorage(...)
    version_id = 'v_test'

    storage.upsert_textbook('test', 'Test', version_id=version_id)
    storage.upsert_chapter('test', '1', 'Chapter', version=1, version_id=version_id)
    storage.upsert_section('test:ch1', '1', 'Section', version_id=version_id)
    storage.upsert_paragraph('test:ch1:s1', '1', 'Text', 'test:ch1:1', 1, [0,0,100,100], version_id=version_id)

    # Validate
    report = storage.validate_graph(version_id)
    assert report['status'] == 'valid'
    assert report['node_counts']['Chapter'] == 1
    assert report['node_counts']['Paragraph'] == 1
```

## Troubleshooting

### Issue 1: Nodes Created Without Version Label

**Problem**: Called upsert method with `version_id` but nodes don't have version label.

**Cause**: Passed `version_id=None` or empty string.

**Solution**: Ensure version_id is a non-empty string:

```python
# Wrong
version_id = None
storage.upsert_chapter(..., version_id=version_id)

# Correct
version_id = 'v2_test'
storage.upsert_chapter(..., version_id=version_id)
```

### Issue 2: Cannot Find Versioned Nodes

**Problem**: Query returns no results for versioned nodes.

**Cause**: Query doesn't include version label filter.

**Solution**: Add version label to query:

```python
# Wrong (queries all nodes regardless of version)
MATCH (c:Chapter {id: 'bailey:ch60'}) RETURN c

# Correct (queries specific version)
MATCH (c:Chapter:v2_test {id: 'bailey:ch60'}) RETURN c
```

### Issue 3: Mixed Versioned/Unversioned Nodes

**Problem**: Some nodes have version labels, others don't.

**Cause**: Inconsistent use of `version_id` parameter across upsert calls.

**Solution**: Ensure all related operations use the same `version_id`:

```python
# Wrong - chapter has version, section doesn't
storage.upsert_chapter('bailey', '60', 'Thorax', version=1, version_id='v2_test')
storage.upsert_section('bailey:ch60', '2', 'Anatomy')  # Missing version_id!

# Correct - both have same version
version_id = 'v2_test'
storage.upsert_chapter('bailey', '60', 'Thorax', version=1, version_id=version_id)
storage.upsert_section('bailey:ch60', '2', 'Anatomy', version_id=version_id)
```

### Issue 4: Relationships Between Versions

**Problem**: Relationship created between nodes of different versions.

**Cause**: Parent node has different version than child node.

**Solution**: Always use the same `version_id` for hierarchically related nodes:

```python
# Ensure entire hierarchy uses same version
def ingest_hierarchy(storage, data, version_id):
    storage.upsert_textbook(data['textbook_id'], data['name'], version_id=version_id)
    storage.upsert_chapter(..., version_id=version_id)
    storage.upsert_section(..., version_id=version_id)
    storage.upsert_subsection(..., version_id=version_id)
    storage.upsert_paragraph(..., version_id=version_id)
```

## Best Practices

1. **Consistent Version Naming**: Use descriptive, timestamped version IDs (e.g., `v2_2025_03_15_hierarchy_fix`)

2. **Version Documentation**: Document what each version represents in a central registry

3. **Atomic Operations**: When creating a versioned graph, complete the entire ingestion before validating

4. **Validation**: Always run `validate_graph(version_id)` after creating a new version

5. **Cleanup**: Delete old experimental versions using `delete_snapshot(version_id)` to prevent database bloat

6. **Baseline Preservation**: Keep a `v1_baseline` version as a reference point

7. **Testing**: Test version isolation to ensure changes in one version don't affect others

## Next Steps

1. Review the [Neo4j Validation Guide](./neo4j_validation.md) for version-aware validation
2. See [Troubleshooting Guide](./troubleshooting.md) for common version-related issues
3. Check [Performance Guide](./performance.md) for version label query optimization

## Example: Complete Migration

```python
from hybridflow.storage.neo4j_client import Neo4jStorage
import os
from dotenv import load_dotenv

load_dotenv()
storage = Neo4jStorage(
    uri=os.getenv('NEO4J_URI'),
    user=os.getenv('NEO4J_USER'),
    password=os.getenv('NEO4J_PASSWORD')
)

# Step 1: Register existing graph as baseline
baseline = storage.register_baseline_graph()
print(f'Registered baseline: {baseline}')

# Step 2: Create experimental version
storage.create_snapshot('v2_experiment', source_version='v1_baseline')

# Step 3: Modify experimental version
storage.upsert_chapter(
    'bailey', '60', 'Updated Title',
    version=2,
    version_id='v2_experiment'
)

# Step 4: Validate experimental version
report = storage.validate_graph('v2_experiment')
if report['status'] == 'valid':
    print('✓ Experiment valid')

    # Step 5: Promote to production if successful
    storage.restore_snapshot('v2_experiment')
else:
    print('✗ Experiment failed validation')
    storage.delete_snapshot('v2_experiment')

storage.close()
```
