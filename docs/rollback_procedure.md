# Rollback Procedure

## Overview
This document defines the step-by-step procedure for rolling back a failed version deployment across all three database systems (SQLite, Qdrant, Neo4j).

## When to Initiate Rollback
Refer to `migration_plan.md` for complete rollback trigger conditions. Common triggers:
- Validation check failure
- Data corruption detected
- Ingestion pipeline errors
- User-initiated rollback request

## Prerequisites
- Access to `version_registry` table
- Docker containers running (Qdrant, Neo4j)
- `latest-copy` backup verified as intact
- Sufficient disk space for temporary operations

## Rollback Steps

### Step 1: Identify Failed Version
**Objective**: Locate the failed version in the version registry and confirm rollback target.

```bash
# Query version registry for failed/staging versions
sqlite3 metadata.db "SELECT version_id, status, status_message, created_at
  FROM version_registry
  WHERE status IN ('staging', 'validating', 'pending')
  ORDER BY created_at DESC
  LIMIT 5;"
```

**Verification**:
- Confirm the version_id to rollback (e.g., `v2_minor_20251226_143022`)
- Verify the version is NOT `v1_baseline` (never rollback baseline)
- Check status_message for failure reason

**Output Example**:
```
v2_minor_20251226_143022|validating|Chunk count mismatch detected|2025-12-26 14:30:22
```

### Step 2: Update Status to 'rolling_back'
**Objective**: Mark the version as rolling back to prevent concurrent operations.

```bash
# Update version status
sqlite3 metadata.db "UPDATE version_registry
  SET status = 'rolling_back',
      updated_at = CURRENT_TIMESTAMP,
      status_message = 'Rollback initiated due to: [REASON]'
  WHERE version_id = 'v2_minor_20251226_143022';"

# Log rollback operation
sqlite3 metadata.db "INSERT INTO operation_log
  (version_id, operation_type, status, details)
  VALUES ('v2_minor_20251226_143022', 'rollback', 'in_progress', 'Starting rollback procedure');"
```

**Verification**:
```bash
# Confirm status update
sqlite3 metadata.db "SELECT version_id, status, status_message
  FROM version_registry
  WHERE version_id = 'v2_minor_20251226_143022';"
```

**Expected Output**:
```
v2_minor_20251226_143022|rolling_back|Rollback initiated due to: [REASON]
```

### Step 3: Drop SQLite Staging Table
**Objective**: Remove staging table to clean up failed ingestion data.

```bash
# Check if staging table exists
sqlite3 metadata.db ".tables chapter_metadata_staging"

# Drop staging table
sqlite3 metadata.db "DROP TABLE IF EXISTS chapter_metadata_staging;"

# Verify deletion
sqlite3 metadata.db ".tables chapter_metadata_staging"
```

**Verification**:
- Command should return empty (table not found)
- No errors during DROP operation

**Rollback Impact**:
- Staging data permanently deleted
- Production `chapter_metadata` table unaffected

### Step 4: Delete Qdrant Staging Collection
**Objective**: Remove staging collection from Qdrant vector database.

```bash
# Check if staging collection exists
curl -X GET "http://localhost:6333/collections/textbook_chunks_staging" 2>/dev/null | python3 -m json.tool

# Delete staging collection
curl -X DELETE "http://localhost:6333/collections/textbook_chunks_staging" 2>/dev/null | python3 -m json.tool

# Verify deletion
curl -X GET "http://localhost:6333/collections" 2>/dev/null | python3 -m json.tool | grep -v "textbook_chunks_staging"
```

**Verification**:
```json
{
  "result": {
    "collections": [
      {
        "name": "textbook_chunks"
      }
    ]
  },
  "status": "ok"
}
```

**Expected Behavior**:
- Staging collection deleted successfully
- Production `textbook_chunks` collection intact
- No impact on production queries

### Step 5: Delete Neo4j Staging Nodes
**Objective**: Remove all nodes labeled with `:staging` from Neo4j graph database.

```bash
# Count staging nodes before deletion
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:staging) RETURN count(n) as staging_count" --format plain

# Delete all staging nodes and relationships
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:staging) DETACH DELETE n"

# Verify deletion
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (n:staging) RETURN count(n) as remaining_staging" --format plain
```

**Verification**:
- `remaining_staging` should be 0
- Production nodes (without `:staging` label) unaffected

**Performance Note**:
- Large staging datasets (>10K nodes) may take 30-60 seconds to delete
- Use `CALL apoc.periodic.iterate()` for batched deletion if needed

### Step 6: Verify latest-copy Intact
**Objective**: Confirm that the latest-copy backup is available and valid.

```bash
# Check latest-copy files exist
ls -lh backups/latest-copy/

# Verify metadata.db integrity
sqlite3 backups/latest-copy/metadata.db "PRAGMA integrity_check;"

# Check chapter count
sqlite3 backups/latest-copy/metadata.db "SELECT COUNT(*) FROM chapter_metadata;"

# Verify Qdrant snapshot exists and has checksum
ls -lh backups/latest-copy/textbook_chunks_latest.snapshot
shasum -a 256 backups/latest-copy/textbook_chunks_latest.snapshot

# Verify Neo4j export exists
ls -lh backups/latest-copy/neo4j_latest.cypher
wc -l backups/latest-copy/neo4j_latest.cypher
```

**Verification Criteria**:
- `PRAGMA integrity_check` returns `ok`
- Chapter count matches expected (e.g., 220)
- All three backup files exist with reasonable sizes
- Checksums match recorded values (if available)

**If latest-copy is corrupted**:
- Escalate to disaster recovery procedure
- Restore from v1_baseline instead
- See `disaster_recovery.md` (to be created)

### Step 7: Restore from latest-copy (if needed)
**Objective**: Restore production databases from latest-copy if staging data contaminated production.

**When to Execute**:
- Only if production data was modified during failed ingestion
- Skip if staging was properly isolated

**Restoration Commands**:

#### SQLite Restoration
```bash
# Backup current state before restore
cp metadata.db backups/pre_rollback_metadata.db

# Restore from latest-copy
cp backups/latest-copy/metadata.db metadata.db

# Verify restoration
sqlite3 metadata.db "SELECT COUNT(*) FROM chapter_metadata;"
```

#### Qdrant Restoration
```bash
# Delete corrupted collection
curl -X DELETE "http://localhost:6333/collections/textbook_chunks"

# Upload latest-copy snapshot
curl -X POST "http://localhost:6333/collections/textbook_chunks/snapshots/upload" \
  -F "snapshot=@backups/latest-copy/textbook_chunks_latest.snapshot"

# Recover from snapshot
curl -X PUT "http://localhost:6333/collections/textbook_chunks/snapshots/recover" \
  -H "Content-Type: application/json" \
  -d '{"location": "textbook_chunks_latest.snapshot"}'

# Verify point count
curl -X GET "http://localhost:6333/collections/textbook_chunks" 2>/dev/null | \
  python3 -c "import sys, json; print(json.load(sys.stdin)['result']['points_count'])"
```

#### Neo4j Restoration
```bash
# Delete all nodes (CAUTION: destructive)
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (n) DETACH DELETE n"

# Restore from latest-copy export
cat backups/latest-copy/neo4j_latest.cypher | \
  docker exec -i hybridflow-neo4j cypher-shell -u neo4j -p password

# Verify paragraph count
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph) RETURN count(p)" --format plain
```

**Verification**:
- All counts match pre-migration baseline
- Run full validation checklist (see `validation_checklist.md`)
- Test search functionality

### Step 8: Update Status to 'rolled_back'
**Objective**: Finalize rollback by updating version registry and logging completion.

```bash
# Update version status
sqlite3 metadata.db "UPDATE version_registry
  SET status = 'rolled_back',
      updated_at = CURRENT_TIMESTAMP,
      status_message = 'Rollback completed successfully. Reason: [REASON]'
  WHERE version_id = 'v2_minor_20251226_143022';"

# Log rollback completion
sqlite3 metadata.db "INSERT INTO operation_log
  (version_id, operation_type, status, details, completed_at)
  VALUES (
    'v2_minor_20251226_143022',
    'rollback',
    'completed',
    'All staging data removed. Production data restored from latest-copy.',
    CURRENT_TIMESTAMP
  );"

# Verify final status
sqlite3 metadata.db "SELECT version_id, status, status_message, updated_at
  FROM version_registry
  WHERE version_id = 'v2_minor_20251226_143022';"
```

**Expected Output**:
```
v2_minor_20251226_143022|rolled_back|Rollback completed successfully. Reason: [REASON]|2025-12-26 14:45:30
```

## Post-Rollback Validation

After completing all 8 steps, run the full validation checklist:

```bash
# Run consistency check
poetry run python scripts/verify_chunk_consistency.py

# Verify counts
sqlite3 metadata.db "SELECT COUNT(*) FROM chapter_metadata;"
curl -X GET "http://localhost:6333/collections/textbook_chunks" | grep points_count
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph) RETURN count(p)"

# Test search functionality
poetry run hybridflow search "lung anatomy" --limit 5

# Check for errors in logs
docker logs hybridflow-qdrant --tail 50
docker logs hybridflow-neo4j --tail 50
```

**Success Criteria**:
- Chapter count = Expected (e.g., 220)
- Qdrant points = Neo4j paragraphs
- Search returns results
- No errors in Docker logs
- All validation checks pass

## Rollback Time Estimates

| Step | Description | Estimated Time |
|------|-------------|----------------|
| 1 | Identify failed version | 1 minute |
| 2 | Update status | 30 seconds |
| 3 | Drop SQLite staging table | 10 seconds |
| 4 | Delete Qdrant staging collection | 1-2 minutes |
| 5 | Delete Neo4j staging nodes | 2-5 minutes |
| 6 | Verify latest-copy | 1 minute |
| 7 | Restore from latest-copy | 5-10 minutes (if needed) |
| 8 | Update final status | 30 seconds |
| **Total** | **Without restoration** | **~10 minutes** |
| **Total** | **With full restoration** | **~20 minutes** |

## Troubleshooting

### Issue: Cannot connect to Qdrant
```bash
# Check container status
docker ps | grep qdrant

# Restart container if needed
docker restart hybridflow-qdrant

# Wait 30 seconds for startup
sleep 30

# Retry connection
curl http://localhost:6333
```

### Issue: Neo4j cypher-shell hangs
```bash
# Check container resources
docker stats hybridflow-neo4j --no-stream

# Increase memory if needed (in docker-compose.yml)
# Restart Neo4j
docker restart hybridflow-neo4j
```

### Issue: latest-copy corrupted
```bash
# Escalate to v1_baseline restoration
cp backups/v1_baseline_*/metadata.db metadata.db

# Restore Qdrant from v1_baseline
# Restore Neo4j from v1_baseline

# This is disaster recovery - document separately
```

## Safety Checklist

Before starting rollback:
- [ ] Identified correct version_id to rollback
- [ ] Confirmed version is not v1_baseline
- [ ] Verified latest-copy exists and is intact
- [ ] Checked disk space (need ~500 MB free)
- [ ] Docker containers are running
- [ ] No active ingestion operations
- [ ] Stakeholders notified (if applicable)

During rollback:
- [ ] Each step completed without errors
- [ ] Verification checks passed
- [ ] Progress logged in operation_log

After rollback:
- [ ] All staging data removed
- [ ] Production data verified intact
- [ ] Validation checks passed
- [ ] Version status updated to 'rolled_back'
- [ ] Root cause documented
- [ ] Lessons learned captured

## Related Documentation
- `versioning_spec.md` - Version states and transitions
- `validation_checklist.md` - Post-rollback validation
- `migration_plan.md` - Rollback trigger conditions
- `migration_monitoring.md` - Monitoring during operations
