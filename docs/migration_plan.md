# Migration Plan: Baseline Version Registration (v1_baseline)

## Overview
This plan details the migration to implement the versioning infrastructure and register the current production state as `v1_baseline`.

## Migration Objectives
1. Create versioning schema tables (`version_registry`, `operation_log`)
2. Register current production data as `v1_baseline`
3. Add version labels to Neo4j nodes
4. Establish rollback capability framework
5. Enable sliding window version management

## Expected Downtime

### Downtime Breakdown

| Operation | Duration | User Impact | Query Disruption |
|-----------|----------|-------------|------------------|
| Schema changes (SQLite) | < 1 minute | None | No disruption |
| Baseline registration | 2-3 minutes | None | No disruption |
| Neo4j label addition | 5-10 minutes | None | Read operations continue |
| Validation checks | 2-3 minutes | None | No disruption |
| **Total Estimated Downtime** | **10-15 minutes** | **Minimal** | **No query disruption** |

### Detailed Timeline

#### Phase 1: Schema Migration (< 1 minute)
```
00:00 - Start migration
00:01 - Create version_registry table
00:02 - Create operation_log table
00:03 - Add indexes
00:04 - Verify schema
00:05 - Phase 1 complete
```
**Impact**: None - Additive schema changes only

#### Phase 2: Baseline Registration (2-3 minutes)
```
00:05 - Insert v1_baseline entry into version_registry
00:06 - Create metadata snapshot
00:07 - Create Qdrant snapshot (via API)
00:08 - Trigger Neo4j export
00:09 - Calculate checksums
00:10 - Phase 2 complete
```
**Impact**: None - Read-only operations, no data modification

#### Phase 3: Neo4j Label Addition (5-10 minutes)
```
00:10 - Begin label addition to 36,216 paragraphs
00:11 - Batch 1-10: 10,000 nodes labeled
00:15 - Batch 11-20: 20,000 nodes labeled
00:18 - Batch 21-36: 36,216 nodes labeled
00:20 - Verify label count
00:21 - Phase 3 complete
```
**Impact**: Minimal - Read operations continue normally
**Note**: Write operations to Neo4j should be paused during label addition

#### Phase 4: Validation (2-3 minutes)
```
00:21 - Run validation checklist
00:22 - Verify counts across all databases
00:23 - Test search functionality
00:24 - Confirm rollback capability
00:25 - Phase 4 complete
```
**Impact**: None - Validation runs against production data

### Query Disruption Analysis

#### SQLite (metadata.db)
- **Schema changes**: No disruption (new tables don't affect existing queries)
- **Baseline registration**: No disruption (INSERT operation, no locks on existing data)
- **Expected behavior**: All queries continue normally

#### Qdrant (Vector Database)
- **Snapshot creation**: No disruption (background operation)
- **Read queries**: Continue unaffected
- **Write operations**: Should be paused during migration (recommended, not required)

#### Neo4j (Graph Database)
- **Label addition**: Minimal impact
  - Read queries continue normally
  - Write operations may experience brief locks (< 100ms per batch)
- **Batch strategy**: Process 1,000 nodes at a time to minimize lock duration
- **Recommended**: Pause write operations during label addition

### Downtime Mitigation Strategies

1. **Progressive Label Addition**:
   ```cypher
   // Process in batches of 1,000
   CALL apoc.periodic.iterate(
     "MATCH (p:Paragraph) RETURN p",
     "SET p:v1_baseline",
     {batchSize: 1000, parallel: false}
   )
   ```

2. **Non-Blocking Snapshot Creation**:
   - Qdrant snapshots created via API (async)
   - Neo4j exports run in background
   - SQLite snapshots use file copy (instant)

3. **Rollback Readiness**:
   - Latest-copy created before any modifications
   - All changes reversible within 5 minutes
   - See `rollback_procedure.md` for details

## Success Criteria

All criteria must be met for migration to be considered successful.

### Criterion 1: version_registry Table Created with v1_baseline Entry

**Verification**:
```sql
SELECT * FROM version_registry WHERE version_id LIKE 'v1_baseline%';
```

**Expected Result**:
```
id|version_id|status|created_at|metadata_snapshot_path|chapter_count|paragraph_count|validation_passed
1|v1_baseline_20251225_200000|committed|2025-12-25 20:00:00|backups/v1_baseline_20251225_200000/metadata.db|220|36216|1
```

**Pass Criteria**:
- Table exists
- v1_baseline entry present
- Status = 'committed'
- All snapshot paths populated
- Counts match expected values

### Criterion 2: operation_log Table Created and Empty

**Verification**:
```sql
.schema operation_log
SELECT COUNT(*) FROM operation_log;
```

**Expected Result**:
```
CREATE TABLE operation_log (...)
0
```

**Pass Criteria**:
- Table exists with correct schema
- Initially empty (no operations logged yet)
- Ready to accept log entries

### Criterion 3: All 220 Chapters Accessible

**Verification**:
```bash
# SQLite
sqlite3 metadata.db "SELECT COUNT(*) FROM chapter_metadata;"

# Verify by textbook
sqlite3 metadata.db "SELECT textbook_id, COUNT(*) FROM chapter_metadata GROUP BY textbook_id;"
```

**Expected Result**:
```
220

bailey|92
sabiston|74
schwartz|54
```

**Pass Criteria**:
- Total chapters = 220
- Distribution matches pre-migration state
- No data loss during schema changes

### Criterion 4: All 36,216 Vectors Queryable

**Verification**:
```bash
# Get collection info
curl -X GET "http://localhost:6333/collections/textbook_chunks" 2>/dev/null | python3 -m json.tool

# Test query
curl -X POST "http://localhost:6333/collections/textbook_chunks/points/scroll" \
  -H "Content-Type: application/json" \
  -d '{"limit": 10, "with_payload": true, "with_vector": false}' 2>/dev/null | python3 -m json.tool
```

**Expected Result**:
```json
{
  "result": {
    "points_count": 36216,
    "status": "green"
  }
}
```

**Pass Criteria**:
- points_count = 36216
- Collection status = "green"
- Query returns results successfully
- No errors in response

### Criterion 5: All 36,216 Paragraphs Have :v1_baseline Label

**Verification**:
```cypher
// Count paragraphs with v1_baseline label
MATCH (p:Paragraph:v1_baseline)
RETURN count(p) as labeled_count;

// Count paragraphs without label
MATCH (p:Paragraph)
WHERE NOT p:v1_baseline
RETURN count(p) as unlabeled_count;
```

**Command**:
```bash
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph:v1_baseline) RETURN count(p)" --format plain
```

**Expected Result**:
```
36216
```

**Pass Criteria**:
- labeled_count = 36216
- unlabeled_count = 0
- All paragraphs have :v1_baseline label

### Criterion 6: No Data Loss

**Verification**:
```bash
# Compare pre-migration and post-migration counts
# Pre-migration counts from pre_migration_report_20251225.md:
# - Chapters: 220
# - Qdrant points: 36,216
# - Neo4j paragraphs: 36,216

# Post-migration verification
echo "SQLite chapters: $(sqlite3 metadata.db 'SELECT COUNT(*) FROM chapter_metadata')"
echo "Qdrant points: $(curl -X GET 'http://localhost:6333/collections/textbook_chunks' 2>/dev/null | python3 -c 'import sys, json; print(json.load(sys.stdin)["result"]["points_count"])')"
echo "Neo4j paragraphs: $(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password 'MATCH (p:Paragraph) RETURN count(p)' --format plain | tail -1)"
```

**Expected Result**:
```
SQLite chapters: 220
Qdrant points: 36216
Neo4j paragraphs: 36216
```

**Pass Criteria**:
- All counts match pre-migration baseline
- No records deleted
- No records corrupted

### Criterion 7: Queries Return Identical Results Pre and Post Migration

**Verification**:
```bash
# Test search query (run before and after migration)
poetry run hybridflow search "lung anatomy" --limit 5 > pre_migration_results.txt

# After migration
poetry run hybridflow search "lung anatomy" --limit 5 > post_migration_results.txt

# Compare results
diff pre_migration_results.txt post_migration_results.txt
```

**Expected Result**:
```
# No diff output (files identical)
```

**Additional Tests**:
```bash
# Test specific chunk retrieval
poetry run hybridflow get-hierarchy bailey:ch60 --json > pre_hierarchy.json
# After migration
poetry run hybridflow get-hierarchy bailey:ch60 --json > post_hierarchy.json
diff pre_hierarchy.json post_hierarchy.json
```

**Pass Criteria**:
- Search results identical
- Hierarchy retrieval identical
- No functional regressions
- Response times within 10% of baseline

### Criterion 8: Versioning Infrastructure Operational

**Verification**:
```bash
# Test version registry queries
sqlite3 metadata.db "SELECT version_id, status FROM version_registry ORDER BY created_at DESC LIMIT 1;"

# Test operation logging
sqlite3 metadata.db "INSERT INTO operation_log (version_id, operation_type, status, details)
  VALUES ('v1_baseline_20251225_200000', 'test', 'success', 'Testing operation log');"

sqlite3 metadata.db "SELECT * FROM operation_log WHERE operation_type='test';"

# Cleanup test entry
sqlite3 metadata.db "DELETE FROM operation_log WHERE operation_type='test';"
```

**Expected Result**:
- version_registry queries work
- operation_log accepts entries
- No errors during INSERT/SELECT

**Pass Criteria**:
- All versioning tables functional
- Queries execute without errors
- Ready for future version tracking

## Rollback Triggers

Any of the following conditions will trigger an immediate rollback.

### Trigger 1: Any Table Creation Failure

**Condition**:
```bash
sqlite3 metadata.db ".tables version_registry" 2>&1 | grep -q "Error"
```

**Action**:
- Rollback Alembic migration
- Restore metadata.db from backup
- Abort migration

**Impact**: Critical - Cannot proceed without versioning tables

### Trigger 2: Baseline Registration Failure

**Condition**:
```sql
SELECT COUNT(*) FROM version_registry WHERE version_id LIKE 'v1_baseline%';
-- Returns 0
```

**Action**:
- Delete partial version_registry entries
- Rollback schema changes
- Investigate INSERT failure cause

**Impact**: Critical - Baseline version is foundation of all versioning

### Trigger 3: Validation Check Failure

**Condition**:
- Any critical validation check fails (Checks 1-5 in `validation_checklist.md`)
- Count mismatches detected
- Data inconsistencies found

**Action**:
- Execute full rollback procedure (see `rollback_procedure.md`)
- Restore from latest-copy
- Log failure details in operation_log

**Impact**: Critical - Data integrity compromised

### Trigger 4: Neo4j Label Addition Failure

**Condition**:
```cypher
MATCH (p:Paragraph:v1_baseline)
RETURN count(p) as labeled
-- If labeled < 36216
```

**Action**:
- Remove partial labels:
  ```cypher
  MATCH (p:Paragraph:v1_baseline)
  REMOVE p:v1_baseline
  ```
- Investigate label addition error
- Retry with smaller batch sizes
- If persistent failure, abort migration

**Impact**: High - Versioning requires labels for version isolation

### Trigger 5: Post-Migration Query Failure

**Condition**:
```bash
poetry run hybridflow search "test query" --limit 5
# Returns error or zero results
```

**Action**:
- Immediately pause migration
- Run diagnostic queries on all databases
- Check Docker container health
- If query functionality broken, execute full rollback

**Impact**: Critical - Application unusable

### Trigger 6: Inconsistent Counts Across Systems

**Condition**:
```bash
# If any of these don't match
SQLITE_COUNT=$(sqlite3 metadata.db "SELECT COUNT(*) FROM chapter_metadata;")
QDRANT_COUNT=$(curl -X GET "http://localhost:6333/collections/textbook_chunks" 2>/dev/null | python3 -c "import sys, json; print(json.load(sys.stdin)['result']['points_count'])")
NEO4J_COUNT=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password "MATCH (p:Paragraph) RETURN count(p)" --format plain | tail -1)

# Expected: All counts stable from pre-migration
```

**Action**:
- Identify which database has incorrect count
- Check for partial writes or deletions
- Execute full rollback
- Run data recovery from backups

**Impact**: Critical - Data consistency violated

### Trigger Response Protocol

When a trigger is activated:

1. **Immediate Actions** (< 1 minute):
   ```bash
   # Stop any running migration processes
   pkill -f "run_migration.sh"

   # Update version_registry
   sqlite3 metadata.db "UPDATE version_registry
     SET status = 'rollback_triggered',
         status_message = 'Trigger 3: Validation check failure - chunk count mismatch'
     WHERE version_id LIKE 'v1_baseline%';"
   ```

2. **Assessment** (1-2 minutes):
   - Determine severity (critical vs high)
   - Identify affected databases
   - Check if partial changes committed

3. **Rollback Execution** (5-15 minutes):
   - Follow `rollback_procedure.md`
   - Verify restoration success
   - Run post-rollback validation

4. **Post-Mortem** (after rollback):
   - Document trigger cause
   - Update operation_log with full details
   - Revise migration plan if needed

## Migration Execution Checklist

### Pre-Migration
- [ ] All backups verified (from TASK M0.1)
- [ ] Pre-migration report reviewed
- [ ] Docker containers running and healthy
- [ ] Disk space sufficient (>500 MB free)
- [ ] No active ingestion operations
- [ ] Test queries executed and results saved

### During Migration
- [ ] Schema changes applied successfully
- [ ] version_registry table created
- [ ] operation_log table created
- [ ] v1_baseline entry inserted
- [ ] Snapshots created (metadata, Qdrant, Neo4j)
- [ ] Neo4j labels added to all paragraphs
- [ ] No errors in Docker logs

### Post-Migration
- [ ] All 8 success criteria verified
- [ ] Validation checklist passed
- [ ] Test queries return identical results
- [ ] Rollback capability confirmed
- [ ] Documentation updated (CLAUDE.md)
- [ ] Migration tagged in git

## Related Documentation
- `versioning_spec.md` - Version identifier format and state transitions
- `rollback_procedure.md` - Detailed rollback steps
- `validation_checklist.md` - 7 validation checks
- `migration_monitoring.md` - Real-time monitoring metrics
- `run_migration.sh` - Migration execution script

## Next Steps After Successful Migration

1. Tag migration in git:
   ```bash
   git tag -a v1.0.0-baseline -m "Baseline version with versioning infrastructure"
   ```

2. Update CLAUDE.md with new versioning features

3. Test version creation workflow with a small ingestion

4. Document lessons learned

5. Plan next version (v2) implementation
