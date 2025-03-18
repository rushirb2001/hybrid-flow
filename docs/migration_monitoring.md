# Migration Monitoring Checklist

## Overview
This document defines the metrics and monitoring procedures to track during migration execution. Real-time monitoring enables early detection of issues and informed rollback decisions.

## Monitoring Context
- **When**: During all migration phases
- **Frequency**: Real-time for critical metrics, periodic for performance metrics
- **Purpose**: Detect failures early, validate progress, trigger rollback if needed
- **Tools**: Command-line queries, Docker logs, SQLite inspection

## Monitoring Metrics

### Metric 1: SQLite Table Creation Success

**Objective**: Verify that versioning tables are created without errors.

**Monitoring Commands**:
```bash
# Check if version_registry table exists
sqlite3 metadata.db ".tables version_registry"

# Verify table schema
sqlite3 metadata.db ".schema version_registry"

# Check if operation_log table exists
sqlite3 metadata.db ".tables operation_log"

# Verify operation_log schema
sqlite3 metadata.db ".schema operation_log"

# Count columns in each table
sqlite3 metadata.db "PRAGMA table_info(version_registry);" | wc -l
sqlite3 metadata.db "PRAGMA table_info(operation_log);" | wc -l
```

**Expected Results**:
```
version_registry
CREATE TABLE version_registry (...)

operation_log
CREATE TABLE operation_log (...)

13  # version_registry column count
7   # operation_log column count
```

**Success Criteria**:
- Both tables exist
- Schemas match specification
- No errors during creation
- Correct column counts

**Failure Indicators**:
- `Error: no such table`
- `Error: table ... already exists` (if not using IF NOT EXISTS)
- Schema mismatch
- Missing columns

**Rollback Trigger**: YES - Critical failure (Trigger 1)

**Logging**:
```bash
# Log table creation status
echo "[$(date)] version_registry table: $(sqlite3 metadata.db '.tables version_registry' 2>&1)" >> migration.log
echo "[$(date)] operation_log table: $(sqlite3 metadata.db '.tables operation_log' 2>&1)" >> migration.log
```

### Metric 2: version_registry Insert Success

**Objective**: Confirm baseline version entry is successfully inserted.

**Monitoring Commands**:
```bash
# Check for v1_baseline entry
sqlite3 metadata.db "SELECT version_id, status, created_at FROM version_registry WHERE version_id LIKE 'v1_baseline%';"

# Verify all fields populated
sqlite3 metadata.db "SELECT * FROM version_registry WHERE version_id LIKE 'v1_baseline%';"

# Count entries (should be 1)
sqlite3 metadata.db "SELECT COUNT(*) FROM version_registry WHERE version_id LIKE 'v1_baseline%';"
```

**Expected Results**:
```
v1_baseline_20251225_200000|pending|2025-12-25 20:00:00

v1_baseline_20251225_200000|pending|2025-12-25 20:00:00|...|220|36216|0|...

1
```

**Success Criteria**:
- Exactly 1 entry exists
- version_id format correct
- Status = 'pending' initially
- chapter_count = 220
- paragraph_count = 36216
- Snapshot paths populated

**Failure Indicators**:
- No entry found (INSERT failed)
- Multiple entries (duplicate execution)
- NULL values in required fields
- Incorrect counts

**Rollback Trigger**: YES - Critical failure (Trigger 2)

**Real-Time Monitoring**:
```bash
# Watch version_registry during migration
watch -n 5 "sqlite3 metadata.db 'SELECT version_id, status, updated_at FROM version_registry ORDER BY created_at DESC LIMIT 3;'"
```

**Logging**:
```bash
# Log registration status
sqlite3 metadata.db "SELECT version_id, status, chapter_count, paragraph_count FROM version_registry WHERE version_id LIKE 'v1_baseline%';" >> migration.log
```

### Metric 3: Neo4j Label Addition Progress

**Objective**: Track batched label addition to 36,216 paragraphs.

**Monitoring Commands**:
```bash
# Count labeled paragraphs (run every 30 seconds)
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph:v1_baseline) RETURN count(p) as labeled" --format plain

# Calculate progress percentage
LABELED=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph:v1_baseline) RETURN count(p)" --format plain | tail -1)
TOTAL=36216
PERCENT=$(echo "scale=2; $LABELED * 100 / $TOTAL" | bc)
echo "Progress: $LABELED / $TOTAL ($PERCENT%)"

# Check for orphaned paragraphs (without label)
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph) WHERE NOT p:v1_baseline RETURN count(p)" --format plain
```

**Expected Progress**:
```
# After 1 minute
Batch 1-5 complete: ~5,000 labeled

# After 3 minutes
Batch 6-15 complete: ~15,000 labeled

# After 6 minutes
Batch 16-30 complete: ~30,000 labeled

# After 8-10 minutes
All batches complete: 36,216 labeled
```

**Success Criteria**:
- Labeled count increases steadily (no stalling)
- Final count = 36,216
- No unlabeled paragraphs remain
- No errors in Neo4j logs

**Failure Indicators**:
- Label count stops increasing (batch failure)
- Final count < 36,216 (incomplete labeling)
- Error messages in Neo4j logs
- Timeouts or connection errors

**Rollback Trigger**: YES if final count ≠ 36,216 (Trigger 4)

**Batched Progress Monitoring**:
```bash
# Monitor progress in real-time
while true; do
  LABELED=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
    "MATCH (p:Paragraph:v1_baseline) RETURN count(p)" --format plain 2>/dev/null | tail -1)
  echo "[$(date)] Labeled: $LABELED / 36216"

  if [ "$LABELED" -eq 36216 ]; then
    echo "Label addition complete!"
    break
  fi

  sleep 30
done
```

**Logging**:
```bash
# Log progress every minute
echo "[$(date)] Neo4j labeling progress: $(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password 'MATCH (p:Paragraph:v1_baseline) RETURN count(p)' --format plain | tail -1) / 36216" >> migration.log
```

### Metric 4: Validation Results for Each Check

**Objective**: Track success/failure of all 7 validation checks.

**Monitoring Commands**:
```bash
# Validation Check 1: Chapter count
CHAPTER_COUNT=$(sqlite3 metadata.db "SELECT COUNT(*) FROM chapter_metadata;")
echo "Check 1 - Chapter count: $CHAPTER_COUNT (expected: 220)"

# Validation Check 2: Qdrant-Neo4j equality
QDRANT_COUNT=$(curl -X GET "http://localhost:6333/collections/textbook_chunks" 2>/dev/null | \
  python3 -c "import sys, json; print(json.load(sys.stdin)['result']['points_count'])")
NEO4J_COUNT=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph) RETURN count(p)" --format plain | tail -1)
echo "Check 2 - Qdrant: $QDRANT_COUNT, Neo4j: $NEO4J_COUNT (must be equal)"

# Validation Check 3: SQLite-Qdrant consistency
poetry run python scripts/validate_sqlite_qdrant.py > validation_check3.log 2>&1
echo "Check 3 - SQLite-Qdrant: $(tail -1 validation_check3.log)"

# Validation Check 4: Qdrant-Neo4j chunk_id consistency
poetry run python scripts/validate_qdrant_neo4j.py > validation_check4.log 2>&1
echo "Check 4 - Qdrant-Neo4j chunk_ids: $(tail -1 validation_check4.log)"

# Validation Check 5: Parent relationships
ORPHANED=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph) WHERE NOT EXISTS {MATCH (p)<-[:HAS_PARAGRAPH|HAS_SUBSECTION|HAS_SECTION]-()} RETURN count(p)" \
  --format plain | tail -1)
echo "Check 5 - Orphaned paragraphs: $ORPHANED (expected: 0)"

# Validation Check 6: NEXT/PREV chains
BROKEN_CHAINS=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "MATCH (p:Paragraph)-[:NEXT]->(n) WHERE NOT (n)-[:PREV]->(p) RETURN count(p)" \
  --format plain | tail -1)
echo "Check 6 - Broken chains: $BROKEN_CHAINS (expected: 0)"

# Validation Check 7: Cross-references
poetry run python scripts/validate_cross_references.py > validation_check7.log 2>&1
echo "Check 7 - Cross-references: $(tail -1 validation_check7.log)"
```

**Expected Results**:
```
Check 1 - Chapter count: 220 (expected: 220) ✓
Check 2 - Qdrant: 36216, Neo4j: 36216 (must be equal) ✓
Check 3 - SQLite-Qdrant: ✓ PASS ✓
Check 4 - Qdrant-Neo4j chunk_ids: ✓ PASS ✓
Check 5 - Orphaned paragraphs: 0 (expected: 0) ✓
Check 6 - Broken chains: 0 (expected: 0) ✓
Check 7 - Cross-references: ✓ PASS ✓
```

**Success Criteria**:
- All 7 checks return expected values
- No "FAIL" messages in logs
- All counts match pre-migration baseline

**Failure Indicators**:
- Any check returns "FAIL"
- Count mismatches
- Non-zero orphaned paragraphs
- Broken NEXT/PREV chains

**Rollback Trigger**: YES for checks 1-5 (Trigger 3), WARNING for checks 6-7

**Validation Summary Script**:
```bash
#!/bin/bash
# scripts/monitor_validation.sh

PASS_COUNT=0
FAIL_COUNT=0

# Run all 7 checks and count results
for i in {1..7}; do
  if run_validation_check_$i; then
    PASS_COUNT=$((PASS_COUNT + 1))
    echo "✓ Check $i PASSED"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo "✗ Check $i FAILED"
  fi
done

echo "=========================================="
echo "Validation Summary: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "=========================================="

if [ $FAIL_COUNT -eq 0 ]; then
  exit 0
else
  exit 1
fi
```

**Logging**:
```bash
# Log all validation results
echo "[$(date)] Validation Results:" >> migration.log
echo "  Chapter count: $CHAPTER_COUNT" >> migration.log
echo "  Qdrant count: $QDRANT_COUNT" >> migration.log
echo "  Neo4j count: $NEO4J_COUNT" >> migration.log
cat validation_check3.log >> migration.log
cat validation_check4.log >> migration.log
```

### Metric 5: Query Response Time Before/After

**Objective**: Ensure migration does not degrade search performance.

**Monitoring Commands**:
```bash
# Measure response time before migration
TIME_BEFORE=$(time (poetry run hybridflow search "lung anatomy" --limit 5 > /dev/null 2>&1) 2>&1 | grep real | awk '{print $2}')
echo "Query time before migration: $TIME_BEFORE"

# After migration, measure again
TIME_AFTER=$(time (poetry run hybridflow search "lung anatomy" --limit 5 > /dev/null 2>&1) 2>&1 | grep real | awk '{print $2}')
echo "Query time after migration: $TIME_AFTER"

# Calculate performance change
# (This is simplified - actual implementation would parse time format)
echo "Performance comparison: Before=$TIME_BEFORE, After=$TIME_AFTER"
```

**Expected Results**:
```
Query time before migration: 0m0.842s
Query time after migration: 0m0.856s
Performance degradation: 1.7% (acceptable)
```

**Success Criteria**:
- Response time increases < 10%
- No timeouts
- Results identical (not just similar)

**Failure Indicators**:
- Response time increases > 20%
- Query timeouts
- Different result counts

**Rollback Trigger**: YES if > 20% degradation or query failures (Trigger 5)

**Benchmark Script**:
```bash
# scripts/benchmark_queries.sh
#!/bin/bash

QUERIES=(
  "lung anatomy"
  "surgery"
  "cardiac"
  "trauma"
  "infection"
)

echo "Running query benchmarks..."
for QUERY in "${QUERIES[@]}"; do
  START=$(date +%s%N)
  poetry run hybridflow search "$QUERY" --limit 5 > /dev/null 2>&1
  END=$(date +%s%N)
  ELAPSED=$(( ($END - $START) / 1000000 ))  # Convert to ms
  echo "Query '$QUERY': ${ELAPSED}ms"
done
```

**Usage**:
```bash
# Before migration
./scripts/benchmark_queries.sh > benchmark_before.txt

# After migration
./scripts/benchmark_queries.sh > benchmark_after.txt

# Compare
diff benchmark_before.txt benchmark_after.txt
```

**Logging**:
```bash
# Log query performance
echo "[$(date)] Query benchmarks:" >> migration.log
cat benchmark_after.txt >> migration.log
```

### Metric 6: Error Logs from All Three Systems

**Objective**: Monitor for errors in SQLite, Qdrant, and Neo4j during migration.

**Monitoring Commands**:

#### SQLite Error Monitoring
```bash
# Test SQLite integrity
sqlite3 metadata.db "PRAGMA integrity_check;"

# Check for constraint violations
sqlite3 metadata.db "SELECT * FROM version_registry WHERE version_id IS NULL;"

# Verify no orphaned foreign keys
sqlite3 metadata.db "PRAGMA foreign_key_check;"
```

**Expected Results**:
```
ok

# (empty result - no NULL version_ids)

# (empty result - no FK violations)
```

#### Qdrant Error Monitoring
```bash
# Check Qdrant container logs (last 50 lines)
docker logs hybridflow-qdrant --tail 50 2>&1 | grep -i error

# Check collection health
curl -X GET "http://localhost:6333/collections/textbook_chunks" 2>/dev/null | \
  python3 -c "import sys, json; print('Status:', json.load(sys.stdin)['result']['status'])"

# Check for disk space issues
docker exec hybridflow-qdrant df -h /qdrant/storage
```

**Expected Results**:
```
# (no error messages in logs)

Status: green

Filesystem      Size  Used Avail Use% Mounted on
overlay         233G  150M  233G   1% /qdrant/storage
```

#### Neo4j Error Monitoring
```bash
# Check Neo4j container logs (last 50 lines)
docker logs hybridflow-neo4j --tail 50 2>&1 | grep -i error

# Check database status
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "CALL dbms.showCurrentUser()" --format plain

# Check for constraint violations
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
  "SHOW CONSTRAINTS" --format plain

# Monitor memory usage
docker stats hybridflow-neo4j --no-stream
```

**Expected Results**:
```
# (no critical errors in logs - warnings are acceptable)

neo4j

# (list of constraints - should not include errors)

CONTAINER        CPU %     MEM USAGE / LIMIT     MEM %
hybridflow-neo4j 5.21%     512MiB / 2GiB        25.6%
```

**Success Criteria**:
- No ERROR level logs (WARN is acceptable)
- SQLite integrity check = "ok"
- Qdrant collection status = "green"
- Neo4j accessible and responsive
- No memory/disk space issues

**Failure Indicators**:
- ERROR messages in any system
- SQLite corruption detected
- Qdrant status = "red" or "yellow"
- Neo4j connection failures
- Out of memory errors

**Rollback Trigger**: YES if critical errors detected

**Continuous Monitoring Script**:
```bash
#!/bin/bash
# scripts/monitor_errors.sh

echo "Monitoring errors across all systems..."
echo "Press Ctrl+C to stop"

while true; do
  # Check SQLite
  SQLITE_ERRORS=$(sqlite3 metadata.db "PRAGMA integrity_check;" | grep -v "ok" | wc -l)

  # Check Qdrant
  QDRANT_ERRORS=$(docker logs hybridflow-qdrant --tail 10 2>&1 | grep -i error | wc -l)

  # Check Neo4j
  NEO4J_ERRORS=$(docker logs hybridflow-neo4j --tail 10 2>&1 | grep -i error | wc -l)

  echo "[$(date)] Errors - SQLite: $SQLITE_ERRORS, Qdrant: $QDRANT_ERRORS, Neo4j: $NEO4J_ERRORS"

  if [ $SQLITE_ERRORS -gt 0 ] || [ $QDRANT_ERRORS -gt 0 ] || [ $NEO4J_ERRORS -gt 0 ]; then
    echo "WARNING: Errors detected!"
  fi

  sleep 10
done
```

**Logging**:
```bash
# Capture all error logs at end of migration
echo "[$(date)] SQLite integrity check:" >> migration.log
sqlite3 metadata.db "PRAGMA integrity_check;" >> migration.log

echo "[$(date)] Qdrant logs (last 50):" >> migration.log
docker logs hybridflow-qdrant --tail 50 >> migration.log 2>&1

echo "[$(date)] Neo4j logs (last 50):" >> migration.log
docker logs hybridflow-neo4j --tail 50 >> migration.log 2>&1
```

## Monitoring Dashboard

### Real-Time Dashboard Script
```bash
#!/bin/bash
# scripts/migration_dashboard.sh

while true; do
  clear
  echo "=========================================="
  echo "Migration Monitoring Dashboard"
  echo "Time: $(date)"
  echo "=========================================="

  # Metric 1: Tables
  echo ""
  echo "[1] SQLite Tables:"
  sqlite3 metadata.db ".tables" | grep -E "version_registry|operation_log" || echo "  Not created yet"

  # Metric 2: Version registry
  echo ""
  echo "[2] Baseline Version:"
  sqlite3 metadata.db "SELECT version_id, status FROM version_registry WHERE version_id LIKE 'v1_baseline%';" || echo "  Not registered yet"

  # Metric 3: Neo4j labels
  echo ""
  echo "[3] Neo4j Label Progress:"
  LABELED=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
    "MATCH (p:Paragraph:v1_baseline) RETURN count(p)" --format plain 2>/dev/null | tail -1 || echo "0")
  PERCENT=$(echo "scale=1; $LABELED * 100 / 36216" | bc)
  echo "  $LABELED / 36216 ($PERCENT%)"

  # Metric 4: Validation status
  echo ""
  echo "[4] Data Counts:"
  echo "  SQLite chapters: $(sqlite3 metadata.db 'SELECT COUNT(*) FROM chapter_metadata;' 2>/dev/null || echo 'N/A')"
  echo "  Qdrant points: $(curl -s http://localhost:6333/collections/textbook_chunks 2>/dev/null | python3 -c 'import sys, json; print(json.load(sys.stdin)["result"]["points_count"])' 2>/dev/null || echo 'N/A')"
  echo "  Neo4j paragraphs: $(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password 'MATCH (p:Paragraph) RETURN count(p)' --format plain 2>/dev/null | tail -1 || echo 'N/A')"

  # Metric 6: Error counts
  echo ""
  echo "[6] Error Monitoring:"
  echo "  Qdrant errors (last 10 logs): $(docker logs hybridflow-qdrant --tail 10 2>&1 | grep -ic error)"
  echo "  Neo4j errors (last 10 logs): $(docker logs hybridflow-neo4j --tail 10 2>&1 | grep -ic error)"

  echo ""
  echo "=========================================="
  echo "Press Ctrl+C to exit monitoring"
  echo "Refreshing in 5 seconds..."

  sleep 5
done
```

**Usage**:
```bash
# Run in separate terminal during migration
./scripts/migration_dashboard.sh
```

## Post-Migration Monitoring

After migration completes, verify metrics remain stable:

```bash
# Wait 5 minutes, then re-check
sleep 300

# Verify counts haven't changed
sqlite3 metadata.db "SELECT COUNT(*) FROM chapter_metadata;"
curl -X GET "http://localhost:6333/collections/textbook_chunks" | grep points_count
docker exec hybridflow-neo4j cypher-shell -u neo4j -p password "MATCH (p:Paragraph) RETURN count(p)"

# Check for new errors
docker logs hybridflow-qdrant --since "5m" 2>&1 | grep -i error
docker logs hybridflow-neo4j --since "5m" 2>&1 | grep -i error

# Test queries still work
poetry run hybridflow search "test" --limit 3
```

## Related Documentation
- `migration_plan.md` - Complete migration plan with downtime estimates
- `validation_checklist.md` - Detailed validation procedures
- `rollback_procedure.md` - Rollback steps if monitoring detects failures
- `versioning_spec.md` - Version state transitions
