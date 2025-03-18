#!/bin/bash

################################################################################
# Migration Execution Script: v1_baseline Registration
#
# Purpose: Execute baseline version registration migration
# Status: TEMPLATE - Review and customize before execution
# Related Docs: docs/migration_plan.md, docs/rollback_procedure.md
################################################################################

set -e  # Exit on any error
set -u  # Exit on undefined variable

################################################################################
# Configuration
################################################################################

MIGRATION_DATE=$(date +%Y%m%d)
MIGRATION_TIME=$(date +%H%M%S)
VERSION_ID="v1_baseline_${MIGRATION_DATE}_${MIGRATION_TIME}"
BACKUP_DIR="/Users/rushirbhavsar/Main/code/git-commits/hybrid-flow/backups"
PROJECT_DIR="/Users/rushirbhavsar/Main/code/git-commits/hybrid-flow"

EXPECTED_CHAPTERS=220
EXPECTED_PARAGRAPHS=36216

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

################################################################################
# Logging Functions
################################################################################

log_info() {
  echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
  echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_section() {
  echo ""
  echo "=========================================="
  echo "$1"
  echo "=========================================="
}

################################################################################
# Section 1: Backup Verification
################################################################################

verify_backups() {
  log_section "Section 1: Backup Verification"

  log_info "Checking for backup files from TASK M0.1..."

  # Check metadata.db backup
  if [ -f "$BACKUP_DIR/metadata_backup_20251225.db" ]; then
    log_info "✓ metadata.db backup found"
  else
    log_error "✗ metadata.db backup NOT found"
    log_error "Please run TASK M0.1 backups before migration"
    exit 1
  fi

  # Check Qdrant snapshot
  if [ -f "$BACKUP_DIR/textbook_chunks_backup_20251225.snapshot" ]; then
    log_info "✓ Qdrant snapshot found"
  else
    log_error "✗ Qdrant snapshot NOT found"
    exit 1
  fi

  # Check Neo4j export
  if [ -f "$BACKUP_DIR/neo4j_backup_20251225.cypher" ]; then
    log_info "✓ Neo4j export found"
  else
    log_error "✗ Neo4j export NOT found"
    exit 1
  fi

  # Verify checksums
  if [ -f "$BACKUP_DIR/checksums_20251225.txt" ]; then
    log_info "Verifying backup checksums..."
    cd "$BACKUP_DIR"
    if shasum -a 256 -c checksums_20251225.txt > /dev/null 2>&1; then
      log_info "✓ All backup checksums verified"
    else
      log_error "✗ Checksum verification failed"
      exit 1
    fi
    cd "$PROJECT_DIR"
  else
    log_warn "Checksums file not found, skipping verification"
  fi

  # Check Docker services
  log_info "Verifying Docker services..."
  if docker ps | grep -q hybridflow-qdrant; then
    log_info "✓ Qdrant container running"
  else
    log_error "✗ Qdrant container not running"
    exit 1
  fi

  if docker ps | grep -q hybridflow-neo4j; then
    log_info "✓ Neo4j container running"
  else
    log_error "✗ Neo4j container not running"
    exit 1
  fi

  # Check disk space (need ~500 MB free)
  AVAILABLE_SPACE=$(df -m "$PROJECT_DIR" | tail -1 | awk '{print $4}')
  if [ "$AVAILABLE_SPACE" -gt 500 ]; then
    log_info "✓ Sufficient disk space: ${AVAILABLE_SPACE} MB available"
  else
    log_error "✗ Insufficient disk space: Only ${AVAILABLE_SPACE} MB available (need 500+ MB)"
    exit 1
  fi

  log_info "All backup verifications passed"
}

################################################################################
# Section 2: Schema Migration
################################################################################

run_schema_migration() {
  log_section "Section 2: Schema Migration"

  log_info "Creating version_registry table..."

  sqlite3 "$PROJECT_DIR/metadata.db" <<EOF
CREATE TABLE IF NOT EXISTS version_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status_message TEXT,
    metadata_snapshot_path TEXT,
    qdrant_snapshot_path TEXT,
    neo4j_snapshot_path TEXT,
    chapter_count INTEGER,
    paragraph_count INTEGER,
    validation_passed BOOLEAN,
    committed_by TEXT,
    notes TEXT
);
EOF

  if [ $? -eq 0 ]; then
    log_info "✓ version_registry table created"
  else
    log_error "✗ Failed to create version_registry table"
    exit 1
  fi

  log_info "Creating operation_log table..."

  sqlite3 "$PROJECT_DIR/metadata.db" <<EOF
CREATE TABLE IF NOT EXISTS operation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    status TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    FOREIGN KEY (version_id) REFERENCES version_registry(version_id)
);
EOF

  if [ $? -eq 0 ]; then
    log_info "✓ operation_log table created"
  else
    log_error "✗ Failed to create operation_log table"
    exit 1
  fi

  # Verify tables exist
  TABLES=$(sqlite3 "$PROJECT_DIR/metadata.db" ".tables" | grep -E "version_registry|operation_log" | wc -l)
  if [ "$TABLES" -eq 2 ]; then
    log_info "✓ All versioning tables verified"
  else
    log_error "✗ Table verification failed"
    exit 1
  fi

  log_info "Schema migration completed successfully"
}

################################################################################
# Section 3: Baseline Registration
################################################################################

register_baseline() {
  log_section "Section 3: Baseline Registration"

  log_info "Creating version directory for $VERSION_ID..."
  VERSION_DIR="$BACKUP_DIR/$VERSION_ID"
  mkdir -p "$VERSION_DIR"

  # Create metadata snapshot
  log_info "Creating metadata.db snapshot..."
  cp "$PROJECT_DIR/metadata.db" "$VERSION_DIR/metadata.db"

  # Create Qdrant snapshot via API
  log_info "Creating Qdrant snapshot..."
  QDRANT_SNAPSHOT=$(curl -X POST "http://localhost:6333/collections/textbook_chunks/snapshots" \
    -H "Content-Type: application/json" 2>/dev/null | \
    python3 -c "import sys, json; print(json.load(sys.stdin)['result']['name'])")

  log_info "Qdrant snapshot created: $QDRANT_SNAPSHOT"

  # Download Qdrant snapshot
  curl -X GET "http://localhost:6333/collections/textbook_chunks/snapshots/$QDRANT_SNAPSHOT" \
    --output "$VERSION_DIR/textbook_chunks.snapshot" 2>/dev/null

  if [ -f "$VERSION_DIR/textbook_chunks.snapshot" ]; then
    log_info "✓ Qdrant snapshot downloaded"
  else
    log_error "✗ Failed to download Qdrant snapshot"
    exit 1
  fi

  # Create Neo4j export
  log_info "Creating Neo4j export (this may take 2-3 minutes)..."
  docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
    "CALL apoc.export.cypher.all(null, {stream: true, format: 'cypher-shell'}) \
     YIELD cypherStatements RETURN cypherStatements" 2>&1 | \
    tail -n +4 > "$VERSION_DIR/neo4j.cypher"

  if [ -f "$VERSION_DIR/neo4j.cypher" ]; then
    log_info "✓ Neo4j export created"
  else
    log_error "✗ Failed to create Neo4j export"
    exit 1
  fi

  # Calculate checksums
  log_info "Calculating checksums..."
  cd "$VERSION_DIR"
  shasum -a 256 * > checksums.txt
  cd "$PROJECT_DIR"

  # Get current counts
  CHAPTER_COUNT=$(sqlite3 "$PROJECT_DIR/metadata.db" "SELECT COUNT(*) FROM chapter_metadata;")
  PARAGRAPH_COUNT=$(curl -X GET "http://localhost:6333/collections/textbook_chunks" 2>/dev/null | \
    python3 -c "import sys, json; print(json.load(sys.stdin)['result']['points_count'])")

  log_info "Current counts - Chapters: $CHAPTER_COUNT, Paragraphs: $PARAGRAPH_COUNT"

  # Insert baseline entry into version_registry
  log_info "Registering baseline version in version_registry..."

  sqlite3 "$PROJECT_DIR/metadata.db" <<EOF
INSERT INTO version_registry (
    version_id,
    status,
    status_message,
    metadata_snapshot_path,
    qdrant_snapshot_path,
    neo4j_snapshot_path,
    chapter_count,
    paragraph_count,
    validation_passed,
    committed_by,
    notes
) VALUES (
    '$VERSION_ID',
    'pending',
    'Baseline version registration in progress',
    '$VERSION_DIR/metadata.db',
    '$VERSION_DIR/textbook_chunks.snapshot',
    '$VERSION_DIR/neo4j.cypher',
    $CHAPTER_COUNT,
    $PARAGRAPH_COUNT,
    0,
    'run_migration.sh',
    'Initial baseline version. Ground truth for all future versions.'
);
EOF

  if [ $? -eq 0 ]; then
    log_info "✓ Baseline version registered"
  else
    log_error "✗ Failed to register baseline version"
    exit 1
  fi

  # Create latest-copy
  log_info "Creating latest-copy backup..."
  LATEST_DIR="$BACKUP_DIR/latest-copy"
  mkdir -p "$LATEST_DIR"
  cp "$VERSION_DIR/metadata.db" "$LATEST_DIR/metadata.db"
  cp "$VERSION_DIR/textbook_chunks.snapshot" "$LATEST_DIR/textbook_chunks_latest.snapshot"
  cp "$VERSION_DIR/neo4j.cypher" "$LATEST_DIR/neo4j_latest.cypher"

  log_info "✓ latest-copy created"

  log_info "Baseline registration completed"
}

################################################################################
# Section 4: Neo4j Label Addition
################################################################################

add_neo4j_labels() {
  log_section "Section 4: Neo4j Label Addition"

  log_info "Adding :v1_baseline label to all paragraphs..."
  log_warn "This operation will process 36,216 nodes and may take 5-10 minutes"

  # Count paragraphs before labeling
  TOTAL_PARAGRAPHS=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
    "MATCH (p:Paragraph) RETURN count(p)" --format plain 2>&1 | tail -1)

  log_info "Total paragraphs to label: $TOTAL_PARAGRAPHS"

  # Add labels using batched approach
  log_info "Starting batched label addition (1000 nodes per batch)..."

  docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
    "CALL apoc.periodic.iterate(
       'MATCH (p:Paragraph) RETURN p',
       'SET p:v1_baseline',
       {batchSize: 1000, parallel: false}
     )" 2>&1 | grep -E "batches|committed|failed" || true

  if [ $? -eq 0 ]; then
    log_info "Label addition completed"
  else
    log_error "Label addition may have encountered errors"
    # Don't exit - verify counts instead
  fi

  # Verify label count
  LABELED_COUNT=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
    "MATCH (p:Paragraph:v1_baseline) RETURN count(p)" --format plain 2>&1 | tail -1)

  log_info "Paragraphs labeled: $LABELED_COUNT / $TOTAL_PARAGRAPHS"

  if [ "$LABELED_COUNT" -eq "$TOTAL_PARAGRAPHS" ]; then
    log_info "✓ All paragraphs successfully labeled"
  else
    log_error "✗ Label count mismatch: Expected $TOTAL_PARAGRAPHS, got $LABELED_COUNT"
    log_error "ROLLBACK TRIGGER 4: Neo4j label addition failure"
    exit 1
  fi
}

################################################################################
# Section 5: Validation
################################################################################

run_validation() {
  log_section "Section 5: Validation"

  log_info "Running validation checks..."

  # Check 1: Chapter count
  log_info "[1/7] Validating chapter count..."
  CHAPTER_COUNT=$(sqlite3 "$PROJECT_DIR/metadata.db" "SELECT COUNT(*) FROM chapter_metadata;")
  if [ "$CHAPTER_COUNT" -eq "$EXPECTED_CHAPTERS" ]; then
    log_info "✓ Chapter count matches ($CHAPTER_COUNT)"
  else
    log_error "✗ Chapter count mismatch: Expected $EXPECTED_CHAPTERS, got $CHAPTER_COUNT"
    log_error "ROLLBACK TRIGGER 3: Validation check failure"
    exit 1
  fi

  # Check 2: Qdrant-Neo4j count equality
  log_info "[2/7] Validating Qdrant-Neo4j count equality..."
  QDRANT_COUNT=$(curl -X GET "http://localhost:6333/collections/textbook_chunks" 2>/dev/null | \
    python3 -c "import sys, json; print(json.load(sys.stdin)['result']['points_count'])")
  NEO4J_COUNT=$(docker exec hybridflow-neo4j cypher-shell -u neo4j -p password \
    "MATCH (p:Paragraph) RETURN count(p)" --format plain 2>&1 | tail -1)

  if [ "$QDRANT_COUNT" -eq "$NEO4J_COUNT" ]; then
    log_info "✓ Counts match: Qdrant=$QDRANT_COUNT, Neo4j=$NEO4J_COUNT"
  else
    log_error "✗ Count mismatch: Qdrant=$QDRANT_COUNT, Neo4j=$NEO4J_COUNT"
    log_error "ROLLBACK TRIGGER 6: Inconsistent counts across systems"
    exit 1
  fi

  # Check 3-7: Run full validation checklist
  log_info "[3-7] Running comprehensive validation checklist..."
  if [ -f "$PROJECT_DIR/scripts/verify_chunk_consistency.py" ]; then
    cd "$PROJECT_DIR"
    if poetry run python scripts/verify_chunk_consistency.py; then
      log_info "✓ Chunk consistency validation passed"
    else
      log_error "✗ Chunk consistency validation failed"
      log_error "ROLLBACK TRIGGER 3: Validation check failure"
      exit 1
    fi
  else
    log_warn "verify_chunk_consistency.py not found, skipping detailed validation"
  fi

  # Test search functionality
  log_info "Testing search functionality..."
  SEARCH_RESULT=$(cd "$PROJECT_DIR" && poetry run hybridflow search "lung" --limit 1 2>&1 | grep -c "bailey\|sabiston\|schwartz" || echo "0")
  if [ "$SEARCH_RESULT" -gt 0 ]; then
    log_info "✓ Search functionality operational"
  else
    log_error "✗ Search query failed"
    log_error "ROLLBACK TRIGGER 5: Post-migration query failure"
    exit 1
  fi

  log_info "All validation checks passed"
}

################################################################################
# Section 6: Rollback Trigger Conditions
################################################################################

check_rollback_triggers() {
  log_section "Section 6: Rollback Trigger Check"

  log_info "Checking for rollback trigger conditions..."

  # Trigger 1: Table creation failure (already checked in schema migration)

  # Trigger 2: Baseline registration failure
  BASELINE_COUNT=$(sqlite3 "$PROJECT_DIR/metadata.db" \
    "SELECT COUNT(*) FROM version_registry WHERE version_id LIKE 'v1_baseline%';")
  if [ "$BASELINE_COUNT" -eq 0 ]; then
    log_error "ROLLBACK TRIGGER 2: Baseline registration failure"
    exit 1
  fi

  # Trigger 3-6: Covered in validation section

  log_info "✓ No rollback triggers detected"
}

################################################################################
# Section 7: Commit Migration
################################################################################

commit_migration() {
  log_section "Section 7: Commit Migration"

  log_info "Updating version_registry status to 'committed'..."

  sqlite3 "$PROJECT_DIR/metadata.db" <<EOF
UPDATE version_registry
SET status = 'committed',
    validation_passed = 1,
    status_message = 'Migration completed successfully',
    updated_at = CURRENT_TIMESTAMP
WHERE version_id = '$VERSION_ID';
EOF

  if [ $? -eq 0 ]; then
    log_info "✓ Version committed"
  else
    log_error "✗ Failed to update version status"
    exit 1
  fi

  # Log migration completion
  sqlite3 "$PROJECT_DIR/metadata.db" <<EOF
INSERT INTO operation_log (version_id, operation_type, status, details, completed_at)
VALUES (
    '$VERSION_ID',
    'migration',
    'completed',
    'Baseline version registration completed successfully',
    CURRENT_TIMESTAMP
);
EOF

  log_info "✓ Migration logged in operation_log"

  # Display final status
  sqlite3 "$PROJECT_DIR/metadata.db" \
    "SELECT version_id, status, chapter_count, paragraph_count, validation_passed
     FROM version_registry WHERE version_id = '$VERSION_ID';"

  log_info "Migration completed successfully!"
}

################################################################################
# Main Execution Flow
################################################################################

main() {
  log_section "Migration Execution: $VERSION_ID"
  log_info "Start time: $(date)"

  # Execute migration phases
  verify_backups
  run_schema_migration
  register_baseline
  add_neo4j_labels
  run_validation
  check_rollback_triggers
  commit_migration

  log_section "Migration Complete"
  log_info "End time: $(date)"
  log_info "Version ID: $VERSION_ID"
  log_info "Status: COMMITTED"
  log_info ""
  log_info "Next steps:"
  log_info "  1. Update CLAUDE.md with versioning features"
  log_info "  2. Tag migration in git: git tag -a v1.0.0-baseline"
  log_info "  3. Test version creation workflow"
  log_info ""
  log_info "For rollback instructions, see: docs/rollback_procedure.md"
}

# Execute main function
main "$@"
