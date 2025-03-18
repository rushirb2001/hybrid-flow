# Versioning Specification

## Version Identifier Format

### Structure
All version identifiers follow the format:
```
v{N}_{type}_{YYYYMMDD}_{HHMMSS}
```

### Components
- **v{N}**: Version sequence number (v1, v2, v3, etc.)
- **{type}**: Version type identifier
  - `baseline`: Initial baseline version (v1_baseline only)
  - `minor`: Minor content updates (chapter modifications)
  - `major`: Major updates (new textbooks, embedding model changes)
- **{YYYYMMDD}**: Migration execution date
- **{HHMMSS}**: Migration execution time

### Examples
```
v1_baseline_20251225_195800  # Initial baseline created Dec 25, 2025 at 19:58:00
v2_minor_20251226_143022     # Minor update on Dec 26, 2025 at 14:30:22
v3_major_20260115_090000     # Major update on Jan 15, 2026 at 09:00:00
```

### Special Version Identifiers
- **`v1_baseline`**: The immutable ground truth version, never deleted
- **`latest-copy`**: Safety backup of the most recent committed version
- **`staging`**: Temporary workspace during ingestion operations

## Version State Transitions

### States

#### 1. pending
- **Definition**: Snapshot created, awaiting ingestion
- **Entry Condition**: Version registered in version_registry
- **Actions Allowed**: Start ingestion, cancel
- **Next States**: staging, cancelled

#### 2. staging
- **Definition**: Ingestion operation in progress
- **Entry Condition**: Ingestion pipeline started
- **Actions Allowed**: Continue ingestion, abort
- **Storage**:
  - SQLite: Staging table `chapter_metadata_staging`
  - Qdrant: Staging collection `textbook_chunks_staging`
  - Neo4j: Nodes with `:staging` label
- **Next States**: validating, rolled_back

#### 3. validating
- **Definition**: Post-ingestion validation checks running
- **Entry Condition**: Ingestion completed successfully
- **Actions Allowed**: Run validation checks, manual inspection
- **Validation Checks**: See validation_checklist.md
- **Next States**: committed, rolled_back

#### 4. committed
- **Definition**: Successfully validated and promoted to production
- **Entry Condition**: All validation checks passed
- **Actions**:
  - Promote staging to production
  - Update `latest-copy`
  - Archive old versions per sliding window policy
- **Next States**: archived (after window expires)

#### 5. rolled_back
- **Definition**: Failed validation or manual rollback triggered
- **Entry Condition**: Validation failure or manual intervention
- **Actions**:
  - Delete staging data
  - Restore from `latest-copy` if needed
  - Log failure reason in operation_log
- **Next States**: archived (after retention period)

#### 6. archived
- **Definition**: Rotated out of sliding window, kept for historical reference
- **Entry Condition**: Exceeded retention window (5 versions)
- **Actions**:
  - Mark as read-only
  - Compress snapshots if storage constrained
  - Never delete v1_baseline
- **Next States**: None (terminal state)

### State Transition Diagram
```
pending → staging → validating → committed → archived
   ↓         ↓          ↓
cancelled  rolled_back  rolled_back
                ↓          ↓
            archived   archived
```

### State Persistence
All state transitions are recorded in the `version_registry` table:
```sql
UPDATE version_registry
SET status = 'new_state',
    updated_at = CURRENT_TIMESTAMP,
    status_message = 'Transition reason'
WHERE version_id = 'vN_type_YYYYMMDD_HHMMSS';
```

## Sliding Window Policy

### Retention Strategy
Maintain a sliding window of versions to balance storage costs with rollback capability.

### Window Components

#### 1. Baseline Version (v1_baseline)
- **Count**: 1 (immutable)
- **Retention**: PERMANENT
- **Purpose**: Ground truth reference, disaster recovery
- **Storage**: Full snapshots of all three databases
- **Never deleted**: Critical safety guarantee

#### 2. Historical Versions
- **Count**: 5 most recent committed versions
- **Retention**: Rolling window (FIFO after 5 versions)
- **Purpose**: Rollback capability, change tracking
- **Storage**: Full snapshots
- **Deletion**: Oldest version deleted when 6th new version commits

#### 3. Latest-Copy
- **Count**: 1 (always current)
- **Retention**: Updated on every commit
- **Purpose**: Fast rollback without snapshot restoration
- **Storage**: Live copy of production data
- **Special**: Continuously updated, never has timestamp

#### 4. Staging
- **Count**: 1 (temporary during ingestion)
- **Retention**: Deleted after commit or rollback
- **Purpose**: Isolated workspace for new data
- **Storage**: Separate tables/collections/labels
- **Lifecycle**: Created on ingestion start, removed after validation

### Total Storage Footprint
At any given time, the system maintains:
```
1 baseline + 5 historical + 1 latest-copy + 0-1 staging = 7-8 versions
```

### Storage Distribution
- **SQLite**: 7-8 complete metadata.db files (~76 KB each = ~600 KB total)
- **Qdrant**: 7-8 snapshots (~145 MB each = ~1.1 GB total)
- **Neo4j**: 7-8 exports (~41 MB each = ~320 MB total)
- **Total**: ~1.5 GB for full version history

### Window Advancement Example
```
Initial State:
[v1_baseline] [v2] [v3] [v4] [v5] [v6] [latest-copy]

After v7 commits:
[v1_baseline] [v3] [v4] [v5] [v6] [v7] [latest-copy]
              ↑ v2 archived (oldest removed)

After v8 commits:
[v1_baseline] [v4] [v5] [v6] [v7] [v8] [latest-copy]
              ↑ v3 archived (oldest removed)
```

### Archive Management
When a version is rotated out:
1. Update status to `archived` in version_registry
2. Move snapshots to `backups/archived/` directory
3. Compress snapshots to save space (optional)
4. Retain for 90 days before permanent deletion (configurable)
5. Exception: v1_baseline NEVER archived or deleted

### Disk Space Monitoring
```bash
# Check current version storage
du -sh backups/v*

# Identify oldest version in window
sqlite3 metadata.db "SELECT version_id FROM version_registry
  WHERE status='committed'
  ORDER BY created_at ASC
  LIMIT 1 OFFSET 5"

# Archive old version (manual or automated)
./scripts/archive_version.sh v2_minor_20251226_143022
```

### Emergency Cleanup
If disk space becomes critical:
1. Compress all Qdrant snapshots (145 MB → ~50 MB each)
2. Archive versions 6+ (keep only 3 most recent)
3. WARNING: Never delete v1_baseline or latest-copy
4. Consider adding external storage for archives

## Version Registry Schema

```sql
CREATE TABLE version_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,  -- pending, staging, validating, committed, rolled_back, archived
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
```

## Best Practices

### Version Creation
- Always create snapshot before ingestion
- Use descriptive status_message for transitions
- Record all validation results in operation_log

### Window Management
- Monitor disk space weekly
- Archive aggressively if storage constrained
- Keep v1_baseline on separate backup drive

### Rollback Safety
- Always verify latest-copy before major operations
- Test rollback procedures quarterly
- Document all manual interventions

### Compliance
- Audit trail: All state transitions logged
- Immutability: v1_baseline never modified
- Traceability: Every version has full metadata
