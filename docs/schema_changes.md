# Schema Changes

## Ingestion Log Schema Enhancement (V1.3)

### Overview
Enhanced the `ingestion_log` table to support versioning and detailed operation tracking.

### Before (Original Schema)

```sql
CREATE TABLE ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chapter_id INTEGER,
    timestamp DATETIME,
    status VARCHAR(20) NOT NULL,
    parsing_strategy VARCHAR(50),
    error_message TEXT,
    chunks_inserted INTEGER,
    FOREIGN KEY(chapter_id) REFERENCES chapter_metadata (id)
);
```

**Columns:**
- `id` - Auto-incrementing primary key
- `chapter_id` - Foreign key to chapter_metadata
- `timestamp` - Timestamp of the ingestion operation
- `status` - Status of the operation (success, failure)
- `parsing_strategy` - Strategy used for parsing
- `error_message` - Error message if failed
- `chunks_inserted` - Number of chunks successfully inserted

### After (Enhanced Schema)

```sql
CREATE TABLE ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chapter_id INTEGER,
    timestamp DATETIME,
    status VARCHAR(20) NOT NULL,
    parsing_strategy VARCHAR(50),
    error_message TEXT,
    chunks_inserted INTEGER,
    version_id TEXT,
    operation_type TEXT,
    chunks_before INTEGER,
    chunks_after INTEGER,
    duration_ms INTEGER,
    metadata_json TEXT,
    diff_json TEXT,
    FOREIGN KEY(chapter_id) REFERENCES chapter_metadata (id)
);
```

**New Columns Added:**
- `version_id` - Version identifier for this operation (links to version_registry)
- `operation_type` - Type of operation (insert, update, delete)
- `chunks_before` - Count of chunks before operation
- `chunks_after` - Count of chunks after operation
- `duration_ms` - Duration of operation in milliseconds
- `metadata_json` - Additional metadata as JSON string
- `diff_json` - Diff information as JSON string

### Migration Details

**Migration Method:** `migrate_ingestion_log_schema()`

**SQL Operations:**
```sql
ALTER TABLE ingestion_log ADD COLUMN version_id TEXT;
ALTER TABLE ingestion_log ADD COLUMN operation_type TEXT;
ALTER TABLE ingestion_log ADD COLUMN chunks_before INTEGER;
ALTER TABLE ingestion_log ADD COLUMN chunks_after INTEGER;
ALTER TABLE ingestion_log ADD COLUMN duration_ms INTEGER;
ALTER TABLE ingestion_log ADD COLUMN metadata_json TEXT;
ALTER TABLE ingestion_log ADD COLUMN diff_json TEXT;
```

### Backward Compatibility

The schema changes are **fully backward compatible**:

1. All new columns are nullable (optional)
2. Existing code using the old signature continues to work:
   ```python
   db.log_ingestion(
       chapter_id=1,
       status='success',
       parsing_strategy='strict',
       error_message=None,
       chunks_inserted=100
   )
   ```

3. New code can use enhanced signature:
   ```python
   db.log_ingestion(
       chapter_id=1,
       status='success',
       parsing_strategy='strict',
       error_message=None,
       chunks_inserted=100,
       version_id='v2_minor_20251225_120000',
       operation_type='insert',
       chunks_before=0,
       chunks_after=100,
       duration_ms=5000,
       metadata_json='{"source": "batch_import"}',
       diff_json='{"added": 100, "removed": 0}'
   )
   ```

### Testing

**Backward Compatibility Test:**
```bash
poetry run python -c "from src.hybridflow.storage.metadata_db import MetadataDatabase; \
  db = MetadataDatabase('metadata.db'); \
  db.log_ingestion(chapter_id=1, status='test', parsing_strategy='strict', \
                   error_message=None, chunks_inserted=100); \
  print('Backward compatible log successful')"
```

**Enhanced Signature Test:**
```bash
poetry run python -c "from src.hybridflow.storage.metadata_db import MetadataDatabase; \
  db = MetadataDatabase('metadata.db'); \
  db.log_ingestion(chapter_id=1, status='test', parsing_strategy='strict', \
                   error_message=None, chunks_inserted=100, version_id='v2_test', \
                   operation_type='insert', duration_ms=5000); \
  print('Enhanced log successful')"
```

**Unit Tests:**
```bash
poetry run pytest tests/test_metadata_db.py::test_log_ingestion -v
```

### Related Changes

This schema enhancement supports the versioning infrastructure introduced in:
- V1.1: Version Registry Tables (`version_registry`, `operation_log`)
- V1.2: Baseline Version Registration (`v1_baseline`)

### Migration Applied

The migration is automatically applied when `create_tables()` is called, ensuring:
- Fresh databases get the enhanced schema immediately
- Existing databases are migrated seamlessly
- Test databases are always up-to-date
