# HybridFlow

**Production-ready hybrid retrieval system combining vector similarity search with knowledge graph traversal for medical textbook content.**

## Overview

HybridFlow is an adaptive ingestion and retrieval pipeline for hierarchical structured data with hybrid vector-graph storage. It provides semantic search over 36,000+ medical textbook paragraphs with graph-based context expansion, cross-reference resolution, and citation formatting. The system is designed for integration with LangGraph, LangChain, and other agentic frameworks.

## Project Statistics

### Scale Metrics

| Metric | Value |
|--------|-------|
| Paragraphs indexed | **36,290** |
| Knowledge graph nodes | **107,454** |
| Graph relationships | **106,859** |
| Chapters processed | **220** |
| Figures indexed | **4,649** |
| Tables indexed | **1,256** |
| Cross-referenced paragraphs | **3,507** |
| Medical textbooks | 3 (Bailey & Love, Sabiston, Schwartz) |

### Graph Node Distribution

| Node Type | Count |
|-----------|-------|
| Paragraph | 36,301 |
| Subsection | 7,788 |
| Figure | 4,649 |
| Section | 2,023 |
| Subsubsection | 1,413 |
| Table | 1,256 |
| Chapter | 269 |

### Performance Metrics

| Operation | p50 | p95 | Target | Status |
|-----------|-----|-----|--------|--------|
| Graph context retrieval | 12.0ms | 16.6ms | <100ms | PASS |
| Sequential navigation (NEXT/PREV) | 10.3ms | 11.5ms | <150ms | PASS |
| Cross-reference resolution | 9.5ms | 12.1ms | <100ms | PASS |
| Chapter metadata lookup | 0.2ms | 0.5ms | <50ms | PASS |
| Health check (3 backends) | 6.7ms | 13.5ms | <200ms | PASS |
| Tool dispatch overhead | ~0ms | <0.1ms | <5ms | PASS |
| Avg per-call (connection pooled) | 5.4ms | - | - | - |

## Key Features

- **Hybrid Search**: Combines vector similarity (Qdrant) with graph traversal (Neo4j)
- **Context Expansion**: 4 presets (none, minimal, standard, comprehensive) for configurable result enrichment
- **Sequential Navigation**: NEXT/PREV paragraph relationships for reading flow
- **Cross-Reference Resolution**: Automatic figure and table linking with file paths
- **Citation Formatting**: Proper academic citations with textbook, chapter, section, and page
- **Agentic Integration**: Tool definitions for LangGraph/LangChain binding
- **Version Control**: Content hashing, transactional ingestion, and snapshot management

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     HybridFlowAPI                           │
│  vector_search() | hybrid_search() | get_context()         │
│  get_surrounding() | get_references() | invoke_tool()      │
└─────────────────────────────────────────────────────────────┘
              │                    │                  │
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Qdrant         │  │  Neo4j          │  │  SQLite         │
│  Vector Store   │  │  Graph Store    │  │  Metadata       │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ 36K vectors     │  │ 107K nodes      │  │ 220 chapters    │
│ 768-dim PubMed  │  │ 107K relations  │  │ Version control │
│ Cosine sim      │  │ 4-level hier    │  │ Ingestion logs  │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

## Tech Stack

### Core Framework
- Python 3.11+
- Pydantic 2.0 (data validation)
- SentenceTransformers (embeddings)

### Databases
- **Vector**: Qdrant (768-dim PubMedBERT embeddings, cosine similarity)
- **Graph**: Neo4j (hierarchical relationships, NEXT/PREV navigation)
- **Metadata**: SQLite with SQLAlchemy ORM (versioning, tracking)

### Embedding Model
- `pritamdeka/S-PubMedBert-MS-MARCO-SCIFACT` (768 dimensions)
- Optimized for medical/scientific text

## Installation

```bash
poetry install

docker-compose up -d

cp .env.example .env
```

## Quick Start

### Python API

```python
from hybridflow import HybridFlowAPI, ExpansionConfig

api = HybridFlowAPI()

# Vector search (fast, conceptual queries)
results = api.vector_search("hemorrhagic shock", limit=5)

# Hybrid search with context expansion
results = api.hybrid_search(
    "thoracotomy procedure",
    limit=5,
    expansion="comprehensive"  # none | minimal | standard | comprehensive
)

# Graph operations
context = api.get_context("bailey:ch60:2.1.1")
surrounding = api.get_surrounding("bailey:ch60:2.1.1", before=2, after=2)
references = api.get_references("bailey:ch60:2.1.1")

# System stats and health
stats = api.get_stats()
health = api.health_check()

api.close()
```

### CLI

```bash
# Search
poetry run hybridflow search "lung anatomy" --limit 10
poetry run hybridflow search "surgery" --expand comprehensive

# Ingestion
poetry run hybridflow ingest-all
poetry run hybridflow ingest-file data/bailey/chapter_60.json
```

### Agentic Integration

```python
from hybridflow import HybridFlowAPI

api = HybridFlowAPI()

# Get LangChain-compatible tool definitions
tools = api.as_tool_definitions()

# Dynamic tool invocation
result = api.invoke_tool("hybrid_search", query="shock management", limit=3)
result = api.invoke_tool("get_context", chunk_id="bailey:ch60:2.1.1")
```

## Expansion Presets

| Preset | Context | Paragraphs | Section | References |
|--------|---------|------------|---------|------------|
| `none` | - | - | - | - |
| `minimal` | Hierarchy | - | - | - |
| `standard` | Hierarchy | 1 before/after | - | - |
| `comprehensive` | Hierarchy | 2 before/after | Summary | Figures/Tables |

## Project Structure

```
hybrid-flow/
├── src/hybridflow/
│   ├── api.py              # Unified API facade
│   ├── models.py           # Pydantic models
│   ├── cli/                # Command-line interface
│   ├── ingestion/          # Data ingestion pipeline
│   ├── parsing/            # Chunk generation, embeddings
│   ├── retrieval/          # QueryEngine, hybrid search
│   ├── storage/            # Qdrant, Neo4j, SQLite clients
│   └── validation/         # Schema validation
├── tests/                  # Test suite
├── docker-compose.yml      # Qdrant + Neo4j services
└── pyproject.toml          # Poetry configuration
```

## Development

```bash
poetry install --with dev

pytest -v

ruff check .
mypy src/
black .
```

## License

**PROPRIETARY AND CONFIDENTIAL**

Copyright (c) 2025 Rushir Bhavsar. All Rights Reserved.

This software is proprietary and confidential. Unauthorized copying, distribution, modification, or use of this software, via any medium, is strictly prohibited.
