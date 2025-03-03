# HybridFlow

**Production-ready data ingestion pipeline for hybrid vector-graph databases with adaptive schema handling, quality monitoring, and version control.**

## Overview

HybridFlow is an adaptive ingestion pipeline for hierarchical structured data with hybrid vector-graph storage. It provides automatic schema validation with graceful degradation, multi-tier parsing strategies, version tracking, and comprehensive quality monitoring. The system handles inconsistent JSON structures through intelligent fallback mechanisms while maintaining data integrity across Qdrant vector database and Neo4j graph database.

## Key Features

- **Adaptive Schema Handling**: Automatic schema validation with graceful degradation for inconsistent data structures
- **Hybrid Storage**: Seamless integration with Qdrant (vector database) and Neo4j (graph database)
- **Multi-tier Parsing**: Intelligent fallback mechanisms for handling varied JSON structures
- **Version Tracking**: Built-in content hashing and version management
- **Quality Monitoring**: Comprehensive data quality validation using Great Expectations
- **Production Ready**: Robust error handling, logging, and monitoring capabilities

## Architecture

HybridFlow uses a hybrid storage architecture combining:

- **Qdrant**: Vector storage for semantic search and similarity operations
- **Neo4j**: Graph storage for relationship traversal and complex queries
- **SQLite/PostgreSQL**: Metadata and version tracking

## Tech Stack

### Core Framework
- Python 3.11+
- Pydantic (data validation and settings)
- jsonschema (JSON schema validation)

### Databases
- **Vector**: Qdrant with sentence-transformers for embeddings
- **Graph**: Neo4j with official Python driver
- **Metadata**: SQLite/PostgreSQL with SQLAlchemy ORM

### Quality & Monitoring
- Great Expectations (data quality framework)
- structlog (structured logging)
- prometheus-client (metrics export)

### CLI & Interface
- Click/Typer (CLI framework)
- Rich (terminal formatting)
- tabulate (report formatting)

### Development
- Poetry (dependency management)
- pytest (testing framework)
- black, ruff, mypy (code quality)
- pre-commit (git hooks)

## Installation

```bash
# Install dependencies using Poetry
poetry install

# Or using pip
pip install -e .
```

## Quick Start

```python
from hybrid_flow import Pipeline

# Initialize the pipeline
pipeline = Pipeline(
    qdrant_config={...},
    neo4j_config={...}
)

# Ingest data
pipeline.ingest(data_source)
```

## Project Structure

```
hybrid-flow/
├── .git/                  # Git repository
├── .gitignore             # Git ignore rules
├── LICENSE                # Proprietary license
├── README.md              # This file
├── pyproject.toml         # Poetry configuration
├── config/                # Configuration templates
├── docs/                  # Documentation
├── examples/              # Usage examples
├── scripts/               # Utility scripts
├── src/
│   └── hybrid-flow/       # Main package
└── tests/                 # Test suite
    └── __init__.py
```

## Documentation

Comprehensive documentation is available in the `docs/` directory and includes:

- Architecture overview
- API reference
- Configuration guide
- Integration examples
- Performance tuning

## Development

```bash
# Install development dependencies
poetry install --with dev

# Run tests
pytest

# Run linters
ruff check .
mypy src/

# Format code
black .

# Run pre-commit hooks
pre-commit run --all-files
```

## License

**PROPRIETARY AND CONFIDENTIAL**

Copyright (c) 2025 Rushir Bhavsar. All Rights Reserved.

This software is proprietary and confidential. Unauthorized copying, distribution, modification, or use of this software, via any medium, is strictly prohibited.

## Acknowledgments

Built with modern Python best practices and production-grade tooling for reliable data ingestion at scale.
