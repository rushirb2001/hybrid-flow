"""Command-line interface for HybridFlow ingestion pipeline."""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from hybridflow.cli.query import add_query_commands
from hybridflow.ingestion.pipeline import IngestionPipeline
from hybridflow.storage.neo4j_client import Neo4jStorage


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for CLI.

    Args:
        verbose: Enable verbose (DEBUG) logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config() -> dict:
    """Load configuration from environment variables.

    Returns:
        Dictionary with configuration values
    """
    # Load .env file if it exists
    load_dotenv()

    config = {
        "qdrant_host": os.getenv("QDRANT_HOST", "localhost"),
        "qdrant_port": int(os.getenv("QDRANT_PORT", "6333")),
        "neo4j_uri": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        "neo4j_user": os.getenv("NEO4J_USER", "neo4j"),
        "neo4j_password": os.getenv("NEO4J_PASSWORD", "password"),
        "metadata_db_path": os.getenv("METADATA_DB_PATH", "./metadata.db"),
        "embedding_model": os.getenv(
            "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
        ),
    }

    return config


def create_pipeline(config: dict) -> IngestionPipeline:
    """Create and initialize ingestion pipeline.

    Args:
        config: Configuration dictionary

    Returns:
        Initialized IngestionPipeline instance
    """
    return IngestionPipeline(
        qdrant_host=config["qdrant_host"],
        qdrant_port=config["qdrant_port"],
        neo4j_uri=config["neo4j_uri"],
        neo4j_user=config["neo4j_user"],
        neo4j_password=config["neo4j_password"],
        metadata_db_path=config["metadata_db_path"],
        embedding_model=config["embedding_model"],
    )


def cmd_ingest_file(args: argparse.Namespace) -> int:
    """Ingest a single chapter file.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    file_path = Path(args.file_path)
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return 1

    if not file_path.suffix == ".json":
        logger.error(f"File must be a JSON file: {file_path}")
        return 1

    try:
        config = load_config()
        pipeline = create_pipeline(config)

        logger.info(f"Ingesting file: {file_path}")
        result = pipeline.ingest_chapter(str(file_path), force=args.force)

        if result["status"] == "success":
            logger.info(
                f"Successfully ingested {result['chunks_inserted']} chunks from {file_path}"
            )
            return 0
        elif result["status"] == "skipped":
            logger.info(f"File skipped (unchanged): {file_path}")
            return 0
        else:
            logger.error(f"Ingestion failed: {result.get('error', 'Unknown error')}")
            return 1

    except Exception as e:
        logger.error(f"Error during ingestion: {e}", exc_info=args.verbose)
        return 1
    finally:
        pipeline.close()


def cmd_ingest_dir(args: argparse.Namespace) -> int:
    """Ingest all JSON files from a directory.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    dir_path = Path(args.directory_path)
    if not dir_path.exists():
        logger.error(f"Directory not found: {dir_path}")
        return 1

    if not dir_path.is_dir():
        logger.error(f"Path is not a directory: {dir_path}")
        return 1

    try:
        config = load_config()
        pipeline = create_pipeline(config)

        logger.info(f"Ingesting directory: {dir_path}")
        result = pipeline.ingest_directory(str(dir_path), force=args.force)

        logger.info(
            f"Ingestion complete: {result['successful_count']} succeeded, "
            f"{result['skipped_count']} skipped, {result['failed_count']} failed "
            f"(total: {result['total_files']} files)"
        )

        return 0 if result["failed_count"] == 0 else 1

    except Exception as e:
        logger.error(f"Error during directory ingestion: {e}", exc_info=args.verbose)
        return 1
    finally:
        pipeline.close()


def cmd_ingest_all(args: argparse.Namespace) -> int:
    """Ingest all chapters from all textbooks.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Default data directories
    base_dir = Path(args.base_dir if args.base_dir else "./data")
    textbooks = ["bailey", "sabiston", "schwartz"]

    try:
        config = load_config()
        pipeline = create_pipeline(config)

        total_successful = 0
        total_skipped = 0
        total_failed = 0
        total_files = 0

        for textbook in textbooks:
            textbook_dir = base_dir / textbook
            if not textbook_dir.exists():
                logger.warning(f"Directory not found, skipping: {textbook_dir}")
                continue

            logger.info(f"Processing textbook: {textbook}")
            result = pipeline.ingest_directory(str(textbook_dir), force=args.force)

            total_successful += result["successful_count"]
            total_skipped += result["skipped_count"]
            total_failed += result["failed_count"]
            total_files += result["total_files"]

            logger.info(
                f"{textbook}: {result['successful_count']} succeeded, "
                f"{result['skipped_count']} skipped, {result['failed_count']} failed"
            )

        logger.info(
            f"\nOverall: {total_successful} succeeded, {total_skipped} skipped, "
            f"{total_failed} failed (total: {total_files} files)"
        )

        return 0 if total_failed == 0 else 1

    except Exception as e:
        logger.error(f"Error during batch ingestion: {e}", exc_info=args.verbose)
        return 1
    finally:
        pipeline.close()


def cmd_version_list(args: argparse.Namespace) -> int:
    """List all available versions.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        pipeline = create_pipeline(config)

        versions = pipeline.list_versions()

        print("\n" + "=" * 60)
        print("Available Versions")
        print("=" * 60)

        if not versions:
            print("No versions found.")
        else:
            for v in versions:
                print(f"\n  Version: {v['version_id']}")
                print(f"  Status:  {v['status']}")
                if v.get('description'):
                    print(f"  Description: {v['description']}")

        print("=" * 60 + "\n")
        return 0

    except Exception as e:
        logger.error(f"Error listing versions: {e}", exc_info=args.verbose)
        return 1
    finally:
        pipeline.close()


def cmd_version_info(args: argparse.Namespace) -> int:
    """Get detailed information about a specific version.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    import json

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        pipeline = create_pipeline(config)

        version_info = pipeline.version_manager.get_version_info(args.version_id)

        if not version_info:
            logger.error(f"Version not found: {args.version_id}")
            return 1

        print("\n" + "=" * 60)
        print(f"Version Details: {args.version_id}")
        print("=" * 60)
        print(json.dumps(version_info, indent=2, default=str))
        print("=" * 60 + "\n")
        return 0

    except Exception as e:
        logger.error(f"Error getting version info: {e}", exc_info=args.verbose)
        return 1
    finally:
        pipeline.close()


def cmd_version_validate(args: argparse.Namespace) -> int:
    """Validate data consistency across all storage systems.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        pipeline = create_pipeline(config)

        logger.info("Validating all storage systems...")
        report = pipeline.validate_system()

        print("\n" + "=" * 60)
        print("Cross-System Validation Report")
        print("=" * 60)

        print(f"\nVersion: {report['version_id']}")
        print(f"Status:  {report['status'].upper()}")

        print(f"\nSQLite (Metadata DB):")
        print(f"  Chapters: {report['sqlite'].get('chapters', 'N/A')}")

        print(f"\nQdrant (Vector DB):")
        qdrant = report.get('qdrant', {})
        print(f"  Collection: {qdrant.get('collection_name', 'N/A')}")
        print(f"  Point Count: {qdrant.get('point_count', 'N/A')}")
        print(f"  Status: {qdrant.get('status', 'N/A')}")

        print(f"\nNeo4j (Graph DB):")
        neo4j = report.get('neo4j', {})
        print(f"  Paragraphs: {neo4j.get('node_counts', {}).get('Paragraph', 'N/A')}")
        print(f"  Status: {neo4j.get('status', 'N/A')}")

        print(f"\nCross-System Check:")
        cross = report.get('cross_system', {})
        print(f"  Qdrant Count: {cross.get('qdrant_count', 'N/A')}")
        print(f"  Neo4j Count: {cross.get('neo4j_count', 'N/A')}")
        print(f"  Match: {'✓ YES' if cross.get('qdrant_neo4j_match') else '✗ NO'}")

        print("=" * 60 + "\n")

        if report['status'] == 'valid':
            logger.info("✓ Validation passed")
            return 0
        else:
            logger.error("✗ Validation failed")
            return 1

    except Exception as e:
        logger.error(f"Error during validation: {e}", exc_info=args.verbose)
        return 1
    finally:
        pipeline.close()


def cmd_version_rotate(args: argparse.Namespace) -> int:
    """Rotate old versions by deleting versions beyond keep_count.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        pipeline = create_pipeline(config)

        logger.info(f"Rotating versions (keeping {args.keep_count} most recent)...")
        result = pipeline.rotate_old_versions(keep_count=args.keep_count)

        print("\n" + "=" * 60)
        print("Version Rotation Result")
        print("=" * 60)

        print(f"\nDeleted ({len(result['deleted'])}):")
        for v in result['deleted']:
            print(f"  - {v}")

        print(f"\nSkipped ({len(result['skipped'])}):")
        for v in result['skipped']:
            print(f"  - {v}")

        print(f"\nRemaining: {result['remaining']}")
        print("=" * 60 + "\n")

        return 0

    except Exception as e:
        logger.error(f"Error during rotation: {e}", exc_info=args.verbose)
        return 1
    finally:
        pipeline.close()


def cmd_version_migrate_baseline(args: argparse.Namespace) -> int:
    """Run baseline migration to register existing data.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = load_config()
        pipeline = create_pipeline(config)

        logger.info("Running baseline migration...")
        baseline_id = pipeline.version_manager.run_baseline_migration(
            description=args.description or "Initial baseline from existing data"
        )

        print("\n" + "=" * 60)
        print("Baseline Migration Complete")
        print("=" * 60)
        print(f"\nBaseline ID: {baseline_id}")
        print("=" * 60 + "\n")

        logger.info(f"✓ Baseline migration successful: {baseline_id}")
        return 0

    except Exception as e:
        logger.error(f"Error during baseline migration: {e}", exc_info=args.verbose)
        return 1
    finally:
        pipeline.close()


def cmd_validate_neo4j(args: argparse.Namespace) -> int:
    """Validate Neo4j graph and generate report.

    Args:
        args: Command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    import json

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    try:
        config = load_config()

        # Create Neo4j storage client
        storage = Neo4jStorage(
            uri=config["neo4j_uri"],
            user=config["neo4j_user"],
            password=config["neo4j_password"],
        )

        logger.info(f"Validating Neo4j graph (version: {args.version or 'current'})...")

        # Get comprehensive stats
        stats = storage.get_graph_stats(args.version)

        # Add Qdrant comparison if requested
        if args.compare_qdrant:
            from qdrant_client import QdrantClient

            logger.info("Comparing with Qdrant...")
            qclient = QdrantClient(host=config["qdrant_host"], port=config["qdrant_port"])

            # Get all Qdrant chunk_ids
            chunk_ids = set()
            offset = None
            while True:
                result, offset = qclient.scroll(
                    "textbook_chunks",
                    limit=1000,
                    offset=offset,
                    with_payload=True
                )
                if not result:
                    break
                chunk_ids.update(
                    p.payload.get("chunk_id") for p in result if p.payload.get("chunk_id")
                )
                if offset is None:
                    break

            comparison = storage.compare_with_qdrant(chunk_ids, args.version)
            stats["qdrant_comparison"] = comparison

        # Output to file if specified
        if args.output:
            output_path = Path(args.output)
            with open(output_path, "w") as f:
                json.dump(stats, f, indent=2)
            logger.info(f"Validation report saved to: {output_path}")

        # Print summary
        print("\n" + "=" * 60)
        print(f"Neo4j Validation Report - Version: {stats['version_id']}")
        print("=" * 60)
        print(f"\nStatus: {stats['status'].upper()}")
        print(f"\nNode Counts:")
        for node_type, count in stats["node_counts"].items():
            print(f"  {node_type:20s}: {count:>6d}")

        print(f"\nData Quality:")
        print(f"  Orphan Paragraphs:      {stats['orphan_paragraphs']:>6d}")
        print(f"  Broken NEXT Chains:     {stats['broken_next_chains']:>6d}")
        print(f"  Broken PREV Chains:     {stats['broken_prev_chains']:>6d}")
        print(f"  Duplicate Chunk IDs:    {stats['duplicate_chunk_ids']:>6d}")
        print(f"  Invalid Hierarchies:    {stats['invalid_hierarchies']:>6d}")

        print(f"\nText Statistics:")
        print(f"  Average Length:         {stats['text_stats']['avg']:>6.1f} chars")
        print(f"  Min Length:             {stats['text_stats']['min']:>6d} chars")
        print(f"  Max Length:             {stats['text_stats']['max']:>6d} chars")

        print(f"\nCross-References:")
        print(f"  Paragraphs with Refs:   {stats['paragraphs_with_cross_references']:>6d}")

        if "qdrant_comparison" in stats:
            comp = stats["qdrant_comparison"]
            print(f"\nQdrant Consistency:")
            print(f"  Neo4j Count:            {comp['neo4j_count']:>6d}")
            print(f"  Qdrant Count:           {comp['qdrant_count']:>6d}")
            print(f"  Common Count:           {comp['common_count']:>6d}")
            print(f"  Only in Neo4j:          {comp['only_in_neo4j']:>6d}")
            print(f"  Only in Qdrant:         {comp['only_in_qdrant']:>6d}")
            print(f"  Status:                 {comp['consistency'].upper()}")

        print("=" * 60 + "\n")

        # Exit code based on validation status
        if stats["status"] == "valid":
            if "qdrant_comparison" in stats and stats["qdrant_comparison"]["consistency"] != "pass":
                logger.warning("Graph is valid but Qdrant consistency check failed")
                return 1
            logger.info("✓ Validation passed")
            return 0
        else:
            logger.error("✗ Validation failed - issues found")
            return 1

    except Exception as e:
        logger.error(f"Error during validation: {e}", exc_info=args.verbose)
        return 1
    finally:
        storage.close()


def main() -> int:
    """Main CLI entry point.

    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(
        description="HybridFlow: Adaptive ingestion pipeline for hierarchical structured data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (DEBUG) logging"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ingest-file command
    parser_file = subparsers.add_parser(
        "ingest-file", help="Ingest a single chapter JSON file"
    )
    parser_file.add_argument("file_path", help="Path to JSON file to ingest")
    parser_file.add_argument(
        "--force", action="store_true", help="Force re-ingestion even if unchanged"
    )
    parser_file.set_defaults(func=cmd_ingest_file)

    # ingest-dir command
    parser_dir = subparsers.add_parser(
        "ingest-dir", help="Ingest all JSON files from a directory"
    )
    parser_dir.add_argument("directory_path", help="Path to directory containing JSON files")
    parser_dir.add_argument(
        "--force", action="store_true", help="Force re-ingestion even if unchanged"
    )
    parser_dir.set_defaults(func=cmd_ingest_dir)

    # ingest-all command
    parser_all = subparsers.add_parser(
        "ingest-all", help="Ingest all chapters from all textbooks"
    )
    parser_all.add_argument(
        "--base-dir",
        default="./data",
        help="Base directory containing textbook subdirectories (default: ./data)",
    )
    parser_all.add_argument(
        "--force", action="store_true", help="Force re-ingestion even if unchanged"
    )
    parser_all.set_defaults(func=cmd_ingest_all)

    # validate-neo4j command
    parser_validate = subparsers.add_parser(
        "validate-neo4j", help="Validate Neo4j graph and generate report"
    )
    parser_validate.add_argument(
        "--version",
        help="Version ID to validate (e.g., v1_baseline, v2_test). If not specified, validates current graph.",
    )
    parser_validate.add_argument(
        "--output",
        help="Output file path for JSON report (e.g., report.json)",
    )
    parser_validate.add_argument(
        "--compare-qdrant",
        action="store_true",
        help="Compare with Qdrant for consistency check",
    )
    parser_validate.set_defaults(func=cmd_validate_neo4j)

    # version command group
    parser_version = subparsers.add_parser(
        "version", help="Version management commands"
    )
    version_subparsers = parser_version.add_subparsers(dest="version_command", help="Version subcommands")

    # version list
    parser_version_list = version_subparsers.add_parser("list", help="List all available versions")
    parser_version_list.set_defaults(func=cmd_version_list)

    # version info
    parser_version_info = version_subparsers.add_parser("info", help="Get details about a specific version")
    parser_version_info.add_argument("version_id", help="Version ID to get info for")
    parser_version_info.set_defaults(func=cmd_version_info)

    # version validate
    parser_version_validate = version_subparsers.add_parser(
        "validate", help="Validate data consistency across all storage systems"
    )
    parser_version_validate.set_defaults(func=cmd_version_validate)

    # version rotate
    parser_version_rotate = version_subparsers.add_parser(
        "rotate", help="Rotate old versions by deleting beyond keep count"
    )
    parser_version_rotate.add_argument(
        "--keep-count", type=int, default=5, help="Number of recent versions to keep (default: 5)"
    )
    parser_version_rotate.set_defaults(func=cmd_version_rotate)

    # version migrate-baseline
    parser_version_baseline = version_subparsers.add_parser(
        "migrate-baseline", help="Run baseline migration to register existing data"
    )
    parser_version_baseline.add_argument(
        "--description", help="Description for baseline version"
    )
    parser_version_baseline.set_defaults(func=cmd_version_migrate_baseline)

    # Add query commands
    add_query_commands(subparsers)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
