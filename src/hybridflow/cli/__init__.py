"""Command-line interface for HybridFlow ingestion pipeline."""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from hybridflow.cli.query import add_query_commands
from hybridflow.ingestion.pipeline import IngestionPipeline


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

    # Add query commands
    add_query_commands(subparsers)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
