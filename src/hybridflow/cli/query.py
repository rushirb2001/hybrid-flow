"""CLI commands for querying and searching the knowledge base."""

import argparse
import json
import logging
import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase

from hybridflow.models import ExpansionConfig
from hybridflow.retrieval.query import QueryEngine
from hybridflow.storage.qdrant_client import QdrantStorage

load_dotenv()


def cmd_search(args: argparse.Namespace) -> int:
    """Search the knowledge base with semantic search.

    Args:
        args: Command-line arguments

    Returns:
        Exit code
    """
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    try:
        qdrant_storage = QdrantStorage(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
        )

        neo4j_driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(
                os.getenv("NEO4J_USER", "neo4j"),
                os.getenv("NEO4J_PASSWORD", "password"),
            ),
        )

        engine = QueryEngine(
            qdrant_client=qdrant_storage.client,
            neo4j_driver=neo4j_driver,
            collection_name=qdrant_storage.read_collection,
        )

        # TASK 5.3: Determine expansion config based on CLI flags
        expansion_config = None

        if hasattr(args, "custom_expand") and args.custom_expand:
            # Parse custom JSON expansion config
            try:
                custom_dict = json.loads(args.custom_expand)
                expansion_config = ExpansionConfig(**custom_dict)
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON in --custom-expand: {e}")
                return 1
            except Exception as e:
                logging.error(f"Invalid expansion config: {e}")
                return 1
        elif hasattr(args, "expand") and args.expand:
            # Use preset expansion levels
            expand_level = args.expand.lower()
            if expand_level == "none":
                expansion_config = ExpansionConfig.none()
            elif expand_level == "minimal":
                expansion_config = ExpansionConfig.minimal()
            elif expand_level == "standard":
                expansion_config = ExpansionConfig.standard()
            elif expand_level == "comprehensive":
                expansion_config = ExpansionConfig.comprehensive()
            else:
                logging.error(f"Unknown expansion level: {expand_level}")
                return 1

        results = engine.hybrid_search(
            query_text=args.query,
            limit=args.limit,
            expansion_config=expansion_config,
            expand_context=True if expansion_config is None else None,
        )

        if not results:
            print("No results found.")
            return 0

        print(f"\nFound {len(results)} results:\n")

        for i, result in enumerate(results, 1):
            print(f"Result {i}:")
            print(f"  Score: {result['score']:.4f}")

            # TASK 4.2: Display formatted citation
            citation = engine.format_citation(result)
            print(f"  Citation: {citation}")

            if "hierarchy" in result:
                print(f"  Hierarchy: {result['hierarchy']}")

            # Show expansion metadata if present
            if "expanded_context" in result:
                meta = result["expanded_context"].get("expansion_metadata", {})
                before = meta.get("before_count", 0)
                after = meta.get("after_count", 0)
                print(f"  Expansion: {before} before, {after} after")

            if "section_context" in result:
                print(f"  Section context: included")

            if "referenced_content" in result:
                refs = result["referenced_content"].get("references", [])
                if refs:
                    counts = result["referenced_content"]["counts"]
                    print(f"  References: {counts['total']} total ({counts['figures']} figures, {counts['tables']} tables)")

            text = result.get("full_text") or result.get("text", "")
            if text:
                preview = text[:200] + "..." if len(text) > 200 else text
                print(f"  Text: {preview}")

            print()

        engine.close()
        return 0

    except Exception as e:
        logging.error(f"Error during search: {e}", exc_info=args.verbose)
        return 1


def cmd_get_hierarchy(args: argparse.Namespace) -> int:
    """Get the full hierarchy of a chapter.

    Args:
        args: Command-line arguments

    Returns:
        Exit code
    """
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    try:
        neo4j_driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(
                os.getenv("NEO4J_USER", "neo4j"),
                os.getenv("NEO4J_PASSWORD", "password"),
            ),
        )

        qdrant_storage = QdrantStorage(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
        )

        engine = QueryEngine(
            qdrant_client=qdrant_storage.client,
            neo4j_driver=neo4j_driver,
            collection_name=qdrant_storage.read_collection,
        )

        structure = engine.get_chapter_structure(args.chapter_id)

        if not structure:
            print(f"No chapter found with ID: {args.chapter_id}")
            return 1

        if args.json:
            print(json.dumps(structure, indent=2))
        else:
            print(f"\nChapter: {structure['chapter_title']}")
            print(f"Number: {structure['chapter_number']}")
            print(f"ID: {structure['chapter_id']}\n")

            sections = structure.get("sections", [])
            if sections:
                print("Sections:")
                for section in sections:
                    if section and section.get("title"):
                        print(f"  - {section.get('number', '?')}: {section['title']}")
            else:
                print("No sections found.")

        engine.close()
        return 0

    except Exception as e:
        logging.error(f"Error getting hierarchy: {e}", exc_info=args.verbose)
        return 1


def add_query_commands(subparsers):
    """Add query-related commands to CLI.

    Args:
        subparsers: Argparse subparsers object
    """
    parser_search = subparsers.add_parser(
        "search", help="Search the knowledge base with semantic search"
    )
    parser_search.add_argument("query", help="Search query")
    parser_search.add_argument(
        "-l", "--limit", type=int, default=5, help="Maximum number of results (default: 5)"
    )
    parser_search.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose output"
    )
    parser_search.add_argument(
        "--expand",
        choices=["none", "minimal", "standard", "comprehensive"],
        help="Expansion level: none (basic results), minimal (hierarchy only), "
             "standard (hierarchy + siblings), comprehensive (all features)",
    )
    parser_search.add_argument(
        "--custom-expand",
        type=str,
        help='Custom expansion config as JSON (e.g., \'{"expand_context": true, "include_references": true}\')',
    )
    parser_search.set_defaults(func=cmd_search)

    parser_hierarchy = subparsers.add_parser(
        "get-hierarchy", help="Get the full hierarchy structure of a chapter"
    )
    parser_hierarchy.add_argument(
        "chapter_id", help="Chapter ID (e.g., bailey:1, sabiston:5)"
    )
    parser_hierarchy.add_argument(
        "--json", action="store_true", help="Output as JSON"
    )
    parser_hierarchy.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose output"
    )
    parser_hierarchy.set_defaults(func=cmd_get_hierarchy)
