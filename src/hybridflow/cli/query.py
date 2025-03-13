"""CLI commands for querying and searching the knowledge base."""

import argparse
import json
import logging
import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

from hybridflow.retrieval.query import QueryEngine

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
        qdrant_client = QdrantClient(
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
            qdrant_client=qdrant_client,
            neo4j_driver=neo4j_driver,
        )

        results = engine.hybrid_search(
            query_text=args.query,
            limit=args.limit,
            expand_context=True,
        )

        if not results:
            print("No results found.")
            return 0

        print(f"\nFound {len(results)} results:\n")

        for i, result in enumerate(results, 1):
            print(f"Result {i}:")
            print(f"  Score: {result['score']:.4f}")
            print(f"  Textbook: {result.get('textbook_id', 'N/A')}")
            print(f"  Chapter: {result.get('chapter_number', 'N/A')}")
            print(f"  Page: {result.get('page', 'N/A')}")

            if "hierarchy" in result:
                print(f"  Hierarchy: {result['hierarchy']}")

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

        qdrant_client = QdrantClient(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
        )

        engine = QueryEngine(
            qdrant_client=qdrant_client,
            neo4j_driver=neo4j_driver,
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
