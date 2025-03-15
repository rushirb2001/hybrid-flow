"""Query engine for semantic search and context retrieval."""

import logging
from typing import Dict, List, Optional

from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class QueryEngine:
    """Performs semantic search and context retrieval across hybrid storage."""

    def __init__(
        self,
        qdrant_client: QdrantClient,
        neo4j_driver,
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
        collection_name: str = "textbook_chunks",
    ):
        """Initialize query engine.

        Args:
            qdrant_client: Qdrant client for vector search
            neo4j_driver: Neo4j driver for graph traversal
            embedding_model: Embedding model name
            collection_name: Qdrant collection name
        """
        self.qdrant = qdrant_client
        self.neo4j = neo4j_driver
        self.collection_name = collection_name
        self.encoder = SentenceTransformer(embedding_model)

    def semantic_search(
        self, query_text: str, limit: int = 5, score_threshold: float = 0.5
    ) -> List[Dict]:
        """Perform semantic search using vector similarity.

        Args:
            query_text: Query string
            limit: Maximum number of results
            score_threshold: Minimum similarity score

        Returns:
            List of results with chunk_id, score, and metadata
        """
        query_vector = self.encoder.encode(query_text).tolist()

        search_results = self.qdrant.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=limit,
            score_threshold=score_threshold,
        ).points

        results = []
        for hit in search_results:
            results.append(
                {
                    "chunk_id": hit.payload.get("chunk_id"),
                    "score": hit.score,
                    "text": hit.payload.get("text", ""),
                    "textbook_id": hit.payload.get("textbook_id"),
                    "chapter_number": hit.payload.get("chapter_number"),
                    "page": hit.payload.get("page"),
                }
            )

        logger.info(f"Semantic search returned {len(results)} results for: {query_text}")
        return results

    def get_context(self, chunk_id: str) -> Optional[Dict]:
        """Retrieve full context for a chunk including hierarchy.

        Args:
            chunk_id: Chunk identifier

        Returns:
            Dictionary with paragraph text and hierarchy path
        """
        query = """
        MATCH (p:Paragraph {chunk_id: $chunk_id})
        OPTIONAL MATCH path = (c:Chapter)-[:HAS_SECTION]->(s:Section)
                      -[:HAS_SUBSECTION*0..1]->(ss)
                      -[:HAS_SUBSUBSECTION*0..1]->(sss)
                      -[:HAS_PARAGRAPH]->(p)
        RETURN p.text as text,
               p.page as page,
               c.id as chapter_id,
               c.title as chapter_title,
               s.title as section_title,
               ss.title as subsection_title,
               sss.title as subsubsection_title
        """

        with self.neo4j.session() as session:
            result = session.run(query, chunk_id=chunk_id)
            record = result.single()

            if not record:
                logger.warning(f"No context found for chunk_id: {chunk_id}")
                return None

            hierarchy_parts = []
            if record["chapter_title"]:
                hierarchy_parts.append(record["chapter_title"])
            if record["section_title"]:
                hierarchy_parts.append(record["section_title"])
            if record["subsection_title"]:
                hierarchy_parts.append(record["subsection_title"])
            if record["subsubsection_title"]:
                hierarchy_parts.append(record["subsubsection_title"])

            return {
                "text": record["text"],
                "page": record["page"],
                "chapter_id": record["chapter_id"],
                "hierarchy": " > ".join(hierarchy_parts),
                # Detailed hierarchy components (TASK 2.1 enhancement)
                "chapter_title": record["chapter_title"],
                "section_title": record["section_title"],
                "subsection_title": record["subsection_title"],
                "subsubsection_title": record["subsubsection_title"],
            }

    def hybrid_search(
        self,
        query_text: str,
        limit: int = 5,
        expand_context: bool = True,
        expand_paragraphs: bool = False,
        before_count: int = 2,
        after_count: int = 2,
        include_section_context: bool = False,
    ) -> List[Dict]:
        """Perform hybrid search combining vector search with graph traversal.

        Args:
            query_text: Query string
            limit: Maximum number of results
            expand_context: Whether to expand results with full hierarchical context
            expand_paragraphs: Whether to expand results with surrounding paragraphs
            before_count: Number of paragraphs to retrieve before each result (if expand_paragraphs=True)
            after_count: Number of paragraphs to retrieve after each result (if expand_paragraphs=True)
            include_section_context: Whether to include parent section summary (TASK 2.3)

        Returns:
            List of results with text, score, and optional hierarchical context and surrounding paragraphs
        """
        search_results = self.semantic_search(query_text, limit=limit)

        if expand_context:
            for result in search_results:
                chunk_id = result["chunk_id"]
                context = self.get_context(chunk_id)
                if context:
                    result["full_text"] = context["text"]
                    result["hierarchy"] = context["hierarchy"]
                    result["chapter_id"] = context["chapter_id"]
                    # Store detailed hierarchy for section context retrieval (TASK 2.1)
                    result["hierarchy_details"] = {
                        "chapter_title": context["chapter_title"],
                        "section_title": context["section_title"],
                        "subsection_title": context["subsection_title"],
                        "subsubsection_title": context["subsubsection_title"],
                    }

        if expand_paragraphs:
            for result in search_results:
                chunk_id = result["chunk_id"]
                expanded = self.get_surrounding_paragraphs(
                    chunk_id, before_count=before_count, after_count=after_count
                )
                if expanded:
                    result["expanded_context"] = {
                        "before_paragraphs": expanded["before"],
                        "current_paragraph": expanded["current"],
                        "after_paragraphs": expanded["after"],
                        "parent_section": expanded["parent_section"],
                        "all_siblings": expanded["all_siblings"],
                        "expansion_metadata": expanded["metadata"],
                    }
                    # If hierarchy wasn't already added, add it from expansion
                    if "hierarchy" not in result and "hierarchy" in expanded:
                        result["hierarchy"] = expanded["hierarchy"]
                    if "chapter_id" not in result and "chapter_id" in expanded:
                        result["chapter_id"] = expanded["chapter_id"]

        # TASK 2.3: Add section context with summary
        if include_section_context:
            for result in search_results:
                chunk_id = result["chunk_id"]

                # Query to find parent section ID
                query = """
                MATCH (p:Paragraph {chunk_id: $chunk_id})
                MATCH (parent)-[:HAS_PARAGRAPH]->(p)
                WHERE parent:Section OR parent:Subsection OR parent:Subsubsection
                RETURN parent.id as parent_id, parent.title as parent_title
                """

                with self.neo4j.session() as session:
                    neo4j_result = session.run(query, chunk_id=chunk_id)
                    record = neo4j_result.single()

                    if record:
                        parent_id = record["parent_id"]
                        parent_title = record["parent_title"]

                        # Get section summary (first paragraph)
                        summary = self.get_section_summary(parent_id)

                        if summary:
                            # Check if it's an error response (empty section)
                            if "error" in summary:
                                # Section was empty, store error info
                                result["section_context"] = {
                                    "parent_id": parent_id,
                                    "parent_title": parent_title,
                                    "error": summary["error"],
                                    "message": summary["message"],
                                }
                            else:
                                # Valid section with content
                                result["section_context"] = {
                                    "parent_id": parent_id,
                                    "parent_title": parent_title,
                                    "summary_paragraph": {
                                        "chunk_id": summary["chunk_id"],
                                        "number": summary["number"],
                                        "text": summary["text"],
                                        "page": summary["page"],
                                    },
                                    "hierarchy": summary["hierarchy"],
                                }

                                # If hierarchy wasn't already added, add it from section context
                                if "hierarchy" not in result:
                                    result["hierarchy"] = summary["hierarchy"]
                                if "chapter_id" not in result:
                                    result["chapter_id"] = summary["chapter_id"]

        return search_results

    def get_chapter_structure(self, chapter_id: str) -> Optional[Dict]:
        """Retrieve complete chapter structure from Neo4j.

        Args:
            chapter_id: Chapter identifier

        Returns:
            Dictionary with complete chapter hierarchy
        """
        query = """
        MATCH (c:Chapter {id: $chapter_id})
        OPTIONAL MATCH (c)-[:HAS_SECTION]->(s:Section)
        RETURN c.title as chapter_title,
               c.number as chapter_number,
               collect(DISTINCT {
                   title: s.title,
                   number: s.number
               }) as sections
        """

        with self.neo4j.session() as session:
            result = session.run(query, chapter_id=chapter_id)
            record = result.single()

            if not record:
                logger.warning(f"No chapter found with id: {chapter_id}")
                return None

            return {
                "chapter_id": chapter_id,
                "chapter_title": record["chapter_title"],
                "chapter_number": record["chapter_number"],
                "sections": record["sections"],
            }

    def get_section_summary(self, section_id: str) -> Optional[Dict]:
        """Retrieve the first paragraph of a section as a summary.

        Args:
            section_id: Section, subsection, or subsubsection identifier

        Returns:
            Dictionary with first paragraph and section metadata, or None if section not found
        """
        query = """
        MATCH (section)
        WHERE section.id = $section_id
          AND (section:Section OR section:Subsection OR section:Subsubsection)

        // Get first paragraph (ordered by paragraph number)
        MATCH (section)-[:HAS_PARAGRAPH]->(p:Paragraph)
        WITH section, p
        ORDER BY p.number
        LIMIT 1

        // Get full hierarchy context
        OPTIONAL MATCH (c:Chapter)-[:HAS_SECTION]->(s:Section)
                      -[:HAS_SUBSECTION*0..1]->(ss)
                      -[:HAS_SUBSUBSECTION*0..1]->(sss)
        WHERE section = s OR section = ss OR section = sss

        RETURN p.chunk_id as chunk_id,
               p.number as number,
               p.text as text,
               p.page as page,
               section.title as section_title,
               section.id as section_id,
               c.id as chapter_id,
               c.title as chapter_title,
               s.title as section_title_level1,
               ss.title as subsection_title,
               sss.title as subsubsection_title
        """

        with self.neo4j.session() as session:
            result = session.run(query, section_id=section_id)
            record = result.single()

            if not record:
                logger.warning(f"No section found with id: {section_id}")
                # Return informative error message for missing sections
                # Sections may be missing because they were empty (no paragraphs/subsections)
                # during ingestion and were intentionally skipped
                return {
                    "error": "section_not_found",
                    "section_id": section_id,
                    "message": (
                        f"Section '{section_id}' not found in Neo4j. "
                        "This section may have been empty (no paragraphs or subsections) "
                        "and was intentionally excluded during ingestion."
                    ),
                }

            # Build hierarchy path
            hierarchy_parts = []
            if record["chapter_title"]:
                hierarchy_parts.append(record["chapter_title"])
            if record["section_title_level1"]:
                hierarchy_parts.append(record["section_title_level1"])
            if record["subsection_title"]:
                hierarchy_parts.append(record["subsection_title"])
            if record["subsubsection_title"]:
                hierarchy_parts.append(record["subsubsection_title"])

            return {
                "chunk_id": record["chunk_id"],
                "number": record["number"],
                "text": record["text"],
                "page": record["page"],
                "section_title": record["section_title"],
                "section_id": record["section_id"],
                "chapter_id": record["chapter_id"],
                "hierarchy": " > ".join(hierarchy_parts),
            }

    def get_sibling_paragraphs(
        self, chunk_id: str, same_level_only: bool = True
    ) -> Optional[Dict]:
        """Retrieve all sibling paragraphs at the same or section level.

        Args:
            chunk_id: Target paragraph chunk identifier
            same_level_only: If True, get siblings from immediate parent only.
                           If False, get all paragraphs from parent section level.

        Returns:
            Dictionary with siblings list, parent info, and hierarchy context
        """
        if same_level_only:
            # Get siblings from immediate parent (section/subsection/subsubsection)
            query = """
            MATCH (current:Paragraph {chunk_id: $chunk_id})

            // Find immediate parent
            MATCH (parent)-[:HAS_PARAGRAPH]->(current)
            WHERE parent:Section OR parent:Subsection OR parent:Subsubsection

            // Get all siblings from same parent
            MATCH (parent)-[:HAS_PARAGRAPH]->(sibling:Paragraph)

            // Get hierarchy context
            OPTIONAL MATCH (c:Chapter)-[:HAS_SECTION]->(s:Section)
                          -[:HAS_SUBSECTION*0..1]->(ss)
                          -[:HAS_SUBSUBSECTION*0..1]->(sss)
            WHERE parent = s OR parent = ss OR parent = sss

            WITH current, parent, sibling,
                 c.id as chapter_id,
                 c.title as chapter_title,
                 s.title as section_title,
                 ss.title as subsection_title,
                 sss.title as subsubsection_title,
                 parent.title as parent_title,
                 parent.id as parent_id
            ORDER BY sibling.number

            RETURN current.chunk_id as current_chunk_id,
                   chapter_id,
                   chapter_title,
                   section_title,
                   subsection_title,
                   subsubsection_title,
                   parent_title,
                   parent_id,
                   collect({
                       chunk_id: sibling.chunk_id,
                       number: sibling.number,
                       text: sibling.text,
                       page: sibling.page,
                       is_current: sibling.chunk_id = current.chunk_id
                   }) as siblings
            """
        else:
            # Get all siblings from parent section level
            query = """
            MATCH (current:Paragraph {chunk_id: $chunk_id})

            // Find top-level section
            MATCH (c:Chapter)-[:HAS_SECTION]->(section:Section)
                  -[:HAS_SUBSECTION*0..1]->(ss)
                  -[:HAS_SUBSUBSECTION*0..1]->(sss)
                  -[:HAS_PARAGRAPH]->(current)

            // Get all paragraphs under that section (at any level)
            MATCH (section)-[:HAS_SUBSECTION*0..1]->(any_ss)
                  -[:HAS_SUBSUBSECTION*0..1]->(any_sss)
                  -[:HAS_PARAGRAPH]->(sibling:Paragraph)

            WITH current, section, c, sibling
            ORDER BY sibling.number

            RETURN current.chunk_id as current_chunk_id,
                   c.id as chapter_id,
                   c.title as chapter_title,
                   section.title as section_title,
                   section.id as parent_id,
                   section.title as parent_title,
                   null as subsection_title,
                   null as subsubsection_title,
                   collect({
                       chunk_id: sibling.chunk_id,
                       number: sibling.number,
                       text: sibling.text,
                       page: sibling.page,
                       is_current: sibling.chunk_id = current.chunk_id
                   }) as siblings
            """

        with self.neo4j.session() as session:
            result = session.run(query, chunk_id=chunk_id)
            record = result.single()

            if not record:
                logger.warning(f"No paragraph found with chunk_id: {chunk_id}")
                return None

            # Build hierarchy path
            hierarchy_parts = []
            if record["chapter_title"]:
                hierarchy_parts.append(record["chapter_title"])
            if record["section_title"]:
                hierarchy_parts.append(record["section_title"])
            if record["subsection_title"]:
                hierarchy_parts.append(record["subsection_title"])
            if record["subsubsection_title"]:
                hierarchy_parts.append(record["subsubsection_title"])

            return {
                "current_chunk_id": record["current_chunk_id"],
                "siblings": record["siblings"],
                "parent_id": record["parent_id"],
                "parent_title": record["parent_title"],
                "hierarchy": " > ".join(hierarchy_parts),
                "chapter_id": record["chapter_id"],
                "same_level_only": same_level_only,
                "total_siblings": len(record["siblings"]),
            }

    def get_surrounding_paragraphs(
        self, chunk_id: str, before_count: int = 2, after_count: int = 2
    ) -> Optional[Dict]:
        """Retrieve surrounding paragraphs using NEXT/PREV relationships.

        Args:
            chunk_id: Target paragraph chunk identifier
            before_count: Number of paragraphs to retrieve before target
            after_count: Number of paragraphs to retrieve after target

        Returns:
            Dictionary with before, current, and after paragraphs, including
            all metadata and hierarchy information
        """
        query = """
        MATCH (current:Paragraph {chunk_id: $chunk_id})

        // Get paragraphs before (using PREV relationships)
        OPTIONAL MATCH path_before = (current)-[:PREV*1..]->(before:Paragraph)
        WITH current, before, path_before
        ORDER BY length(path_before) ASC
        WITH current, collect(before)[0..$before_count] as before_paragraphs

        // Get paragraphs after (using NEXT relationships)
        OPTIONAL MATCH path_after = (current)-[:NEXT*1..]->(after:Paragraph)
        WITH current, before_paragraphs, after, path_after
        ORDER BY length(path_after) ASC
        WITH current, before_paragraphs, collect(after)[0..$after_count] as after_paragraphs

        // Get hierarchy context for current paragraph
        OPTIONAL MATCH (c:Chapter)-[:HAS_SECTION]->(s:Section)
                      -[:HAS_SUBSECTION*0..1]->(ss)
                      -[:HAS_SUBSUBSECTION*0..1]->(sss)
                      -[:HAS_PARAGRAPH]->(current)

        // Get parent node (section, subsection, or subsubsection)
        OPTIONAL MATCH (parent)-[:HAS_PARAGRAPH]->(current)
        WHERE parent:Section OR parent:Subsection OR parent:Subsubsection

        // Get all paragraphs in the same parent for full context
        OPTIONAL MATCH (parent)-[:HAS_PARAGRAPH]->(sibling:Paragraph)
        WITH current, before_paragraphs, after_paragraphs,
             c.title as chapter_title, c.id as chapter_id,
             s.title as section_title,
             ss.title as subsection_title,
             sss.title as subsubsection_title,
             parent.title as parent_title,
             collect(DISTINCT {
                 chunk_id: sibling.chunk_id,
                 number: sibling.number,
                 text: sibling.text,
                 page: sibling.page
             }) as all_siblings

        RETURN current.chunk_id as current_chunk_id,
               current.number as current_number,
               current.text as current_text,
               current.page as current_page,
               chapter_id,
               chapter_title,
               section_title,
               subsection_title,
               subsubsection_title,
               parent_title,
               [p IN before_paragraphs | {
                   chunk_id: p.chunk_id,
                   number: p.number,
                   text: p.text,
                   page: p.page
               }] as before,
               [p IN after_paragraphs | {
                   chunk_id: p.chunk_id,
                   number: p.number,
                   text: p.text,
                   page: p.page
               }] as after,
               all_siblings
        """

        with self.neo4j.session() as session:
            result = session.run(
                query, chunk_id=chunk_id, before_count=before_count, after_count=after_count
            )
            record = result.single()

            if not record:
                logger.warning(f"No paragraph found with chunk_id: {chunk_id}")
                return None

            # Build hierarchy path
            hierarchy_parts = []
            if record["chapter_title"]:
                hierarchy_parts.append(record["chapter_title"])
            if record["section_title"]:
                hierarchy_parts.append(record["section_title"])
            if record["subsection_title"]:
                hierarchy_parts.append(record["subsection_title"])
            if record["subsubsection_title"]:
                hierarchy_parts.append(record["subsubsection_title"])

            # Sort all siblings by paragraph number for proper ordering
            all_siblings = record["all_siblings"]
            if all_siblings:
                all_siblings = sorted(all_siblings, key=lambda x: x["number"])

            return {
                "current": {
                    "chunk_id": record["current_chunk_id"],
                    "number": record["current_number"],
                    "text": record["current_text"],
                    "page": record["current_page"],
                    "position": "current",
                },
                "before": [
                    {**p, "position": "before"} for p in reversed(record["before"]) if p
                ],
                "after": [{**p, "position": "after"} for p in record["after"] if p],
                "hierarchy": " > ".join(hierarchy_parts),
                "chapter_id": record["chapter_id"],
                "parent_section": record["parent_title"],
                "all_siblings": all_siblings,
                "metadata": {
                    "requested_before": before_count,
                    "requested_after": after_count,
                    "returned_before": len([p for p in record["before"] if p]),
                    "returned_after": len([p for p in record["after"] if p]),
                },
            }

    def close(self):
        """Close connections."""
        if self.neo4j:
            self.neo4j.close()
