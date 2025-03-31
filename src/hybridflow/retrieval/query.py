"""Query engine for semantic search and context retrieval."""

import json
import logging
from typing import Dict, List, Optional, Union

from sentence_transformers import SentenceTransformer

from hybridflow.models import ExpansionConfig
from hybridflow.storage.neo4j_client import Neo4jStorage
from hybridflow.storage.qdrant_client import QdrantStorage

logger = logging.getLogger(__name__)


class QueryEngine:
    """Performs semantic search and context retrieval across hybrid storage."""

    def __init__(
        self,
        qdrant_storage: QdrantStorage,
        neo4j_storage: Neo4jStorage,
        embedding_model: str = "pritamdeka/S-PubMedBert-MS-MARCO-SCIFACT",
    ):
        """Initialize query engine.

        Args:
            qdrant_storage: QdrantStorage instance for vector search
            neo4j_storage: Neo4jStorage instance for graph traversal
            embedding_model: Embedding model name
        """
        self.qdrant_storage = qdrant_storage
        self.neo4j_storage = neo4j_storage
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

        search_results = self.qdrant_storage.client.query_points(
            collection_name=self.qdrant_storage.read_collection,
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
            chunk_id: Chunk identifier (non-versioned, e.g., 'bailey:ch60:2.1.1')

        Returns:
            Dictionary with paragraph text and hierarchy path
        """
        context = self.neo4j_storage.get_paragraph_context(chunk_id)

        if not context:
            logger.warning(f"No context found for chunk_id: {chunk_id}")
            return None

        return {
            "text": context["text"],
            "page": context["page"],
            "chapter_id": context["chapter_id"],
            "hierarchy": context["hierarchy"],
            # Detailed hierarchy components (TASK 2.1 enhancement)
            "chapter_title": context["hierarchy_parts"]["chapter"],
            "section_title": context["hierarchy_parts"]["section"],
            "subsection_title": context["hierarchy_parts"]["subsection"],
            "subsubsection_title": context["hierarchy_parts"]["subsubsection"],
        }

    def hybrid_search(
        self,
        query_text: str,
        limit: int = 5,
        expansion_config: Optional[Union[ExpansionConfig, dict]] = None,
        expand_context: bool = True,
        expand_paragraphs: bool = False,
        before_count: int = 2,
        after_count: int = 2,
        include_section_context: bool = False,
        include_references: bool = False,
    ) -> List[Dict]:
        """Perform hybrid search combining vector search with graph traversal.

        Args:
            query_text: Query string
            limit: Maximum number of results
            expansion_config: Optional ExpansionConfig object or dict to configure all expansion settings.
                            If provided, takes precedence over individual parameters.
            expand_context: Whether to expand results with full hierarchical context (deprecated, use expansion_config)
            expand_paragraphs: Whether to expand results with surrounding paragraphs (deprecated, use expansion_config)
            before_count: Number of paragraphs to retrieve before each result (deprecated, use expansion_config)
            after_count: Number of paragraphs to retrieve after each result (deprecated, use expansion_config)
            include_section_context: Whether to include parent section summary (deprecated, use expansion_config)
            include_references: Whether to include referenced figures/tables (deprecated, use expansion_config)

        Returns:
            List of results with text, score, and optional hierarchical context, surrounding paragraphs, and references
        """
        # TASK 5.2: Support ExpansionConfig while maintaining backward compatibility
        if expansion_config is not None:
            # If dict provided, convert to ExpansionConfig
            if isinstance(expansion_config, dict):
                config = ExpansionConfig(**expansion_config)
            else:
                config = expansion_config

            # Extract parameters from config
            expand_context = config.expand_context
            expand_paragraphs = config.expand_paragraphs
            before_count = config.before_count
            after_count = config.after_count
            include_section_context = config.include_section_context
            include_references = config.include_references

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
                # Supports both versioned and non-versioned data
                query = """
                MATCH (p:Paragraph)
                WHERE p.chunk_id = $chunk_id OR p.original_chunk_id = $chunk_id
                MATCH (parent)-[:HAS_PARAGRAPH]->(p)
                WHERE parent:Section OR parent:Subsection OR parent:Subsubsection
                RETURN parent.id as parent_id, parent.title as parent_title
                """

                with self.neo4j_storage.driver.session() as session:
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

        # TASK 3.3: Add cross-referenced figures/tables
        if include_references:
            for result in search_results:
                chunk_id = result["chunk_id"]
                referenced_content = self.get_referenced_content(chunk_id)

                # Always add referenced_content field for consistency,
                # even if there are no references (empty list)
                if referenced_content:
                    result["referenced_content"] = referenced_content

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

        with self.neo4j_storage.driver.session() as session:
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
        # Query supports both versioned and non-versioned section IDs
        query = """
        MATCH (section)
        WHERE (section.id = $section_id OR section.original_id = $section_id)
          AND (section:Section OR section:Subsection OR section:Subsubsection)

        // Get first paragraph (ordered by paragraph number)
        MATCH (section)-[:HAS_PARAGRAPH]->(p:Paragraph)
        WITH section, p
        ORDER BY p.number
        LIMIT 1

        // Get chapter
        OPTIONAL MATCH (c:Chapter)-[:HAS_SECTION]->(s:Section)
        WHERE section = s
           OR (s)-[:HAS_SUBSECTION]->(section)
           OR (s)-[:HAS_SUBSECTION]->()-[:HAS_SUBSUBSECTION]->(section)

        // Get subsection if applicable
        OPTIONAL MATCH (s)-[:HAS_SUBSECTION]->(ss:Subsection)
        WHERE section = ss OR (ss)-[:HAS_SUBSUBSECTION]->(section)

        // Get subsubsection if applicable
        OPTIONAL MATCH (ss)-[:HAS_SUBSUBSECTION]->(sss:Subsubsection)
        WHERE section = sss

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
        LIMIT 1
        """

        with self.neo4j_storage.driver.session() as session:
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
            # Supports both versioned and non-versioned data
            query = """
            MATCH (current:Paragraph)
            WHERE current.chunk_id = $chunk_id OR current.original_chunk_id = $chunk_id

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
            # Supports both versioned and non-versioned data
            query = """
            MATCH (current:Paragraph)
            WHERE current.chunk_id = $chunk_id OR current.original_chunk_id = $chunk_id

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

        with self.neo4j_storage.driver.session() as session:
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
        # Query supports both versioned and non-versioned data
        # Uses explicit OPTIONAL MATCH for each step to avoid issues with
        # variable-length path matching in Neo4j
        query = """
        MATCH (current:Paragraph)
        WHERE current.chunk_id = $chunk_id OR current.original_chunk_id = $chunk_id

        // Get paragraphs before by following PREV relationships
        OPTIONAL MATCH (current)-[:PREV]->(b1:Paragraph)
        OPTIONAL MATCH (b1)-[:PREV]->(b2:Paragraph)
        WITH current,
             CASE WHEN b2 IS NOT NULL THEN [b2, b1]
                  WHEN b1 IS NOT NULL THEN [b1]
                  ELSE [] END as before_paragraphs

        // Get paragraphs after by following NEXT relationships
        OPTIONAL MATCH (current)-[:NEXT]->(a1:Paragraph)
        OPTIONAL MATCH (a1)-[:NEXT]->(a2:Paragraph)
        WITH current, before_paragraphs,
             CASE WHEN a2 IS NOT NULL THEN [a1, a2]
                  WHEN a1 IS NOT NULL THEN [a1]
                  ELSE [] END as after_paragraphs

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

        with self.neo4j_storage.driver.session() as session:
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

            # Get before paragraphs and slice to requested count
            before_list = [
                {**p, "position": "before"} for p in reversed(record["before"]) if p
            ]
            before_list = before_list[:before_count] if before_count > 0 else []

            # Get after paragraphs and slice to requested count
            after_list = [{**p, "position": "after"} for p in record["after"] if p]
            after_list = after_list[:after_count] if after_count > 0 else []

            return {
                "current": {
                    "chunk_id": record["current_chunk_id"],
                    "number": record["current_number"],
                    "text": record["current_text"],
                    "page": record["current_page"],
                    "position": "current",
                },
                "before": before_list,
                "after": after_list,
                "hierarchy": " > ".join(hierarchy_parts),
                "chapter_id": record["chapter_id"],
                "parent_section": record["parent_title"],
                "all_siblings": all_siblings,
                "metadata": {
                    "requested_before": before_count,
                    "requested_after": after_count,
                    "returned_before": len(before_list),
                    "returned_after": len(after_list),
                },
            }

    def get_referenced_content(self, chunk_id: str) -> Optional[Dict]:
        """Retrieve figures and tables referenced by a paragraph.

        Reads the paragraph's pre-computed cross_references property and fetches
        the complete Figure/Table entities including file paths for efficient
        retrieval without re-parsing text.

        Args:
            chunk_id: Paragraph chunk identifier

        Returns:
            Dictionary with:
                - chunk_id: The paragraph chunk ID
                - references: List of figure/table dictionaries with complete data
                - counts: Summary of reference types found

        Examples:
            >>> result = engine.get_referenced_content("bailey:ch60:1.2.1")
            >>> result["references"]
            [
                {
                    "type": "figure",
                    "number": "60.1",
                    "caption": "...",
                    "file_png": "figures/fileoutpart0.png",
                    ...
                }
            ]
        """
        # First get the paragraph's cross_references property
        # Supports both versioned and non-versioned data
        query = """
        MATCH (p:Paragraph)
        WHERE p.chunk_id = $chunk_id OR p.original_chunk_id = $chunk_id
        RETURN p.cross_references as cross_references
        """

        with self.neo4j_storage.driver.session() as session:
            result = session.run(query, chunk_id=chunk_id)
            record = result.single()

            if not record or not record["cross_references"]:
                return {
                    "chunk_id": chunk_id,
                    "references": [],
                    "counts": {"figures": 0, "tables": 0},
                }

            # Deserialize cross_references from JSON string
            cross_references_json = record["cross_references"]
            cross_references = json.loads(cross_references_json) if cross_references_json else []

            # Fetch actual Figure/Table entities for each reference
            references = []
            figure_count = 0
            table_count = 0

            for ref in cross_references:
                ref_type = ref["type"]
                ref_number = ref["number"]

                if ref_type == "figure":
                    # Look up Figure node by figure_number
                    fig_query = """
                    MATCH (f:Figure {figure_number: $figure_number})
                    RETURN f.figure_number as number,
                           f.caption as caption,
                           f.file_png as file_png,
                           f.page as page,
                           f.bounds as bounds,
                           f.paragraph_id as paragraph_id
                    LIMIT 1
                    """
                    fig_result = session.run(fig_query, figure_number=ref_number)
                    fig_record = fig_result.single()

                    if fig_record:
                        references.append({
                            "type": "figure",
                            "number": fig_record["number"],
                            "caption": fig_record["caption"],
                            "file_png": fig_record["file_png"],
                            "page": fig_record["page"],
                            "bounds": fig_record["bounds"],
                            "source_paragraph_id": fig_record["paragraph_id"],
                        })
                        figure_count += 1
                    else:
                        # Reference found but entity missing (edge case)
                        references.append({
                            "type": "figure",
                            "number": ref_number,
                            "error": "Figure entity not found in database",
                        })

                elif ref_type == "table":
                    # Look up Table node by table_number
                    table_query = """
                    MATCH (t:Table {table_number: $table_number})
                    RETURN t.table_number as number,
                           t.description as description,
                           t.file_png as file_png,
                           t.file_xlsx as file_xlsx,
                           t.page as page,
                           t.bounds as bounds,
                           t.paragraph_id as paragraph_id
                    LIMIT 1
                    """
                    table_result = session.run(table_query, table_number=ref_number)
                    table_record = table_result.single()

                    if table_record:
                        references.append({
                            "type": "table",
                            "number": table_record["number"],
                            "description": table_record["description"],
                            "file_png": table_record["file_png"],
                            "file_xlsx": table_record["file_xlsx"],
                            "page": table_record["page"],
                            "bounds": table_record["bounds"],
                            "source_paragraph_id": table_record["paragraph_id"],
                        })
                        table_count += 1
                    else:
                        # Reference found but entity missing (edge case)
                        references.append({
                            "type": "table",
                            "number": ref_number,
                            "error": "Table entity not found in database",
                        })

            return {
                "chunk_id": chunk_id,
                "references": references,
                "counts": {
                    "figures": figure_count,
                    "tables": table_count,
                    "total": figure_count + table_count,
                },
            }

    def format_citation(self, result: Dict) -> str:
        """Format a search result as a citation.

        Args:
            result: Search result dictionary

        Returns:
            Formatted citation string (e.g., "Bailey & Love, Ch 60, Section 2.4, p. 1025")
        """
        # Map textbook IDs to proper names
        textbook_names = {
            "bailey": "Bailey & Love",
            "sabiston": "Sabiston",
            "schwartz": "Schwartz",
        }

        parts = []

        # Add textbook name
        textbook_id = result.get("textbook_id", "")
        if textbook_id:
            textbook_name = textbook_names.get(textbook_id, textbook_id.title())
            parts.append(textbook_name)

        # Add chapter number
        chapter_number = result.get("chapter_number")
        if chapter_number:
            parts.append(f"Ch {chapter_number}")

        # Add section number from chunk_id if available
        chunk_id = result.get("chunk_id", "")
        if chunk_id and ":" in chunk_id:
            # Extract paragraph number which encodes the hierarchy
            # Hierarchy format: section.subsection.subsubsection.paragraph
            # Examples:
            #   "1.3" = Section 1, Subsection 3 → show "Section 1.3"
            #   "1.3.8" = Section 1, Subsection 3, Para 8 → show "Section 1.3"
            #   "2.4.4.2" = Section 2, Subsec 4, Subsub 4, Para 2 → show "Section 2.4.4"
            id_parts = chunk_id.split(":")
            if len(id_parts) >= 3:
                paragraph_num = id_parts[2]
                # Handle versioned chunk_ids by stripping version suffix
                if "::" in paragraph_num:
                    paragraph_num = paragraph_num.split("::")[0]
                if "." in paragraph_num:
                    section_parts = paragraph_num.split(".")
                    num_parts = len(section_parts)
                    if num_parts == 2:
                        # Subsection level: show both parts (e.g., "1.3")
                        section_num = f"{section_parts[0]}.{section_parts[1]}"
                    elif num_parts >= 3:
                        # Deeper hierarchy: show all but last (paragraph) number
                        section_num = ".".join(section_parts[:-1])
                    else:
                        section_num = section_parts[0]
                    parts.append(f"Section {section_num}")
                else:
                    # Single number at section level
                    parts.append(f"Section {paragraph_num}")

        # Add page number
        page = result.get("page")
        if page:
            parts.append(f"p. {page}")

        return ", ".join(parts) if parts else "Unknown source"

    def close(self):
        """Close connections."""
        if self.neo4j_storage:
            self.neo4j_storage.close()
