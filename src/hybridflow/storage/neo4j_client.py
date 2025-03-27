"""Neo4j graph database client for hierarchical data storage."""

import json
from typing import Any, Dict, List, Optional, Set

from neo4j import GraphDatabase


class Neo4jStorage:
    """Manages graph storage for hierarchical textbook data using Neo4j."""

    def __init__(self, uri: str, user: str, password: str):
        """Initialize the Neo4j driver.

        Args:
            uri: Neo4j connection URI (e.g., bolt://localhost:7687)
            user: Database username
            password: Database password
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def _get_version_labels(self, version_id: Optional[str] = None) -> str:
        """Get version labels string for Cypher queries.

        This helper method generates Neo4j label syntax for version-aware queries.
        Version labels enable multiple versions of the graph to coexist using
        Neo4j's multi-label capability.

        **Neo4j Community Edition Compatibility:**
        This label-based versioning strategy works with Neo4j Community Edition.
        Multi-database versioning requires Neo4j Enterprise Edition, but label-based
        versioning provides similar isolation capabilities using only labels, making
        it compatible with all Neo4j editions.

        Args:
            version_id: Optional version identifier (e.g., 'v2_test', 'v1_baseline')
                       If None, returns empty string (no version filtering)

        Returns:
            str: Version label string in format ':version_id' or empty string

        Examples:
            >>> storage._get_version_labels(None)
            ''
            >>> storage._get_version_labels('v1_baseline')
            ':v1_baseline'
            >>> storage._get_version_labels('v2_test')
            ':v2_test'
        """
        if version_id is None:
            return ""
        return f":{version_id}"

    def _versioned_id(self, base_id: str, version_id: Optional[str] = None) -> str:
        """Generate versioned ID by appending version suffix.

        Converts a base ID into a versioned ID to avoid constraint conflicts.
        Instead of using labels for versioning (which conflicts with unique constraints),
        we append the version_id as a suffix to create distinct IDs.

        Args:
            base_id: Original ID (e.g., 'bailey', 'bailey:ch60')
            version_id: Optional version identifier (e.g., 'v2_test', 'staging_1')
                       If None, returns base_id unchanged

        Returns:
            str: Versioned ID in format 'base_id::version_id' or base_id if no version

        Examples:
            >>> storage._versioned_id('bailey', None)
            'bailey'
            >>> storage._versioned_id('bailey', 'v2_test')
            'bailey::v2_test'
            >>> storage._versioned_id('bailey:ch60', 'staging_1')
            'bailey:ch60::staging_1'
        """
        if version_id is None:
            return base_id
        return f"{base_id}::{version_id}"

    def _build_node_pattern(
        self, node_type: str, version_id: Optional[str] = None, properties: str = ""
    ) -> str:
        """Build Cypher node pattern with optional version labels and properties.

        Constructs a Cypher node pattern string combining node type, version labels,
        and property constraints. Used to generate version-aware Cypher queries
        dynamically.

        Args:
            node_type: Neo4j node label (e.g., 'Paragraph', 'Chapter', 'Section')
            version_id: Optional version identifier for multi-version support
            properties: Optional Cypher property map string (e.g., '{chunk_id: $chunk_id}')
                       Should be empty string or valid Cypher property syntax

        Returns:
            str: Complete Cypher node pattern string

        Examples:
            >>> storage._build_node_pattern('Paragraph', None, '{chunk_id: $chunk_id}')
            '(:Paragraph {chunk_id: $chunk_id})'
            >>> storage._build_node_pattern('Paragraph', 'v2_test', '{chunk_id: $chunk_id}')
            '(:Paragraph:v2_test {chunk_id: $chunk_id})'
            >>> storage._build_node_pattern('Chapter', 'v1_baseline')
            '(:Chapter:v1_baseline )'
            >>> storage._build_node_pattern('Section')
            '(:Section )'
        """
        version_labels = self._get_version_labels(version_id)
        return f"(:{node_type}{version_labels} {properties})"

    def create_constraints(self) -> None:
        """Create uniqueness constraints and indexes for key node types.

        Creates indexes on Section, Subsection, and Subsubsection IDs
        for fast parent lookups during paragraph ingestion.
        """
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Textbook) REQUIRE t.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chapter) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paragraph) REQUIRE p.chunk_id IS UNIQUE",
        ]

        # Indexes for fast parent lookups during ingestion
        indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (s:Section) ON (s.id)",
            "CREATE INDEX IF NOT EXISTS FOR (ss:Subsection) ON (ss.id)",
            "CREATE INDEX IF NOT EXISTS FOR (sss:Subsubsection) ON (sss.id)",
        ]

        with self.driver.session() as session:
            for constraint in constraints:
                session.run(constraint)
            for index in indexes:
                session.run(index)

    def upsert_textbook(self, textbook_id: str, name: str, version_id: Optional[str] = None) -> None:
        """Insert or update a textbook node.

        Args:
            textbook_id: Unique textbook identifier
            name: Full name of the textbook
            version_id: Optional version identifier for multi-version support.
                       If provided, appends version to ID (e.g., 'bailey::v2_test').
                       If None, uses base ID unchanged (backward compatible).

        Examples:
            # Non-versioned (backward compatible)
            >>> storage.upsert_textbook('bailey', 'Bailey & Love')
            # Creates node with id='bailey'

            # Versioned
            >>> storage.upsert_textbook('bailey', 'Bailey & Love', version_id='v2_test')
            # Creates node with id='bailey::v2_test', original_id='bailey'
        """
        actual_id = self._versioned_id(textbook_id, version_id)
        query = """
        MERGE (t:Textbook {id: $actual_id})
        SET t.name = $name, t.original_id = $original_id
        """
        with self.driver.session() as session:
            session.run(query, actual_id=actual_id, name=name, original_id=textbook_id)

    def upsert_chapter(
        self, textbook_id: str, chapter_number: str, title: str, version: int, version_id: Optional[str] = None
    ) -> None:
        """Insert or update a chapter node and link to textbook.

        Args:
            textbook_id: Parent textbook identifier
            chapter_number: Chapter number
            title: Chapter title
            version: Version number
            version_id: Optional version identifier for multi-version support.
                       If provided, appends version to IDs for isolation.
                       If None, uses base IDs unchanged (backward compatible).

        Examples:
            # Non-versioned (backward compatible)
            >>> storage.upsert_chapter('bailey', '60', 'The Thorax', version=1)
            # Creates chapter with id='bailey:ch60'

            # Versioned
            >>> storage.upsert_chapter('bailey', '60', 'The Thorax', version=1, version_id='v2_test')
            # Creates chapter with id='bailey:ch60::v2_test', original_id='bailey:ch60'
        """
        chapter_id = f"{textbook_id}:ch{chapter_number}"
        actual_textbook_id = self._versioned_id(textbook_id, version_id)
        actual_chapter_id = self._versioned_id(chapter_id, version_id)
        query = """
        MERGE (t:Textbook {id: $actual_textbook_id})
        MERGE (c:Chapter {id: $actual_chapter_id})
        SET c.number = $chapter_number, c.title = $title, c.version = $version, c.original_id = $original_id
        MERGE (t)-[:CONTAINS]->(c)
        """
        with self.driver.session() as session:
            session.run(
                query,
                actual_textbook_id=actual_textbook_id,
                actual_chapter_id=actual_chapter_id,
                original_id=chapter_id,
                chapter_number=chapter_number,
                title=title,
                version=version,
            )

    def upsert_section(
        self, chapter_id: str, section_number: str, title: str, version_id: Optional[str] = None
    ) -> None:
        """Insert or update a section node and link to chapter.

        Args:
            chapter_id: Parent chapter identifier (base ID without version)
            section_number: Section number
            title: Section title
            version_id: Optional version identifier for multi-version support.
                       If provided, appends version to IDs for isolation.
                       If None, uses base IDs unchanged (backward compatible).

        Examples:
            # Non-versioned (backward compatible)
            >>> storage.upsert_section('bailey:ch60', '2', 'Anatomy')
            # Creates section with id='bailey:ch60:s2'

            # Versioned
            >>> storage.upsert_section('bailey:ch60', '2', 'Anatomy', version_id='v2_test')
            # Creates section with id='bailey:ch60:s2::v2_test', original_id='bailey:ch60:s2'
        """
        section_id = f"{chapter_id}:s{section_number}"
        actual_chapter_id = self._versioned_id(chapter_id, version_id)
        actual_section_id = self._versioned_id(section_id, version_id)
        query = """
        MERGE (c:Chapter {id: $actual_chapter_id})
        MERGE (s:Section {id: $actual_section_id})
        SET s.number = $section_number, s.title = $title, s.original_id = $original_id
        MERGE (c)-[:HAS_SECTION]->(s)
        """
        with self.driver.session() as session:
            session.run(
                query,
                actual_chapter_id=actual_chapter_id,
                actual_section_id=actual_section_id,
                original_id=section_id,
                section_number=section_number,
                title=title,
            )

    def upsert_subsection(
        self, section_id: str, subsection_number: str, title: str, version_id: Optional[str] = None
    ) -> None:
        """Insert or update a subsection node and link to section.

        Args:
            section_id: Parent section identifier (base ID without version)
            subsection_number: Subsection number
            title: Subsection title
            version_id: Optional version identifier for multi-version support.
                       If provided, appends version to IDs for isolation.
                       If None, uses base IDs unchanged (backward compatible).

        Examples:
            # Non-versioned (backward compatible)
            >>> storage.upsert_subsection('bailey:ch60:s2', '2.1', 'Lung Anatomy')
            # Creates subsection with id='bailey:ch60:s2:ss2.1'

            # Versioned
            >>> storage.upsert_subsection('bailey:ch60:s2', '2.1', 'Lung Anatomy', version_id='v2_test')
            # Creates subsection with id='bailey:ch60:s2:ss2.1::v2_test'
        """
        subsection_id = f"{section_id}:ss{subsection_number}"
        actual_section_id = self._versioned_id(section_id, version_id)
        actual_subsection_id = self._versioned_id(subsection_id, version_id)
        query = """
        MERGE (s:Section {id: $actual_section_id})
        MERGE (ss:Subsection {id: $actual_subsection_id})
        SET ss.number = $subsection_number, ss.title = $title, ss.original_id = $original_id
        MERGE (s)-[:HAS_SUBSECTION]->(ss)
        """
        with self.driver.session() as session:
            session.run(
                query,
                actual_section_id=actual_section_id,
                actual_subsection_id=actual_subsection_id,
                original_id=subsection_id,
                subsection_number=subsection_number,
                title=title,
            )

    def upsert_subsubsection(
        self, subsection_id: str, subsubsection_number: str, title: str, version_id: Optional[str] = None
    ) -> None:
        """Insert or update a subsubsection node and link to subsection.

        Args:
            subsection_id: Parent subsection identifier (base ID without version)
            subsubsection_number: Subsubsection number
            title: Subsubsection title
            version_id: Optional version identifier for multi-version support.
                       If provided, appends version to IDs for isolation.
                       If None, uses base IDs unchanged (backward compatible).

        Examples:
            # Non-versioned (backward compatible)
            >>> storage.upsert_subsubsection('bailey:ch60:s2:ss2.1', '2.1.1', 'Bronchi')
            # Creates subsubsection with id='bailey:ch60:s2:ss2.1:sss2.1.1'

            # Versioned
            >>> storage.upsert_subsubsection('bailey:ch60:s2:ss2.1', '2.1.1', 'Bronchi', version_id='v2_test')
            # Creates subsubsection with id='bailey:ch60:s2:ss2.1:sss2.1.1::v2_test'
        """
        subsubsection_id = f"{subsection_id}:sss{subsubsection_number}"
        actual_subsection_id = self._versioned_id(subsection_id, version_id)
        actual_subsubsection_id = self._versioned_id(subsubsection_id, version_id)
        query = """
        MERGE (ss:Subsection {id: $actual_subsection_id})
        MERGE (sss:Subsubsection {id: $actual_subsubsection_id})
        SET sss.number = $subsubsection_number, sss.title = $title, sss.original_id = $original_id
        MERGE (ss)-[:HAS_SUBSUBSECTION]->(sss)
        """
        with self.driver.session() as session:
            session.run(
                query,
                actual_subsection_id=actual_subsection_id,
                actual_subsubsection_id=actual_subsubsection_id,
                original_id=subsubsection_id,
                subsubsection_number=subsubsection_number,
                title=title,
            )

    def upsert_paragraph(
        self,
        parent_id: str,
        paragraph_number: str,
        text: str,
        chunk_id: str,
        page: int,
        bounds: List[float],
        cross_references: List[Dict] = None,
        version_id: Optional[str] = None,
    ) -> None:
        """Insert or update a paragraph node and link to parent.

        Args:
            parent_id: Parent node identifier (section/subsection/subsubsection, base ID)
            paragraph_number: Paragraph number
            text: Paragraph text content
            chunk_id: Unique chunk identifier (base chunk_id)
            page: Page number
            bounds: Bounding box coordinates [x1, y1, x2, y2]
            cross_references: List of cross-references to figures/tables (optional)
                Format: [{"type": "figure", "number": "60.5"}, ...]
            version_id: Optional version identifier for multi-version support.
                       If provided, appends version to chunk_id and parent_id for isolation.
                       If None, uses base IDs unchanged (backward compatible).

        Examples:
            # Non-versioned (backward compatible)
            >>> storage.upsert_paragraph('bailey:ch60:s2:ss2.1', '2.1.1', 'Text...', 'bailey:ch60:2.1.1',
            ...                          page=1025, bounds=[100, 200, 500, 250])
            # Creates paragraph with chunk_id='bailey:ch60:2.1.1'

            # Versioned
            >>> storage.upsert_paragraph('bailey:ch60:s2:ss2.1', '2.1.1', 'Text...', 'bailey:ch60:2.1.1',
            ...                          page=1025, bounds=[100, 200, 500, 250], version_id='v2_test')
            # Creates paragraph with chunk_id='bailey:ch60:2.1.1::v2_test', original_chunk_id='bailey:ch60:2.1.1'
        """
        if cross_references is None:
            cross_references = []

        # Serialize cross_references to JSON string for Neo4j storage
        # Neo4j doesn't support lists of maps, only primitives or arrays of primitives
        cross_references_json = json.dumps(cross_references)

        actual_parent_id = self._versioned_id(parent_id, version_id)
        actual_chunk_id = self._versioned_id(chunk_id, version_id)

        query = """
        MERGE (p:Paragraph {chunk_id: $actual_chunk_id})
        SET p.number = $paragraph_number, p.text = $text, p.page = $page, p.bounds = $bounds,
            p.cross_references = $cross_references, p.original_chunk_id = $original_chunk_id
        WITH p
        OPTIONAL MATCH (parent)
        WHERE parent.id = $actual_parent_id
        FOREACH (ignored IN CASE WHEN parent IS NOT NULL THEN [1] ELSE [] END |
            MERGE (parent)-[:HAS_PARAGRAPH]->(p)
        )
        """
        with self.driver.session() as session:
            session.run(
                query,
                actual_parent_id=actual_parent_id,
                actual_chunk_id=actual_chunk_id,
                original_chunk_id=chunk_id,
                paragraph_number=paragraph_number,
                text=text,
                page=page,
                bounds=bounds,
                cross_references=cross_references_json,
            )

    def upsert_table(
        self,
        paragraph_chunk_id: str,
        table_number: str,
        description: str,
        page: int,
        bounds: List[float],
        file_png: str = "",
        file_xlsx: str = "",
        version_id: Optional[str] = None,
    ) -> None:
        """Insert or update a table node and link to paragraph.

        Args:
            paragraph_chunk_id: Parent paragraph chunk identifier (base chunk_id)
            table_number: Table number
            description: Table description
            page: Page number
            bounds: Bounding box coordinates [x1, y1, x2, y2]
            file_png: PNG file path (optional)
            file_xlsx: Excel file path (optional)
            version_id: Optional version identifier for multi-version support.
                       If provided, uses versioned paragraph chunk_id for lookup.
                       If None, uses base chunk_id (backward compatible).

        Examples:
            # Non-versioned (backward compatible)
            >>> storage.upsert_table('bailey:ch60:2.1.1', '60.1', 'Lung volumes',
            ...                      page=1025, bounds=[100, 200, 500, 400])

            # Versioned
            >>> storage.upsert_table('bailey:ch60:2.1.1', '60.1', 'Lung volumes',
            ...                      page=1025, bounds=[100, 200, 500, 400], version_id='v2_test')
        """
        actual_paragraph_chunk_id = self._versioned_id(paragraph_chunk_id, version_id)
        query = """
        MATCH (p:Paragraph {chunk_id: $actual_paragraph_chunk_id})
        MERGE (t:Table {paragraph_id: $actual_paragraph_chunk_id, table_number: $table_number})
        SET t.file_png = $file_png, t.file_xlsx = $file_xlsx,
            t.description = $description, t.page = $page, t.bounds = $bounds
        MERGE (p)-[:CONTAINS_TABLE]->(t)
        """
        with self.driver.session() as session:
            session.run(
                query,
                actual_paragraph_chunk_id=actual_paragraph_chunk_id,
                table_number=table_number,
                file_png=file_png,
                file_xlsx=file_xlsx,
                description=description,
                page=page,
                bounds=bounds,
            )

    def upsert_figure(
        self,
        paragraph_chunk_id: str,
        figure_number: str,
        caption: str,
        page: int,
        bounds: List[float],
        file_png: str = "",
        version_id: Optional[str] = None,
    ) -> None:
        """Insert or update a figure node and link to paragraph.

        Args:
            paragraph_chunk_id: Parent paragraph chunk identifier (base chunk_id)
            figure_number: Figure number
            caption: Figure caption
            page: Page number
            bounds: Bounding box coordinates [x1, y1, x2, y2]
            file_png: PNG file path (optional)
            version_id: Optional version identifier for multi-version support.
                       If provided, uses versioned paragraph chunk_id for lookup.
                       If None, uses base chunk_id (backward compatible).

        Examples:
            # Non-versioned (backward compatible)
            >>> storage.upsert_figure('bailey:ch60:2.1.1', '60.5', 'Bronchial tree',
            ...                       page=1026, bounds=[100, 300, 500, 600])

            # Versioned
            >>> storage.upsert_figure('bailey:ch60:2.1.1', '60.5', 'Bronchial tree',
            ...                       page=1026, bounds=[100, 300, 500, 600], version_id='v2_test')
        """
        actual_paragraph_chunk_id = self._versioned_id(paragraph_chunk_id, version_id)
        query = """
        MATCH (p:Paragraph {chunk_id: $actual_paragraph_chunk_id})
        MERGE (f:Figure {paragraph_id: $actual_paragraph_chunk_id, figure_number: $figure_number})
        SET f.file_png = $file_png, f.caption = $caption, f.page = $page, f.bounds = $bounds
        MERGE (p)-[:CONTAINS_FIGURE]->(f)
        """
        with self.driver.session() as session:
            session.run(
                query,
                actual_paragraph_chunk_id=actual_paragraph_chunk_id,
                figure_number=figure_number,
                file_png=file_png,
                caption=caption,
                page=page,
                bounds=bounds,
            )

    def get_chapter_hierarchy(self, chapter_id: str) -> Dict:
        """Retrieve complete chapter hierarchy.

        Args:
            chapter_id: Chapter identifier

        Returns:
            Dictionary containing chapter and nested structure
        """
        query = """
        MATCH path = (c:Chapter {id: $chapter_id})-[:HAS_SECTION*0..]->(s:Section)
                     -[:HAS_SUBSECTION*0..]->(ss)-[:HAS_SUBSUBSECTION*0..]->(sss)
                     -[:HAS_PARAGRAPH*0..]->(p:Paragraph)
        RETURN c, collect(distinct s) as sections, collect(distinct ss) as subsections,
               collect(distinct sss) as subsubsections, collect(distinct p) as paragraphs
        """
        with self.driver.session() as session:
            result = session.run(query, chapter_id=chapter_id)
            record = result.single()

            if record:
                return {
                    "chapter": dict(record["c"]),
                    "sections": [dict(s) for s in record["sections"] if s],
                    "subsections": [dict(ss) for ss in record["subsections"] if ss],
                    "subsubsections": [
                        dict(sss) for sss in record["subsubsections"] if sss
                    ],
                    "paragraphs": [dict(p) for p in record["paragraphs"] if p],
                }
            return {}

    def link_sequential_paragraphs(self, chapter_id: str, version_id: Optional[str] = None) -> int:
        """Create NEXT/PREV relationships between paragraphs in reading order.

        Links paragraphs within the same parent (section/subsection/subsubsection)
        in sequential order based on their paragraph numbers.

        Args:
            chapter_id: Chapter identifier to link paragraphs within (base chapter_id)
            version_id: Optional version identifier for multi-version support.
                       If provided, uses versioned chapter_id for filtering.
                       If None, uses base chapter_id (backward compatible).

        Returns:
            Number of NEXT relationships created

        Examples:
            # Non-versioned (backward compatible)
            >>> links_created = storage.link_sequential_paragraphs('bailey:ch60')

            # Versioned
            >>> links_created = storage.link_sequential_paragraphs('bailey:ch60', version_id='v2_test')
        """
        actual_chapter_id = self._versioned_id(chapter_id, version_id)
        query = """
        MATCH (c:Chapter {id: $actual_chapter_id})
        MATCH (parent)-[:HAS_PARAGRAPH]->(p:Paragraph)
        WHERE (c)-[:HAS_SECTION*0..1]->()-[:HAS_SUBSECTION*0..1]->()-[:HAS_SUBSUBSECTION*0..1]->(parent)
        WITH parent, p
        ORDER BY p.number
        WITH parent, collect(p) as paragraphs
        UNWIND range(0, size(paragraphs)-2) as i
        WITH paragraphs[i] as current, paragraphs[i+1] as next
        MERGE (current)-[:NEXT]->(next)
        MERGE (next)-[:PREV]->(current)
        RETURN count(*) as links_created
        """
        with self.driver.session() as session:
            result = session.run(query, actual_chapter_id=actual_chapter_id)
            record = result.single()
            return record["links_created"] if record else 0

    def register_baseline_graph(self) -> str:
        """Register the current graph as baseline by adding :v1_baseline labels to all nodes.

        This method adds the :v1_baseline label to all existing nodes in the graph,
        establishing them as the baseline version. The operation is:

        1. **Idempotent**: Can be run multiple times safely. If all nodes already have
           the :v1_baseline label, returns immediately without making changes.

        2. **Non-destructive**: Only adds labels to nodes, does not modify existing
           data, properties, or relationships.

        3. **Backward compatible**: Original queries without version labels continue
           to work unchanged (e.g., MATCH (p:Paragraph) still returns all paragraphs).

        Processes nodes in batches to avoid memory issues:
        - Textbook, Chapter, Section, Subsection, Subsubsection: 1000 nodes per batch
        - Paragraph: 1000 nodes per batch with progress tracking (large volume)
        - Table, Figure: 1000 nodes per batch

        Returns:
            str: "v1_baseline" to indicate the baseline version identifier

        Example:
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> baseline = storage.register_baseline_graph()
            Registering 36221 nodes as baseline...
            Labeling Textbook nodes...
            Labeling Chapter nodes...
            ...
            Baseline registration complete - all nodes labeled
            >>> print(f'Baseline: {baseline}')
            Baseline: v1_baseline

            >>> # Running again is safe (idempotent)
            >>> baseline = storage.register_baseline_graph()
            No nodes to register (graph already baseline or empty)
        """
        with self.driver.session() as session:
            # Check if there are any nodes without version labels
            result = session.run(
                "MATCH (n) WHERE NOT n:v1_baseline RETURN count(n) as count"
            )
            unlabeled_count = result.single()["count"]

            if unlabeled_count == 0:
                print("No nodes to register (graph already baseline or empty)")
                return "v1_baseline"

            print(f"Registering {unlabeled_count} nodes as baseline...")

            # Add :v1_baseline label to all Textbook nodes in batches
            print("Labeling Textbook nodes...")
            while True:
                result = session.run(
                    """
                    MATCH (t:Textbook)
                    WHERE NOT t:v1_baseline
                    WITH t LIMIT 1000
                    SET t:v1_baseline
                    RETURN count(t) as count
                    """
                )
                count = result.single()["count"]
                if count == 0:
                    break

            # Add :v1_baseline label to all Chapter nodes in batches
            print("Labeling Chapter nodes...")
            while True:
                result = session.run(
                    """
                    MATCH (c:Chapter)
                    WHERE NOT c:v1_baseline
                    WITH c LIMIT 1000
                    SET c:v1_baseline
                    RETURN count(c) as count
                    """
                )
                count = result.single()["count"]
                if count == 0:
                    break

            # Add :v1_baseline label to all Section nodes in batches
            print("Labeling Section nodes...")
            while True:
                result = session.run(
                    """
                    MATCH (s:Section)
                    WHERE NOT s:v1_baseline
                    WITH s LIMIT 1000
                    SET s:v1_baseline
                    RETURN count(s) as count
                    """
                )
                count = result.single()["count"]
                if count == 0:
                    break

            # Add :v1_baseline label to all Subsection nodes in batches
            print("Labeling Subsection nodes...")
            while True:
                result = session.run(
                    """
                    MATCH (ss:Subsection)
                    WHERE NOT ss:v1_baseline
                    WITH ss LIMIT 1000
                    SET ss:v1_baseline
                    RETURN count(ss) as count
                    """
                )
                count = result.single()["count"]
                if count == 0:
                    break

            # Add :v1_baseline label to all Subsubsection nodes in batches
            print("Labeling Subsubsection nodes...")
            while True:
                result = session.run(
                    """
                    MATCH (sss:Subsubsection)
                    WHERE NOT sss:v1_baseline
                    WITH sss LIMIT 1000
                    SET sss:v1_baseline
                    RETURN count(sss) as count
                    """
                )
                count = result.single()["count"]
                if count == 0:
                    break

            # Add :v1_baseline label to all Paragraph nodes in batches with progress tracking
            print("Labeling Paragraph nodes...")
            result = session.run("MATCH (p:Paragraph) RETURN count(p) as count")
            total_paragraphs = result.single()["count"]
            labeled = 0
            batch_size = 1000

            while labeled < total_paragraphs:
                result = session.run(
                    """
                    MATCH (p:Paragraph)
                    WHERE NOT p:v1_baseline
                    WITH p LIMIT $batch
                    SET p:v1_baseline
                    RETURN count(p) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                if count == 0:
                    break
                labeled += count
                if labeled % 5000 == 0 or labeled == total_paragraphs:
                    print(f"  Progress: {labeled}/{total_paragraphs} paragraphs labeled")

            # Add :v1_baseline label to all Table nodes in batches
            print("Labeling Table nodes...")
            while True:
                result = session.run(
                    """
                    MATCH (t:Table)
                    WHERE NOT t:v1_baseline
                    WITH t LIMIT 1000
                    SET t:v1_baseline
                    RETURN count(t) as count
                    """
                )
                count = result.single()["count"]
                if count == 0:
                    break

            # Add :v1_baseline label to all Figure nodes in batches
            print("Labeling Figure nodes...")
            while True:
                result = session.run(
                    """
                    MATCH (f:Figure)
                    WHERE NOT f:v1_baseline
                    WITH f LIMIT 1000
                    SET f:v1_baseline
                    RETURN count(f) as count
                    """
                )
                count = result.single()["count"]
                if count == 0:
                    break

            # Create index on version labels for performance
            # Note: In Neo4j, you create index on label, not property
            print("Creating index on :v1_baseline label...")
            session.run("CREATE INDEX IF NOT EXISTS FOR (n:v1_baseline) ON (n.chunk_id)")

            # Verify all nodes labeled
            result = session.run(
                "MATCH (n) WHERE NOT n:v1_baseline RETURN count(n) as unlabeled"
            )
            unlabeled = result.single()["unlabeled"]

            if unlabeled == 0:
                print("Baseline registration complete - all nodes labeled")
            else:
                print(f"Warning: {unlabeled} nodes still unlabeled")

            return "v1_baseline"

    def create_snapshot(self, version_id: str, show_progress: bool = True) -> int:
        """Create a snapshot of the current graph by copying to a new version label.

        WARNING: This operation can take 10-15 minutes for large graphs (36K nodes).
        The entire graph is copied node-by-node and relationship-by-relationship.

        **Memory Considerations:**
        Large snapshots may require Neo4j heap size adjustments. If you encounter
        OutOfMemory errors, increase Neo4j heap size in neo4j.conf:
        - dbms.memory.heap.initial_size=2G
        - dbms.memory.heap.max_size=4G

        **Rollback Procedure:**
        If snapshot creation fails partway through, use cleanup_partial_snapshot()
        to remove incomplete snapshot nodes before retrying.

        Args:
            version_id: Version identifier for the snapshot (e.g., 'v2_test')
                       Must not contain 'baseline' (baseline already exists)
            show_progress: If True, print progress updates during copying

        Returns:
            int: Total number of nodes copied to the snapshot

        Raises:
            ValueError: If version_id contains 'baseline'

        Example:
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> count = storage.create_snapshot('v2_test', show_progress=True)
            Copying Textbook nodes...
            Copying Chapter nodes...
            ...
            Created snapshot 'v2_test' with 10 nodes
            >>> print(f'Snapshot nodes: {count}')
            Snapshot nodes: 10
        """
        # Baseline uses existing nodes, no copy needed
        if "baseline" in version_id:
            if show_progress:
                print("Baseline version already exists, no copy needed")
            return 0

        with self.driver.session() as session:
            # Check if snapshot already exists
            try:
                check_query = f"MATCH (n:{version_id}) RETURN count(n) as count LIMIT 1"
                result = session.run(check_query)
                existing_count = result.single()["count"]

                if existing_count > 0:
                    if show_progress:
                        print(
                            f"Snapshot '{version_id}' already exists with {existing_count} nodes"
                        )
                    return existing_count
            except Exception:
                # Label doesn't exist yet, which is fine
                pass

            # Count total nodes to copy (from v1_baseline)
            result = session.run(
                "MATCH (n:v1_baseline) RETURN count(n) as total"
            )
            total_nodes = result.single()["total"]

            if show_progress:
                print(f"Creating snapshot '{version_id}' from v1_baseline ({total_nodes} nodes)...")

            # Drop constraints temporarily to allow duplicate IDs across versions
            if show_progress:
                print("Temporarily dropping uniqueness constraints...")

            constraint_names = []
            result = session.run("SHOW CONSTRAINTS")
            for record in result:
                if record["type"] == "UNIQUENESS":
                    constraint_names.append(record["name"])
                    session.run(f"DROP CONSTRAINT {record['name']}")

            if show_progress and constraint_names:
                print(f"  Dropped {len(constraint_names)} constraints")

            total_copied = 0

            # Copy Textbook nodes
            if show_progress:
                print("Copying Textbook nodes...")
            result = session.run(
                f"""
                MATCH (t:Textbook:v1_baseline)
                CREATE (copy:Textbook:{version_id})
                SET copy = properties(t)
                RETURN count(copy) as count
                """
            )
            count = result.single()["count"]
            total_copied += count
            if show_progress and count > 0:
                print(f"  Copied {count} Textbook nodes")

            # Copy Chapter nodes in batches
            if show_progress:
                print("Copying Chapter nodes...")
            batch_size = 1000
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (c:Chapter:v1_baseline)
                    WHERE NOT (c:{version_id})
                    WITH c LIMIT $batch
                    CREATE (copy:Chapter:{version_id})
                    SET copy = properties(c)
                    RETURN count(copy) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                total_copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"  Copied {copied} Chapter nodes")

            # Copy Section nodes in batches
            if show_progress:
                print("Copying Section nodes...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (s:Section:v1_baseline)
                    WHERE NOT (s:{version_id})
                    WITH s LIMIT $batch
                    CREATE (copy:Section:{version_id})
                    SET copy = properties(s)
                    RETURN count(copy) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                total_copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"  Copied {copied} Section nodes")

            # Copy Subsection nodes in batches
            if show_progress:
                print("Copying Subsection nodes...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (ss:Subsection:v1_baseline)
                    WHERE NOT (ss:{version_id})
                    WITH ss LIMIT $batch
                    CREATE (copy:Subsection:{version_id})
                    SET copy = properties(ss)
                    RETURN count(copy) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                total_copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"  Copied {copied} Subsection nodes")

            # Copy Subsubsection nodes in batches
            if show_progress:
                print("Copying Subsubsection nodes...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (sss:Subsubsection:v1_baseline)
                    WHERE NOT (sss:{version_id})
                    WITH sss LIMIT $batch
                    CREATE (copy:Subsubsection:{version_id})
                    SET copy = properties(sss)
                    RETURN count(copy) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                total_copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"  Copied {copied} Subsubsection nodes")

            # Copy Paragraph nodes in batches with progress tracking
            if show_progress:
                print("Copying Paragraph nodes...")
            result = session.run(
                "MATCH (p:Paragraph:v1_baseline) RETURN count(p) as count"
            )
            total_paragraphs = result.single()["count"]
            copied_paragraphs = 0

            while copied_paragraphs < total_paragraphs:
                result = session.run(
                    f"""
                    MATCH (p:Paragraph:v1_baseline)
                    WHERE NOT (p:{version_id})
                    WITH p LIMIT $batch
                    CREATE (copy:Paragraph:{version_id})
                    SET copy = properties(p)
                    RETURN count(copy) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                if count == 0:
                    break
                copied_paragraphs += count
                total_copied += count
                if show_progress and (copied_paragraphs % 5000 == 0 or copied_paragraphs == total_paragraphs):
                    print(f"  Progress: {copied_paragraphs}/{total_paragraphs} Paragraph nodes")

            # Copy Table nodes in batches
            if show_progress:
                print("Copying Table nodes...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (t:Table:v1_baseline)
                    WHERE NOT (t:{version_id})
                    WITH t LIMIT $batch
                    CREATE (copy:Table:{version_id})
                    SET copy = properties(t)
                    RETURN count(copy) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                total_copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"  Copied {copied} Table nodes")

            # Copy Figure nodes in batches
            if show_progress:
                print("Copying Figure nodes...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (f:Figure:v1_baseline)
                    WHERE NOT (f:{version_id})
                    WITH f LIMIT $batch
                    CREATE (copy:Figure:{version_id})
                    SET copy = properties(f)
                    RETURN count(copy) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                total_copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"  Copied {copied} Figure nodes")

            # Now copy relationships
            if show_progress:
                print("Copying relationships...")

            # Copy CONTAINS relationships (Textbook -> Chapter)
            if show_progress:
                print("  Copying CONTAINS relationships...")
            result = session.run(
                f"""
                MATCH (t_orig:Textbook:v1_baseline)-[:CONTAINS]->(c_orig:Chapter:v1_baseline)
                MATCH (t_copy:Textbook:{version_id} {{id: t_orig.id}})
                MATCH (c_copy:Chapter:{version_id} {{id: c_orig.id}})
                MERGE (t_copy)-[:CONTAINS]->(c_copy)
                RETURN count(*) as count
                """
            )
            rel_count = result.single()["count"]
            if show_progress and rel_count > 0:
                print(f"    Created {rel_count} CONTAINS relationships")

            # Copy HAS_SECTION relationships (Chapter -> Section)
            if show_progress:
                print("  Copying HAS_SECTION relationships...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (c_orig:Chapter:v1_baseline)-[:HAS_SECTION]->(s_orig:Section:v1_baseline)
                    MATCH (c_copy:Chapter:{version_id} {{id: c_orig.id}})
                    MATCH (s_copy:Section:{version_id} {{id: s_orig.id}})
                    WHERE NOT (c_copy)-[:HAS_SECTION]->(s_copy)
                    WITH c_copy, s_copy LIMIT $batch
                    MERGE (c_copy)-[:HAS_SECTION]->(s_copy)
                    RETURN count(*) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"    Created {copied} HAS_SECTION relationships")

            # Copy HAS_SUBSECTION relationships (Section -> Subsection)
            if show_progress:
                print("  Copying HAS_SUBSECTION relationships...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (s_orig:Section:v1_baseline)-[:HAS_SUBSECTION]->(ss_orig:Subsection:v1_baseline)
                    MATCH (s_copy:Section:{version_id} {{id: s_orig.id}})
                    MATCH (ss_copy:Subsection:{version_id} {{id: ss_orig.id}})
                    WHERE NOT (s_copy)-[:HAS_SUBSECTION]->(ss_copy)
                    WITH s_copy, ss_copy LIMIT $batch
                    MERGE (s_copy)-[:HAS_SUBSECTION]->(ss_copy)
                    RETURN count(*) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"    Created {copied} HAS_SUBSECTION relationships")

            # Copy HAS_SUBSUBSECTION relationships (Subsection -> Subsubsection)
            if show_progress:
                print("  Copying HAS_SUBSUBSECTION relationships...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (ss_orig:Subsection:v1_baseline)-[:HAS_SUBSUBSECTION]->(sss_orig:Subsubsection:v1_baseline)
                    MATCH (ss_copy:Subsection:{version_id} {{id: ss_orig.id}})
                    MATCH (sss_copy:Subsubsection:{version_id} {{id: sss_orig.id}})
                    WHERE NOT (ss_copy)-[:HAS_SUBSUBSECTION]->(sss_copy)
                    WITH ss_copy, sss_copy LIMIT $batch
                    MERGE (ss_copy)-[:HAS_SUBSUBSECTION]->(sss_copy)
                    RETURN count(*) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"    Created {copied} HAS_SUBSUBSECTION relationships")

            # Copy HAS_PARAGRAPH relationships (from all parent types)
            if show_progress:
                print("  Copying HAS_PARAGRAPH relationships...")
            copied = 0
            # Paragraph parents can be Section, Subsection, or Subsubsection
            for parent_type in ["Section", "Subsection", "Subsubsection"]:
                while True:
                    result = session.run(
                        f"""
                        MATCH (parent_orig:{parent_type}:v1_baseline)-[:HAS_PARAGRAPH]->(p_orig:Paragraph:v1_baseline)
                        MATCH (parent_copy:{parent_type}:{version_id} {{id: parent_orig.id}})
                        MATCH (p_copy:Paragraph:{version_id} {{chunk_id: p_orig.chunk_id}})
                        WHERE NOT (parent_copy)-[:HAS_PARAGRAPH]->(p_copy)
                        WITH parent_copy, p_copy LIMIT $batch
                        MERGE (parent_copy)-[:HAS_PARAGRAPH]->(p_copy)
                        RETURN count(*) as count
                        """,
                        batch=batch_size,
                    )
                    count = result.single()["count"]
                    copied += count
                    if count == 0:
                        break
            if show_progress and copied > 0:
                print(f"    Created {copied} HAS_PARAGRAPH relationships")

            # Copy NEXT relationships between Paragraphs
            if show_progress:
                print("  Copying NEXT relationships...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (p1_orig:Paragraph:v1_baseline)-[:NEXT]->(p2_orig:Paragraph:v1_baseline)
                    MATCH (p1_copy:Paragraph:{version_id} {{chunk_id: p1_orig.chunk_id}})
                    MATCH (p2_copy:Paragraph:{version_id} {{chunk_id: p2_orig.chunk_id}})
                    WHERE NOT (p1_copy)-[:NEXT]->(p2_copy)
                    WITH p1_copy, p2_copy LIMIT $batch
                    MERGE (p1_copy)-[:NEXT]->(p2_copy)
                    RETURN count(*) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"    Created {copied} NEXT relationships")

            # Copy PREV relationships between Paragraphs
            if show_progress:
                print("  Copying PREV relationships...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (p1_orig:Paragraph:v1_baseline)-[:PREV]->(p2_orig:Paragraph:v1_baseline)
                    MATCH (p1_copy:Paragraph:{version_id} {{chunk_id: p1_orig.chunk_id}})
                    MATCH (p2_copy:Paragraph:{version_id} {{chunk_id: p2_orig.chunk_id}})
                    WHERE NOT (p1_copy)-[:PREV]->(p2_copy)
                    WITH p1_copy, p2_copy LIMIT $batch
                    MERGE (p1_copy)-[:PREV]->(p2_copy)
                    RETURN count(*) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"    Created {copied} PREV relationships")

            # Copy CONTAINS_TABLE relationships
            if show_progress:
                print("  Copying CONTAINS_TABLE relationships...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (p_orig:Paragraph:v1_baseline)-[:CONTAINS_TABLE]->(t_orig:Table:v1_baseline)
                    MATCH (p_copy:Paragraph:{version_id} {{chunk_id: p_orig.chunk_id}})
                    MATCH (t_copy:Table:{version_id} {{paragraph_id: t_orig.paragraph_id, table_number: t_orig.table_number}})
                    WHERE NOT (p_copy)-[:CONTAINS_TABLE]->(t_copy)
                    WITH p_copy, t_copy LIMIT $batch
                    MERGE (p_copy)-[:CONTAINS_TABLE]->(t_copy)
                    RETURN count(*) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"    Created {copied} CONTAINS_TABLE relationships")

            # Copy CONTAINS_FIGURE relationships
            if show_progress:
                print("  Copying CONTAINS_FIGURE relationships...")
            copied = 0
            while True:
                result = session.run(
                    f"""
                    MATCH (p_orig:Paragraph:v1_baseline)-[:CONTAINS_FIGURE]->(f_orig:Figure:v1_baseline)
                    MATCH (p_copy:Paragraph:{version_id} {{chunk_id: p_orig.chunk_id}})
                    MATCH (f_copy:Figure:{version_id} {{paragraph_id: f_orig.paragraph_id, figure_number: f_orig.figure_number}})
                    WHERE NOT (p_copy)-[:CONTAINS_FIGURE]->(f_copy)
                    WITH p_copy, f_copy LIMIT $batch
                    MERGE (p_copy)-[:CONTAINS_FIGURE]->(f_copy)
                    RETURN count(*) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                copied += count
                if count == 0:
                    break
            if show_progress and copied > 0:
                print(f"    Created {copied} CONTAINS_FIGURE relationships")

            # Verify snapshot completeness
            result = session.run(
                f"MATCH (n:{version_id}) RETURN count(n) as count"
            )
            final_count = result.single()["count"]

            # Recreate constraints
            if show_progress:
                print("Recreating uniqueness constraints...")

            self.create_constraints()

            if show_progress and constraint_names:
                print(f"  Recreated {len(constraint_names)} constraints")

            if final_count != total_nodes:
                print(
                    f"Warning: Snapshot may be incomplete. "
                    f"Expected {total_nodes} nodes, got {final_count} nodes"
                )

            if show_progress:
                print(f"Created snapshot '{version_id}' with {final_count} nodes")

            return final_count

    def restore_snapshot(self, version_id: str) -> None:
        """Restore a snapshot by switching query context to use version label.

        **Note:** This method uses a query-based approach rather than physically
        copying nodes. To query a specific version, update application queries to
        filter by version label instead of using this method.

        For example:
        - Original: MATCH (p:Paragraph) WHERE ...
        - Versioned: MATCH (p:Paragraph:v2_test) WHERE ...

        This approach avoids massive copy operations and allows instant version
        switching by simply changing the label filter in queries.

        Args:
            version_id: Version identifier to restore

        Raises:
            ValueError: If snapshot does not exist

        Example:
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> storage.restore_snapshot('v2_test')
            Snapshot 'v2_test' verified. Update queries to use :v2_test label filter.
        """
        with self.driver.session() as session:
            # Verify snapshot exists
            result = session.run(
                f"MATCH (n:{version_id}) RETURN count(n) as count LIMIT 1"
            )
            count = result.single()["count"]

            if count == 0:
                raise ValueError(f"Snapshot '{version_id}' not found")

            print(
                f"Snapshot '{version_id}' verified with {count} nodes. "
                f"Update queries to use :{version_id} label filter."
            )

    def delete_snapshot(self, version_id: str) -> int:
        """Delete a snapshot by removing all nodes with versioned IDs.

        WARNING: This operation is irreversible. All nodes and relationships
        with IDs ending in '::version_id' will be permanently deleted.

        Args:
            version_id: Version identifier to delete
                       Must not contain 'baseline' (baseline cannot be deleted)

        Returns:
            int: Total number of nodes deleted

        Raises:
            ValueError: If trying to delete baseline version

        Example:
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> deleted = storage.delete_snapshot('v2_test')
            Deleted snapshot 'v2_test': 10 nodes removed
            >>> print(f'Deleted: {deleted}')
            Deleted: 10
        """
        # Prevent deletion of baseline
        if "baseline" in version_id:
            raise ValueError("Cannot delete baseline version")

        with self.driver.session() as session:
            deleted_total = 0
            batch_size = 1000
            version_suffix = f"::{version_id}"

            # Delete all nodes with versioned IDs in batches
            # DETACH DELETE removes the node and all its relationships
            while True:
                # Match nodes by ID pattern (for paragraphs use chunk_id)
                result = session.run(
                    """
                    MATCH (n)
                    WHERE (n.id IS NOT NULL AND n.id ENDS WITH $suffix)
                       OR (n.chunk_id IS NOT NULL AND n.chunk_id ENDS WITH $suffix)
                    WITH n LIMIT $batch
                    DETACH DELETE n
                    RETURN count(n) as count
                    """,
                    suffix=version_suffix,
                    batch=batch_size,
                )
                count = result.single()["count"]
                deleted_total += count
                if count == 0:
                    break

            # Verify deletion
            result = session.run(
                """
                MATCH (n)
                WHERE (n.id IS NOT NULL AND n.id ENDS WITH $suffix)
                   OR (n.chunk_id IS NOT NULL AND n.chunk_id ENDS WITH $suffix)
                RETURN count(n) as count
                """,
                suffix=version_suffix,
            )
            remaining = result.single()["count"]

            if remaining > 0:
                raise Exception(
                    f"Deletion incomplete: {remaining} nodes still exist with IDs ending in '{version_suffix}'"
                )

            print(f"Deleted snapshot '{version_id}': {deleted_total} nodes removed")
            return deleted_total

    def list_snapshots(self) -> List[str]:
        """List all available snapshot version identifiers.

        Returns:
            List[str]: Sorted list of version identifiers (e.g., ['v1_baseline', 'v2_test'])

        Example:
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> snapshots = storage.list_snapshots()
            >>> print(f'Available snapshots: {snapshots}')
            Available snapshots: ['v1_baseline', 'v2_test']
        """
        with self.driver.session() as session:
            # Query for distinct version labels (labels starting with 'v')
            result = session.run(
                """
                MATCH (n)
                UNWIND labels(n) as label
                WITH DISTINCT label
                WHERE label STARTS WITH 'v'
                RETURN label as version_label
                ORDER BY label
                """
            )

            # Extract version_ids
            version_ids = [record["version_label"] for record in result]

            return version_ids

    def cleanup_partial_snapshot(self, version_id: str) -> int:
        """Remove a partial snapshot that failed during creation.

        Use this method if create_snapshot() fails partway through, leaving
        incomplete snapshot nodes in the database.

        Args:
            version_id: Version identifier of the partial snapshot to remove

        Returns:
            int: Number of nodes removed

        Example:
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> # If create_snapshot failed...
            >>> removed = storage.cleanup_partial_snapshot('v2_failed')
            Cleaned up partial snapshot 'v2_failed': 5000 nodes removed
            >>> print(f'Removed: {removed}')
            Removed: 5000
        """
        # Use same deletion logic as delete_snapshot but without baseline check
        with self.driver.session() as session:
            deleted_total = 0
            batch_size = 1000

            while True:
                result = session.run(
                    f"""
                    MATCH (n:{version_id})
                    WITH n LIMIT $batch
                    DETACH DELETE n
                    RETURN count(n) as count
                    """,
                    batch=batch_size,
                )
                count = result.single()["count"]
                deleted_total += count
                if count == 0:
                    break

            print(
                f"Cleaned up partial snapshot '{version_id}': {deleted_total} nodes removed"
            )
            return deleted_total

    def validate_graph(self, version_id: Optional[str] = None) -> Dict[str, Any]:
        """Validate graph health and integrity.

        Performs comprehensive validation checks including:
        - Node counts by type
        - Relationship counts by type
        - Orphan node detection
        - Broken NEXT/PREV chain detection
        - Duplicate chunk_id detection
        - Invalid hierarchy detection

        Args:
            version_id: Optional version identifier to validate (e.g., 'v1_baseline', 'v2_test')
                       If None, validates current graph without version filter

        Returns:
            Dict containing validation report with status and detailed findings

        Example:
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> report = storage.validate_graph('v1_baseline')
            >>> print(f"Status: {report['status']}")
            Status: valid
        """
        with self.driver.session() as session:
            # Construct version filter for ID-based versioning
            # Baseline uses the main graph without version suffix
            if version_id and "baseline" in version_id:
                version_suffix = None
            elif version_id:
                version_suffix = f"::{version_id}"
            else:
                version_suffix = None

            # Count nodes by type
            node_counts = {}
            for node_type in [
                "Textbook",
                "Chapter",
                "Section",
                "Subsection",
                "Subsubsection",
                "Table",
                "Figure",
            ]:
                if version_suffix:
                    query = f"MATCH (n:{node_type}) WHERE n.id ENDS WITH $suffix RETURN count(n) as count"
                    result = session.run(query, suffix=version_suffix)
                else:
                    query = f"MATCH (n:{node_type}) WHERE NOT n.id CONTAINS '::' RETURN count(n) as count"
                    result = session.run(query)
                node_counts[node_type] = result.single()["count"]

            # Paragraphs use chunk_id instead of id
            if version_suffix:
                query = "MATCH (p:Paragraph) WHERE p.chunk_id ENDS WITH $suffix RETURN count(p) as count"
                result = session.run(query, suffix=version_suffix)
            else:
                query = "MATCH (p:Paragraph) WHERE NOT p.chunk_id CONTAINS '::' RETURN count(p) as count"
                result = session.run(query)
            node_counts["Paragraph"] = result.single()["count"]

            # Count relationships by type
            relationship_counts = {}
            for rel_type in [
                "CONTAINS",
                "HAS_SECTION",
                "HAS_SUBSECTION",
                "HAS_SUBSUBSECTION",
                "HAS_PARAGRAPH",
                "NEXT",
                "PREV",
                "CONTAINS_TABLE",
                "CONTAINS_FIGURE",
            ]:
                # For versioned validation, filter by nodes having version suffix
                if version_suffix:
                    query = f"""
                    MATCH (n)-[r:{rel_type}]->(m)
                    WHERE ((n.id IS NOT NULL AND n.id ENDS WITH $suffix) OR
                           (n.chunk_id IS NOT NULL AND n.chunk_id ENDS WITH $suffix))
                      AND ((m.id IS NOT NULL AND m.id ENDS WITH $suffix) OR
                           (m.chunk_id IS NOT NULL AND m.chunk_id ENDS WITH $suffix))
                    RETURN count(r) as count
                    """
                    result = session.run(query, suffix=version_suffix)
                else:
                    query = f"""
                    MATCH (n)-[r:{rel_type}]->(m)
                    WHERE ((n.id IS NULL OR NOT n.id CONTAINS '::') OR
                           (n.chunk_id IS NULL OR NOT n.chunk_id CONTAINS '::'))
                      AND ((m.id IS NULL OR NOT m.id CONTAINS '::') OR
                           (m.chunk_id IS NULL OR NOT m.chunk_id CONTAINS '::'))
                    RETURN count(r) as count
                    """
                    result = session.run(query)
                relationship_counts[rel_type] = result.single()["count"]

            # Find orphan paragraphs (paragraphs without parent relationships)
            if version_suffix:
                query = """
                MATCH (p:Paragraph)
                WHERE p.chunk_id ENDS WITH $suffix
                  AND NOT EXISTS((p)<-[:HAS_PARAGRAPH]-())
                RETURN count(p) as orphans
                """
                orphan_paragraphs = session.run(query, suffix=version_suffix).single()["orphans"]
            else:
                query = """
                MATCH (p:Paragraph)
                WHERE NOT p.chunk_id CONTAINS '::'
                  AND NOT EXISTS((p)<-[:HAS_PARAGRAPH]-())
                RETURN count(p) as orphans
                """
                orphan_paragraphs = session.run(query).single()["orphans"]

            # Check for broken NEXT/PREV chains
            if version_suffix:
                query = """
                MATCH (p:Paragraph)-[:NEXT]->(p2:Paragraph)
                WHERE p.chunk_id ENDS WITH $suffix
                  AND p2.chunk_id ENDS WITH $suffix
                  AND NOT EXISTS((p2)-[:PREV]->(p))
                RETURN count(p) as broken_next
                """
                broken_next = session.run(query, suffix=version_suffix).single()["broken_next"]

                query = """
                MATCH (p:Paragraph)-[:PREV]->(p2:Paragraph)
                WHERE p.chunk_id ENDS WITH $suffix
                  AND p2.chunk_id ENDS WITH $suffix
                  AND NOT EXISTS((p2)-[:NEXT]->(p))
                RETURN count(p) as broken_prev
                """
                broken_prev = session.run(query, suffix=version_suffix).single()["broken_prev"]
            else:
                query = """
                MATCH (p:Paragraph)-[:NEXT]->(p2:Paragraph)
                WHERE NOT p.chunk_id CONTAINS '::'
                  AND NOT p2.chunk_id CONTAINS '::'
                  AND NOT EXISTS((p2)-[:PREV]->(p))
                RETURN count(p) as broken_next
                """
                broken_next = session.run(query).single()["broken_next"]

                query = """
                MATCH (p:Paragraph)-[:PREV]->(p2:Paragraph)
                WHERE NOT p.chunk_id CONTAINS '::'
                  AND NOT p2.chunk_id CONTAINS '::'
                  AND NOT EXISTS((p2)-[:NEXT]->(p))
                RETURN count(p) as broken_prev
                """
                broken_prev = session.run(query).single()["broken_prev"]

            # Check for duplicate chunk_ids
            if version_suffix:
                query = """
                MATCH (p:Paragraph)
                WHERE p.chunk_id ENDS WITH $suffix
                WITH p.chunk_id as chunk_id, count(*) as count
                WHERE count > 1
                RETURN sum(count) as duplicates
                """
                result = session.run(query, suffix=version_suffix).single()
            else:
                query = """
                MATCH (p:Paragraph)
                WHERE NOT p.chunk_id CONTAINS '::'
                WITH p.chunk_id as chunk_id, count(*) as count
                WHERE count > 1
                RETURN sum(count) as duplicates
                """
                result = session.run(query).single()
            duplicates = result["duplicates"] if result["duplicates"] else 0

            # Verify parent-child relationships are valid (ID hierarchy check)
            # Check if section original_id properly starts with their chapter original_id
            if version_suffix:
                query = """
                MATCH (c:Chapter)-[:HAS_SECTION]->(s:Section)
                WHERE c.id ENDS WITH $suffix
                  AND s.id ENDS WITH $suffix
                  AND NOT s.original_id STARTS WITH c.original_id
                RETURN count(s) as invalid
                """
                invalid_sections = session.run(query, suffix=version_suffix).single()["invalid"]
            else:
                query = """
                MATCH (c:Chapter)-[:HAS_SECTION]->(s:Section)
                WHERE NOT c.id CONTAINS '::'
                  AND NOT s.id CONTAINS '::'
                  AND (s.original_id IS NULL OR NOT s.original_id STARTS WITH c.id)
                RETURN count(s) as invalid
                """
                invalid_sections = session.run(query).single()["invalid"]

            # Determine overall status
            issues = (
                orphan_paragraphs > 0
                or broken_next > 0
                or broken_prev > 0
                or duplicates > 0
                or invalid_sections > 0
            )

            return {
                "version_id": version_id or "current",
                "node_counts": node_counts,
                "relationship_counts": relationship_counts,
                "orphan_paragraphs": orphan_paragraphs,
                "broken_next_chains": broken_next,
                "broken_prev_chains": broken_prev,
                "duplicate_chunk_ids": duplicates,
                "invalid_hierarchies": invalid_sections,
                "status": "issues_found" if issues else "valid",
            }

    def get_graph_stats(self, version_id: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive statistical analysis of the graph.

        Extends validate_graph() with additional statistics including:
        - Text length statistics (avg, min, max)
        - Top chapters by paragraph count
        - Cross-reference statistics

        Args:
            version_id: Optional version identifier (e.g., 'v1_baseline')

        Returns:
            Dict containing validation report plus statistical analysis

        Example:
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> stats = storage.get_graph_stats('v1_baseline')
            >>> print(f"Average text length: {stats['text_stats']['avg']}")
            Average text length: 245.3
        """
        # Get base validation
        validation = self.validate_graph(version_id)

        with self.driver.session() as session:
            version_label = f":{version_id}" if version_id else ""

            # Get paragraph text length statistics
            query = f"""
            MATCH (p:Paragraph{version_label})
            RETURN avg(size(p.text)) as avg_text_length,
                   min(size(p.text)) as min_text_length,
                   max(size(p.text)) as max_text_length
            """
            result = session.run(query).single()
            text_stats = {
                "avg": result["avg_text_length"],
                "min": result["min_text_length"],
                "max": result["max_text_length"],
            }

            # Get paragraph distribution by chapter
            query = f"""
            MATCH (c:Chapter{version_label})-[:HAS_SECTION|HAS_SUBSECTION|HAS_SUBSUBSECTION*]->(p:Paragraph{version_label})
            WITH c, count(p) as paragraphs
            RETURN c.number as chapter, paragraphs
            ORDER BY paragraphs DESC
            LIMIT 10
            """
            top_chapters = session.run(query).data()

            # Get cross-reference statistics
            query = f"""
            MATCH (p:Paragraph{version_label})
            WHERE p.cross_references IS NOT NULL AND p.cross_references <> '[]'
            RETURN count(p) as paragraphs_with_refs
            """
            refs_count = session.run(query).single()["paragraphs_with_refs"]

            # Return comprehensive stats
            return {
                **validation,
                "text_stats": text_stats,
                "top_chapters_by_paragraphs": top_chapters,
                "paragraphs_with_cross_references": refs_count,
            }

    def compare_with_qdrant(
        self, qdrant_chunk_ids: Set[str], version_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Compare Neo4j paragraphs with Qdrant vectors for consistency.

        Identifies chunks that exist in one database but not the other,
        helping detect synchronization issues between Neo4j and Qdrant.

        Args:
            qdrant_chunk_ids: Set of chunk_ids from Qdrant
            version_id: Optional version identifier for Neo4j

        Returns:
            Dict containing comparison report with consistency status

        Example:
            >>> from qdrant_client import QdrantClient
            >>> qclient = QdrantClient(host='localhost', port=6333)
            >>> result, _ = qclient.scroll('textbook_chunks', limit=1000)
            >>> qids = set(p.payload.get('chunk_id') for p in result)
            >>> storage = Neo4jStorage(uri='bolt://localhost:7687', user='neo4j', password='password')
            >>> comparison = storage.compare_with_qdrant(qids)
            >>> print(f"Consistency: {comparison['consistency']}")
            Consistency: pass
        """
        with self.driver.session() as session:
            version_label = f":{version_id}" if version_id else ""

            # Get all paragraph chunk_ids from Neo4j
            query = f"""
            MATCH (p:Paragraph{version_label})
            RETURN p.chunk_id as chunk_id
            """
            result = session.run(query)
            neo4j_chunk_ids = set(record["chunk_id"] for record in result)

            # Calculate set differences
            in_neo4j_not_qdrant = neo4j_chunk_ids - qdrant_chunk_ids
            in_qdrant_not_neo4j = qdrant_chunk_ids - neo4j_chunk_ids
            in_both = neo4j_chunk_ids & qdrant_chunk_ids

            # Return comparison report
            return {
                "neo4j_count": len(neo4j_chunk_ids),
                "qdrant_count": len(qdrant_chunk_ids),
                "common_count": len(in_both),
                "only_in_neo4j": len(in_neo4j_not_qdrant),
                "only_in_qdrant": len(in_qdrant_not_neo4j),
                "consistency": "pass"
                if len(in_neo4j_not_qdrant) == 0 and len(in_qdrant_not_neo4j) == 0
                else "mismatch",
                "sample_only_neo4j": list(in_neo4j_not_qdrant)[:10],
                "sample_only_qdrant": list(in_qdrant_not_neo4j)[:10],
            }

    def batch_upsert_hierarchy(
        self,
        textbook_id: str,
        textbook_name: str,
        chapter_number: str,
        chapter_title: str,
        chapter_version: int,
        sections: List[Dict[str, Any]],
        subsections: List[Dict[str, Any]],
        subsubsections: List[Dict[str, Any]],
        version_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Batch upsert hierarchy nodes (textbook, chapter, sections, subsections, subsubsections).

        Uses UNWIND for efficient batch operations in a single session.

        Args:
            textbook_id: Textbook identifier
            textbook_name: Full name of the textbook
            chapter_number: Chapter number
            chapter_title: Chapter title
            chapter_version: Version number for the chapter
            sections: List of section dicts with keys: chapter_id, number, title
            subsections: List of subsection dicts with keys: section_id, number, title
            subsubsections: List of subsubsection dicts with keys: subsection_id, number, title
            version_id: Optional version identifier for multi-version support

        Returns:
            Dict with counts of upserted nodes by type

        Example:
            >>> storage.batch_upsert_hierarchy(
            ...     textbook_id='bailey',
            ...     textbook_name='Bailey & Love',
            ...     chapter_number='60',
            ...     chapter_title='The Thorax',
            ...     chapter_version=1,
            ...     sections=[{'chapter_id': 'bailey:ch60', 'number': '1', 'title': 'Intro'}],
            ...     subsections=[],
            ...     subsubsections=[],
            ...     version_id='staging_123'
            ... )
            {'textbook': 1, 'chapter': 1, 'sections': 1, 'subsections': 0, 'subsubsections': 0}
        """
        counts = {'textbook': 0, 'chapter': 0, 'sections': 0, 'subsections': 0, 'subsubsections': 0}

        with self.driver.session() as session:
            # Upsert textbook
            actual_textbook_id = self._versioned_id(textbook_id, version_id)
            session.run(
                """
                MERGE (t:Textbook {id: $actual_id})
                SET t.name = $name, t.original_id = $original_id
                """,
                actual_id=actual_textbook_id,
                name=textbook_name,
                original_id=textbook_id,
            )
            counts['textbook'] = 1

            # Upsert chapter
            chapter_id = f"{textbook_id}:ch{chapter_number}"
            actual_chapter_id = self._versioned_id(chapter_id, version_id)
            session.run(
                """
                MERGE (t:Textbook {id: $actual_textbook_id})
                MERGE (c:Chapter {id: $actual_chapter_id})
                SET c.number = $chapter_number, c.title = $title, c.version = $version, c.original_id = $original_id
                MERGE (t)-[:CONTAINS]->(c)
                """,
                actual_textbook_id=actual_textbook_id,
                actual_chapter_id=actual_chapter_id,
                original_id=chapter_id,
                chapter_number=chapter_number,
                title=chapter_title,
                version=chapter_version,
            )
            counts['chapter'] = 1

            # Batch upsert sections using UNWIND
            if sections:
                section_data = []
                for sec in sections:
                    section_id = f"{sec['chapter_id']}:s{sec['number']}"
                    section_data.append({
                        'actual_chapter_id': self._versioned_id(sec['chapter_id'], version_id),
                        'actual_section_id': self._versioned_id(section_id, version_id),
                        'original_id': section_id,
                        'number': sec['number'],
                        'title': sec['title'],
                    })

                result = session.run(
                    """
                    UNWIND $sections AS sec
                    MERGE (c:Chapter {id: sec.actual_chapter_id})
                    MERGE (s:Section {id: sec.actual_section_id})
                    SET s.number = sec.number, s.title = sec.title, s.original_id = sec.original_id
                    MERGE (c)-[:HAS_SECTION]->(s)
                    RETURN count(s) as count
                    """,
                    sections=section_data,
                )
                counts['sections'] = result.single()['count']

            # Batch upsert subsections using UNWIND
            if subsections:
                subsection_data = []
                for subsec in subsections:
                    subsection_id = f"{subsec['section_id']}:ss{subsec['number']}"
                    subsection_data.append({
                        'actual_section_id': self._versioned_id(subsec['section_id'], version_id),
                        'actual_subsection_id': self._versioned_id(subsection_id, version_id),
                        'original_id': subsection_id,
                        'number': subsec['number'],
                        'title': subsec['title'],
                    })

                result = session.run(
                    """
                    UNWIND $subsections AS subsec
                    MERGE (s:Section {id: subsec.actual_section_id})
                    MERGE (ss:Subsection {id: subsec.actual_subsection_id})
                    SET ss.number = subsec.number, ss.title = subsec.title, ss.original_id = subsec.original_id
                    MERGE (s)-[:HAS_SUBSECTION]->(ss)
                    RETURN count(ss) as count
                    """,
                    subsections=subsection_data,
                )
                counts['subsections'] = result.single()['count']

            # Batch upsert subsubsections using UNWIND
            if subsubsections:
                subsubsection_data = []
                for subsubsec in subsubsections:
                    subsubsection_id = f"{subsubsec['subsection_id']}:sss{subsubsec['number']}"
                    subsubsection_data.append({
                        'actual_subsection_id': self._versioned_id(subsubsec['subsection_id'], version_id),
                        'actual_subsubsection_id': self._versioned_id(subsubsection_id, version_id),
                        'original_id': subsubsection_id,
                        'number': subsubsec['number'],
                        'title': subsubsec['title'],
                    })

                result = session.run(
                    """
                    UNWIND $subsubsections AS subsubsec
                    MERGE (ss:Subsection {id: subsubsec.actual_subsection_id})
                    MERGE (sss:Subsubsection {id: subsubsec.actual_subsubsection_id})
                    SET sss.number = subsubsec.number, sss.title = subsubsec.title, sss.original_id = subsubsec.original_id
                    MERGE (ss)-[:HAS_SUBSUBSECTION]->(sss)
                    RETURN count(sss) as count
                    """,
                    subsubsections=subsubsection_data,
                )
                counts['subsubsections'] = result.single()['count']

        return counts

    def batch_upsert_paragraphs(
        self,
        paragraphs: List[Dict[str, Any]],
        version_id: Optional[str] = None,
        batch_size: int = 500,
    ) -> int:
        """Batch upsert paragraph nodes with their parent relationships.

        Uses UNWIND for efficient batch operations, processing in configurable batch sizes
        to avoid memory issues with large datasets. Creates paragraphs first, then
        links to parents in separate optimized queries by parent type.

        Args:
            paragraphs: List of paragraph dicts with keys:
                - parent_id: Parent node ID (section/subsection/subsubsection)
                - paragraph_number: Paragraph number
                - text: Paragraph text content
                - chunk_id: Unique chunk identifier
                - page: Page number
                - bounds: Bounding box [x1, y1, x2, y2]
                - cross_references: Optional list of cross-references (will be JSON serialized)
            version_id: Optional version identifier for multi-version support
            batch_size: Number of paragraphs to process per batch (default: 500)

        Returns:
            int: Total number of paragraphs upserted

        Example:
            >>> paragraphs = [
            ...     {'parent_id': 'bailey:ch60:s2', 'paragraph_number': '2.1',
            ...      'text': 'Lorem ipsum...', 'chunk_id': 'bailey:ch60:2.1',
            ...      'page': 1025, 'bounds': [100, 200, 500, 250], 'cross_references': []}
            ... ]
            >>> count = storage.batch_upsert_paragraphs(paragraphs, version_id='staging_123')
            >>> print(f'Upserted {count} paragraphs')
        """
        if not paragraphs:
            return 0

        total_upserted = 0

        with self.driver.session() as session:
            # Process in batches to avoid memory issues
            for i in range(0, len(paragraphs), batch_size):
                batch = paragraphs[i:i + batch_size]

                # Prepare batch data with versioned IDs and JSON-serialized cross_references
                # Also categorize by parent type for efficient linking
                batch_data = []
                section_links = []
                subsection_links = []
                subsubsection_links = []

                for p in batch:
                    cross_refs = p.get('cross_references', [])
                    cross_refs_json = json.dumps(cross_refs) if cross_refs else '[]'
                    actual_parent_id = self._versioned_id(p['parent_id'], version_id)
                    actual_chunk_id = self._versioned_id(p['chunk_id'], version_id)

                    batch_data.append({
                        'actual_chunk_id': actual_chunk_id,
                        'original_chunk_id': p['chunk_id'],
                        'paragraph_number': p['paragraph_number'],
                        'text': p['text'],
                        'page': p['page'],
                        'bounds': p['bounds'],
                        'cross_references': cross_refs_json,
                    })

                    # Categorize parent link by type (based on ID pattern)
                    link_data = {'parent_id': actual_parent_id, 'chunk_id': actual_chunk_id}
                    if ':sss' in p['parent_id']:
                        subsubsection_links.append(link_data)
                    elif ':ss' in p['parent_id']:
                        subsection_links.append(link_data)
                    else:
                        section_links.append(link_data)

                # Step 1: Create all paragraph nodes (fast, no parent lookup)
                result = session.run(
                    """
                    UNWIND $paragraphs AS p
                    MERGE (para:Paragraph {chunk_id: p.actual_chunk_id})
                    SET para.number = p.paragraph_number,
                        para.text = p.text,
                        para.page = p.page,
                        para.bounds = p.bounds,
                        para.cross_references = p.cross_references,
                        para.original_chunk_id = p.original_chunk_id
                    RETURN count(para) as count
                    """,
                    paragraphs=batch_data,
                )
                total_upserted += result.single()['count']

                # Step 2: Link to Section parents (with label for index use)
                if section_links:
                    session.run(
                        """
                        UNWIND $links AS link
                        MATCH (parent:Section {id: link.parent_id})
                        MATCH (para:Paragraph {chunk_id: link.chunk_id})
                        MERGE (parent)-[:HAS_PARAGRAPH]->(para)
                        """,
                        links=section_links,
                    )

                # Step 3: Link to Subsection parents
                if subsection_links:
                    session.run(
                        """
                        UNWIND $links AS link
                        MATCH (parent:Subsection {id: link.parent_id})
                        MATCH (para:Paragraph {chunk_id: link.chunk_id})
                        MERGE (parent)-[:HAS_PARAGRAPH]->(para)
                        """,
                        links=subsection_links,
                    )

                # Step 4: Link to Subsubsection parents
                if subsubsection_links:
                    session.run(
                        """
                        UNWIND $links AS link
                        MATCH (parent:Subsubsection {id: link.parent_id})
                        MATCH (para:Paragraph {chunk_id: link.chunk_id})
                        MERGE (parent)-[:HAS_PARAGRAPH]->(para)
                        """,
                        links=subsubsection_links,
                    )

        return total_upserted

    def batch_upsert_tables(
        self,
        tables: List[Dict[str, Any]],
        version_id: Optional[str] = None,
    ) -> int:
        """Batch upsert table nodes linked to paragraphs.

        Args:
            tables: List of table dicts with keys:
                - paragraph_chunk_id: Parent paragraph chunk ID
                - table_number: Table number
                - description: Table description
                - page: Page number
                - bounds: Bounding box [x1, y1, x2, y2]
                - file_png: Optional PNG file path
                - file_xlsx: Optional Excel file path
            version_id: Optional version identifier for multi-version support

        Returns:
            int: Number of tables upserted
        """
        if not tables:
            return 0

        with self.driver.session() as session:
            table_data = []
            for t in tables:
                table_data.append({
                    'actual_paragraph_chunk_id': self._versioned_id(t['paragraph_chunk_id'], version_id),
                    'table_number': t['table_number'],
                    'description': t['description'],
                    'page': t['page'],
                    'bounds': t['bounds'],
                    'file_png': t.get('file_png', ''),
                    'file_xlsx': t.get('file_xlsx', ''),
                })

            result = session.run(
                """
                UNWIND $tables AS t
                MATCH (p:Paragraph {chunk_id: t.actual_paragraph_chunk_id})
                MERGE (tbl:Table {paragraph_id: t.actual_paragraph_chunk_id, table_number: t.table_number})
                SET tbl.file_png = t.file_png,
                    tbl.file_xlsx = t.file_xlsx,
                    tbl.description = t.description,
                    tbl.page = t.page,
                    tbl.bounds = t.bounds
                MERGE (p)-[:CONTAINS_TABLE]->(tbl)
                RETURN count(tbl) as count
                """,
                tables=table_data,
            )
            return result.single()['count']

    def batch_upsert_figures(
        self,
        figures: List[Dict[str, Any]],
        version_id: Optional[str] = None,
    ) -> int:
        """Batch upsert figure nodes linked to paragraphs.

        Args:
            figures: List of figure dicts with keys:
                - paragraph_chunk_id: Parent paragraph chunk ID
                - figure_number: Figure number
                - caption: Figure caption
                - page: Page number
                - bounds: Bounding box [x1, y1, x2, y2]
                - file_png: Optional PNG file path
            version_id: Optional version identifier for multi-version support

        Returns:
            int: Number of figures upserted
        """
        if not figures:
            return 0

        with self.driver.session() as session:
            figure_data = []
            for f in figures:
                figure_data.append({
                    'actual_paragraph_chunk_id': self._versioned_id(f['paragraph_chunk_id'], version_id),
                    'figure_number': f['figure_number'],
                    'caption': f['caption'],
                    'page': f['page'],
                    'bounds': f['bounds'],
                    'file_png': f.get('file_png', ''),
                })

            result = session.run(
                """
                UNWIND $figures AS f
                MATCH (p:Paragraph {chunk_id: f.actual_paragraph_chunk_id})
                MERGE (fig:Figure {paragraph_id: f.actual_paragraph_chunk_id, figure_number: f.figure_number})
                SET fig.file_png = f.file_png,
                    fig.caption = f.caption,
                    fig.page = f.page,
                    fig.bounds = f.bounds
                MERGE (p)-[:CONTAINS_FIGURE]->(fig)
                RETURN count(fig) as count
                """,
                figures=figure_data,
            )
            return result.single()['count']

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        self.driver.close()
