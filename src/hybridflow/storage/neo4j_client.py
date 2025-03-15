"""Neo4j graph database client for hierarchical data storage."""

import json
from typing import Dict, List

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

    def create_constraints(self) -> None:
        """Create uniqueness constraints for key node types."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Textbook) REQUIRE t.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chapter) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paragraph) REQUIRE p.chunk_id IS UNIQUE",
        ]

        with self.driver.session() as session:
            for constraint in constraints:
                session.run(constraint)

    def upsert_textbook(self, textbook_id: str, name: str) -> None:
        """Insert or update a textbook node.

        Args:
            textbook_id: Unique textbook identifier
            name: Full name of the textbook
        """
        query = """
        MERGE (t:Textbook {id: $textbook_id})
        SET t.name = $name
        """
        with self.driver.session() as session:
            session.run(query, textbook_id=textbook_id, name=name)

    def upsert_chapter(
        self, textbook_id: str, chapter_number: str, title: str, version: int
    ) -> None:
        """Insert or update a chapter node and link to textbook.

        Args:
            textbook_id: Parent textbook identifier
            chapter_number: Chapter number
            title: Chapter title
            version: Version number
        """
        chapter_id = f"{textbook_id}:ch{chapter_number}"
        query = """
        MERGE (t:Textbook {id: $textbook_id})
        MERGE (c:Chapter {id: $chapter_id})
        SET c.number = $chapter_number, c.title = $title, c.version = $version
        MERGE (t)-[:CONTAINS]->(c)
        """
        with self.driver.session() as session:
            session.run(
                query,
                textbook_id=textbook_id,
                chapter_id=chapter_id,
                chapter_number=chapter_number,
                title=title,
                version=version,
            )

    def upsert_section(
        self, chapter_id: str, section_number: str, title: str
    ) -> None:
        """Insert or update a section node and link to chapter.

        Args:
            chapter_id: Parent chapter identifier
            section_number: Section number
            title: Section title
        """
        section_id = f"{chapter_id}:s{section_number}"
        query = """
        MERGE (c:Chapter {id: $chapter_id})
        MERGE (s:Section {id: $section_id})
        SET s.number = $section_number, s.title = $title
        MERGE (c)-[:HAS_SECTION]->(s)
        """
        with self.driver.session() as session:
            session.run(
                query,
                chapter_id=chapter_id,
                section_id=section_id,
                section_number=section_number,
                title=title,
            )

    def upsert_subsection(
        self, section_id: str, subsection_number: str, title: str
    ) -> None:
        """Insert or update a subsection node and link to section.

        Args:
            section_id: Parent section identifier
            subsection_number: Subsection number
            title: Subsection title
        """
        subsection_id = f"{section_id}:ss{subsection_number}"
        query = """
        MERGE (s:Section {id: $section_id})
        MERGE (ss:Subsection {id: $subsection_id})
        SET ss.number = $subsection_number, ss.title = $title
        MERGE (s)-[:HAS_SUBSECTION]->(ss)
        """
        with self.driver.session() as session:
            session.run(
                query,
                section_id=section_id,
                subsection_id=subsection_id,
                subsection_number=subsection_number,
                title=title,
            )

    def upsert_subsubsection(
        self, subsection_id: str, subsubsection_number: str, title: str
    ) -> None:
        """Insert or update a subsubsection node and link to subsection.

        Args:
            subsection_id: Parent subsection identifier
            subsubsection_number: Subsubsection number
            title: Subsubsection title
        """
        subsubsection_id = f"{subsection_id}:sss{subsubsection_number}"
        query = """
        MERGE (ss:Subsection {id: $subsection_id})
        MERGE (sss:Subsubsection {id: $subsubsection_id})
        SET sss.number = $subsubsection_number, sss.title = $title
        MERGE (ss)-[:HAS_SUBSUBSECTION]->(sss)
        """
        with self.driver.session() as session:
            session.run(
                query,
                subsection_id=subsection_id,
                subsubsection_id=subsubsection_id,
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
    ) -> None:
        """Insert or update a paragraph node and link to parent.

        Args:
            parent_id: Parent node identifier (section/subsection/subsubsection)
            paragraph_number: Paragraph number
            text: Paragraph text content
            chunk_id: Unique chunk identifier
            page: Page number
            bounds: Bounding box coordinates [x1, y1, x2, y2]
            cross_references: List of cross-references to figures/tables (optional)
                Format: [{"type": "figure", "number": "60.5"}, ...]
        """
        if cross_references is None:
            cross_references = []

        # Serialize cross_references to JSON string for Neo4j storage
        # Neo4j doesn't support lists of maps, only primitives or arrays of primitives
        cross_references_json = json.dumps(cross_references)

        query = """
        MERGE (p:Paragraph {chunk_id: $chunk_id})
        SET p.number = $paragraph_number, p.text = $text, p.page = $page, p.bounds = $bounds,
            p.cross_references = $cross_references
        WITH p
        OPTIONAL MATCH (parent)
        WHERE parent.id = $parent_id
        FOREACH (ignored IN CASE WHEN parent IS NOT NULL THEN [1] ELSE [] END |
            MERGE (parent)-[:HAS_PARAGRAPH]->(p)
        )
        """
        with self.driver.session() as session:
            session.run(
                query,
                parent_id=parent_id,
                paragraph_number=paragraph_number,
                text=text,
                chunk_id=chunk_id,
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
    ) -> None:
        """Insert or update a table node and link to paragraph.

        Args:
            paragraph_chunk_id: Parent paragraph chunk identifier
            table_number: Table number
            description: Table description
            page: Page number
            bounds: Bounding box coordinates [x1, y1, x2, y2]
            file_png: PNG file path (optional)
            file_xlsx: Excel file path (optional)
        """
        query = """
        MATCH (p:Paragraph {chunk_id: $paragraph_chunk_id})
        MERGE (t:Table {paragraph_id: $paragraph_chunk_id, table_number: $table_number})
        SET t.file_png = $file_png, t.file_xlsx = $file_xlsx,
            t.description = $description, t.page = $page, t.bounds = $bounds
        MERGE (p)-[:CONTAINS_TABLE]->(t)
        """
        with self.driver.session() as session:
            session.run(
                query,
                paragraph_chunk_id=paragraph_chunk_id,
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
    ) -> None:
        """Insert or update a figure node and link to paragraph.

        Args:
            paragraph_chunk_id: Parent paragraph chunk identifier
            figure_number: Figure number
            caption: Figure caption
            page: Page number
            bounds: Bounding box coordinates [x1, y1, x2, y2]
            file_png: PNG file path (optional)
        """
        query = """
        MATCH (p:Paragraph {chunk_id: $paragraph_chunk_id})
        MERGE (f:Figure {paragraph_id: $paragraph_chunk_id, figure_number: $figure_number})
        SET f.file_png = $file_png, f.caption = $caption, f.page = $page, f.bounds = $bounds
        MERGE (p)-[:CONTAINS_FIGURE]->(f)
        """
        with self.driver.session() as session:
            session.run(
                query,
                paragraph_chunk_id=paragraph_chunk_id,
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

    def link_sequential_paragraphs(self, chapter_id: str) -> int:
        """Create NEXT/PREV relationships between paragraphs in reading order.

        Links paragraphs within the same parent (section/subsection/subsubsection)
        in sequential order based on their paragraph numbers.

        Args:
            chapter_id: Chapter identifier to link paragraphs within

        Returns:
            Number of NEXT relationships created
        """
        query = """
        MATCH (c:Chapter {id: $chapter_id})
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
            result = session.run(query, chapter_id=chapter_id)
            record = result.single()
            return record["links_created"] if record else 0

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        self.driver.close()
