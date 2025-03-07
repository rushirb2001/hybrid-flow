"""Neo4j graph database client for hierarchical data storage."""

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
        MATCH (t:Textbook {id: $textbook_id})
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
        MATCH (c:Chapter {id: $chapter_id})
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
        MATCH (s:Section {id: $section_id})
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
        MATCH (ss:Subsection {id: $subsection_id})
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
    ) -> None:
        """Insert or update a paragraph node and link to parent.

        Args:
            parent_id: Parent node identifier (section/subsection/subsubsection)
            paragraph_number: Paragraph number
            text: Paragraph text content
            chunk_id: Unique chunk identifier
            page: Page number
            bounds: Bounding box coordinates [x1, y1, x2, y2]
        """
        query = """
        MATCH (parent)
        WHERE parent.id = $parent_id
        MERGE (p:Paragraph {chunk_id: $chunk_id})
        SET p.number = $paragraph_number, p.text = $text, p.page = $page, p.bounds = $bounds
        MERGE (parent)-[:HAS_PARAGRAPH]->(p)
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
            )

    def upsert_table(
        self,
        paragraph_chunk_id: str,
        table_number: str,
        file_png: str,
        file_xlsx: str,
        description: str,
        page: int,
    ) -> None:
        """Insert or update a table node and link to paragraph.

        Args:
            paragraph_chunk_id: Parent paragraph chunk identifier
            table_number: Table number
            file_png: PNG file path
            file_xlsx: Excel file path
            description: Table description
            page: Page number
        """
        query = """
        MATCH (p:Paragraph {chunk_id: $paragraph_chunk_id})
        MERGE (t:Table {paragraph_id: $paragraph_chunk_id, table_number: $table_number})
        SET t.file_png = $file_png, t.file_xlsx = $file_xlsx,
            t.description = $description, t.page = $page
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
            )

    def upsert_figure(
        self,
        paragraph_chunk_id: str,
        figure_number: str,
        file_png: str,
        caption: str,
        page: int,
    ) -> None:
        """Insert or update a figure node and link to paragraph.

        Args:
            paragraph_chunk_id: Parent paragraph chunk identifier
            figure_number: Figure number
            file_png: PNG file path
            caption: Figure caption
            page: Page number
        """
        query = """
        MATCH (p:Paragraph {chunk_id: $paragraph_chunk_id})
        MERGE (f:Figure {paragraph_id: $paragraph_chunk_id, figure_number: $figure_number})
        SET f.file_png = $file_png, f.caption = $caption, f.page = $page
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

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        self.driver.close()
