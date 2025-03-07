"""Tests for Neo4j graph storage client."""

import pytest

from hybridflow.storage.neo4j_client import Neo4jStorage


@pytest.fixture
def neo4j_storage():
    """Create a Neo4j storage instance for testing."""
    storage = Neo4jStorage(uri="bolt://localhost:7687", user="neo4j", password="password")
    storage.create_constraints()
    yield storage

    # Teardown: Clear the database
    with storage.driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    storage.close()


def test_create_constraints(neo4j_storage):
    """Test that uniqueness constraints are created successfully."""
    query = "SHOW CONSTRAINTS"

    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        constraints = [record for record in result]

        # Check that we have constraints
        assert len(constraints) > 0

        # Extract constraint names/types
        constraint_info = [str(record) for record in constraints]
        constraint_str = " ".join(constraint_info)

        # Verify key constraints exist
        assert "Textbook" in constraint_str or "textbook" in constraint_str.lower()
        assert "Chapter" in constraint_str or "chapter" in constraint_str.lower()
        assert "Paragraph" in constraint_str or "paragraph" in constraint_str.lower()


def test_upsert_textbook(neo4j_storage):
    """Test upserting a textbook node."""
    textbook_id = "bailey"
    name = "Bailey & Love's Short Practice of Surgery"

    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name=name)

    # Verify textbook was created
    query = 'MATCH (t:Textbook {id: $textbook_id}) RETURN t.name as name'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, textbook_id=textbook_id)
        record = result.single()

        assert record is not None
        assert record["name"] == name


def test_upsert_chapter(neo4j_storage):
    """Test upserting a chapter node."""
    textbook_id = "bailey"
    chapter_number = "2"
    title = "Shock"
    version = 1

    # Create textbook first
    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")

    # Create chapter
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id,
        chapter_number=chapter_number,
        title=title,
        version=version,
    )

    # Verify chapter was created
    chapter_id = f"{textbook_id}:ch{chapter_number}"
    query = 'MATCH (c:Chapter {id: $chapter_id}) RETURN c'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, chapter_id=chapter_id)
        record = result.single()

        assert record is not None
        assert record["c"]["id"] == chapter_id
        assert record["c"]["title"] == title


def test_chapter_textbook_relationship(neo4j_storage):
    """Test that chapter-textbook relationship is created."""
    textbook_id = "bailey"
    chapter_number = "2"

    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id,
        chapter_number=chapter_number,
        title="Shock",
        version=1,
    )

    # Verify relationship exists
    query = 'MATCH (t:Textbook)-[:CONTAINS]->(c:Chapter) RETURN count(c) as count'
    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        record = result.single()

        assert record is not None
        assert record["count"] == 1


def test_upsert_section(neo4j_storage):
    """Test upserting a section node."""
    textbook_id = "bailey"
    chapter_number = "2"
    section_number = "2.1"
    section_title = "Pathophysiology"

    # Create textbook and chapter first
    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id,
        chapter_number=chapter_number,
        title="Shock",
        version=1,
    )

    # Create section
    chapter_id = f"{textbook_id}:ch{chapter_number}"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id,
        section_number=section_number,
        title=section_title,
    )

    # Verify section was created
    section_id = f"{chapter_id}:s{section_number}"
    query = 'MATCH (s:Section {id: $section_id}) RETURN s'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, section_id=section_id)
        record = result.single()

        assert record is not None
        assert record["s"]["title"] == section_title


def test_upsert_subsection(neo4j_storage):
    """Test upserting a subsection node."""
    textbook_id = "bailey"
    chapter_number = "2"
    section_number = "2.1"
    subsection_number = "2.1.1"
    subsection_title = "Cellular"

    # Create hierarchy
    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")
    chapter_id = f"{textbook_id}:ch{chapter_number}"
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id, chapter_number=chapter_number, title="Shock", version=1
    )
    section_id = f"{chapter_id}:s{section_number}"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number=section_number, title="Patho"
    )

    # Create subsection
    neo4j_storage.upsert_subsection(
        section_id=section_id,
        subsection_number=subsection_number,
        title=subsection_title,
    )

    # Verify subsection and relationship
    query = 'MATCH (s:Section)-[:HAS_SUBSECTION]->(ss:Subsection) RETURN ss'
    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        record = result.single()

        assert record is not None
        assert record["ss"]["title"] == subsection_title


def test_upsert_subsubsection(neo4j_storage):
    """Test upserting a subsubsection node."""
    textbook_id = "bailey"
    chapter_number = "2"
    section_number = "2.1"
    subsection_number = "2.1.1"
    subsubsection_number = "2.1.1.1"
    subsubsection_title = "Mitochondrial"

    # Create hierarchy
    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")
    chapter_id = f"{textbook_id}:ch{chapter_number}"
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id, chapter_number=chapter_number, title="Shock", version=1
    )
    section_id = f"{chapter_id}:s{section_number}"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number=section_number, title="Patho"
    )
    subsection_id = f"{section_id}:ss{subsection_number}"
    neo4j_storage.upsert_subsection(
        section_id=section_id, subsection_number=subsection_number, title="Cellular"
    )

    # Create subsubsection
    neo4j_storage.upsert_subsubsection(
        subsection_id=subsection_id,
        subsubsection_number=subsubsection_number,
        title=subsubsection_title,
    )

    # Verify subsubsection and relationship
    query = 'MATCH (ss:Subsection)-[:HAS_SUBSUBSECTION]->(sss:Subsubsection) RETURN sss'
    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        record = result.single()

        assert record is not None
        assert record["sss"]["title"] == subsubsection_title


def test_upsert_paragraph(neo4j_storage):
    """Test upserting a paragraph node."""
    textbook_id = "bailey"
    chapter_number = "2"
    section_number = "2.1"
    chunk_id = "bailey:ch2:2.1.1"
    text = "Sample medical text about shock pathophysiology."
    page = 42
    bounds = [10.0, 20.0, 100.0, 200.0]

    # Create hierarchy
    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")
    chapter_id = f"{textbook_id}:ch{chapter_number}"
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id, chapter_number=chapter_number, title="Shock", version=1
    )
    section_id = f"{chapter_id}:s{section_number}"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number=section_number, title="Patho"
    )

    # Create paragraph
    neo4j_storage.upsert_paragraph(
        parent_id=section_id,
        paragraph_number="1",
        text=text,
        chunk_id=chunk_id,
        page=page,
        bounds=bounds,
    )

    # Verify paragraph
    query = 'MATCH (p:Paragraph {chunk_id: $chunk_id}) RETURN p'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, chunk_id=chunk_id)
        record = result.single()

        assert record is not None
        assert record["p"]["text"] == text
        assert record["p"]["chunk_id"] == chunk_id


def test_upsert_table(neo4j_storage):
    """Test upserting a table node."""
    # Create paragraph first
    textbook_id = "bailey"
    chapter_number = "2"
    section_number = "2.1"
    chunk_id = "bailey:ch2:2.1.1"

    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")
    chapter_id = f"{textbook_id}:ch{chapter_number}"
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id, chapter_number=chapter_number, title="Shock", version=1
    )
    section_id = f"{chapter_id}:s{section_number}"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number=section_number, title="Patho"
    )
    neo4j_storage.upsert_paragraph(
        parent_id=section_id,
        paragraph_number="1",
        text="Text",
        chunk_id=chunk_id,
        page=42,
        bounds=[10.0, 20.0, 100.0, 200.0],
    )

    # Create table
    neo4j_storage.upsert_table(
        paragraph_chunk_id=chunk_id,
        table_number="2.1",
        file_png="table_2_1.png",
        file_xlsx="table_2_1.xlsx",
        description="Classification of shock",
        page=42,
    )

    # Verify table
    query = 'MATCH (p:Paragraph)-[:CONTAINS_TABLE]->(t:Table) RETURN t'
    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        record = result.single()

        assert record is not None
        assert record["t"]["table_number"] == "2.1"


def test_upsert_figure(neo4j_storage):
    """Test upserting a figure node."""
    # Create paragraph first
    textbook_id = "bailey"
    chapter_number = "2"
    section_number = "2.1"
    chunk_id = "bailey:ch2:2.1.1"

    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")
    chapter_id = f"{textbook_id}:ch{chapter_number}"
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id, chapter_number=chapter_number, title="Shock", version=1
    )
    section_id = f"{chapter_id}:s{section_number}"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number=section_number, title="Patho"
    )
    neo4j_storage.upsert_paragraph(
        parent_id=section_id,
        paragraph_number="1",
        text="Text",
        chunk_id=chunk_id,
        page=42,
        bounds=[10.0, 20.0, 100.0, 200.0],
    )

    # Create figure
    neo4j_storage.upsert_figure(
        paragraph_chunk_id=chunk_id,
        figure_number="2.1",
        file_png="figure_2_1.png",
        caption="Shock cascade diagram",
        page=42,
    )

    # Verify figure
    query = 'MATCH (p:Paragraph)-[:CONTAINS_FIGURE]->(f:Figure) RETURN f'
    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        record = result.single()

        assert record is not None
        assert record["f"]["figure_number"] == "2.1"


def test_full_hierarchy(neo4j_storage):
    """Test creating and querying a full hierarchy."""
    textbook_id = "bailey"
    chapter_number = "2"

    # Create full hierarchy
    neo4j_storage.upsert_textbook(textbook_id=textbook_id, name="Bailey")

    chapter_id = f"{textbook_id}:ch{chapter_number}"
    neo4j_storage.upsert_chapter(
        textbook_id=textbook_id, chapter_number=chapter_number, title="Shock", version=1
    )

    section_id = f"{chapter_id}:s2.1"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number="2.1", title="Patho"
    )

    subsection_id = f"{section_id}:ss2.1.1"
    neo4j_storage.upsert_subsection(
        section_id=section_id, subsection_number="2.1.1", title="Cellular"
    )

    subsubsection_id = f"{subsection_id}:sss2.1.1.1"
    neo4j_storage.upsert_subsubsection(
        subsection_id=subsection_id,
        subsubsection_number="2.1.1.1",
        title="Mitochondrial",
    )

    neo4j_storage.upsert_paragraph(
        parent_id=subsubsection_id,
        paragraph_number="1",
        text="Sample text",
        chunk_id="bailey:ch2:para1",
        page=42,
        bounds=[10.0, 20.0, 100.0, 200.0],
    )

    # Query full path
    query = """
    MATCH path = (t:Textbook)-[:CONTAINS]->(c:Chapter)
                 -[:HAS_SECTION]->(s:Section)
                 -[:HAS_SUBSECTION]->(ss:Subsection)
                 -[:HAS_SUBSUBSECTION]->(sss:Subsubsection)
                 -[:HAS_PARAGRAPH]->(p:Paragraph)
    RETURN length(path) as path_length
    """
    with neo4j_storage.driver.session() as session:
        result = session.run(query)
        record = result.single()

        assert record is not None
        assert record["path_length"] == 5  # 6 nodes, 5 relationships
