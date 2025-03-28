"""Tests for Neo4j graph storage client."""

import pytest

from hybridflow.storage.neo4j_client import Neo4jStorage


@pytest.fixture
def neo4j_storage():
    """Create a Neo4j storage instance for testing."""
    storage = Neo4jStorage(uri="bolt://localhost:7687", user="neo4j", password="password")
    storage.create_constraints()
    yield storage

    # No cleanup - preserve production data
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

    # Verify relationship exists for this specific chapter
    chapter_id = f"{textbook_id}:ch{chapter_number}"
    query = 'MATCH (t:Textbook {id: $textbook_id})-[:CONTAINS]->(c:Chapter {id: $chapter_id}) RETURN c'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, textbook_id=textbook_id, chapter_id=chapter_id)
        record = result.single()

        assert record is not None
        assert record["c"]["id"] == chapter_id


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

    # Verify subsection and relationship for this specific subsection
    subsection_id = f"{section_id}:ss{subsection_number}"
    query = 'MATCH (s:Section {id: $section_id})-[:HAS_SUBSECTION]->(ss:Subsection {id: $subsection_id}) RETURN ss'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, section_id=section_id, subsection_id=subsection_id)
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

    # Verify subsubsection and relationship for this specific subsubsection
    subsubsection_id = f"{subsection_id}:sss{subsubsection_number}"
    query = 'MATCH (ss:Subsection {id: $subsection_id})-[:HAS_SUBSUBSECTION]->(sss:Subsubsection {id: $subsubsection_id}) RETURN sss'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, subsection_id=subsection_id, subsubsection_id=subsubsection_id)
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
        bounds=[50.0, 300.0, 400.0, 500.0],
    )

    # Verify table for this specific paragraph
    query = 'MATCH (p:Paragraph {chunk_id: $chunk_id})-[:CONTAINS_TABLE]->(t:Table) RETURN t'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, chunk_id=chunk_id)
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
        bounds=[50.0, 550.0, 400.0, 750.0],
    )

    # Verify figure for this specific paragraph
    query = 'MATCH (p:Paragraph {chunk_id: $chunk_id})-[:CONTAINS_FIGURE]->(f:Figure) RETURN f'
    with neo4j_storage.driver.session() as session:
        result = session.run(query, chunk_id=chunk_id)
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

    # Query full path for the specific test paragraph
    chunk_id = "bailey:ch2:para1"
    query = """
    MATCH path = (t:Textbook)-[:CONTAINS]->(c:Chapter)
                 -[:HAS_SECTION]->(s:Section)
                 -[:HAS_SUBSECTION]->(ss:Subsection)
                 -[:HAS_SUBSUBSECTION]->(sss:Subsubsection)
                 -[:HAS_PARAGRAPH]->(p:Paragraph {chunk_id: $chunk_id})
    RETURN length(path) as path_length
    """
    with neo4j_storage.driver.session() as session:
        result = session.run(query, chunk_id=chunk_id)
        record = result.single()

        assert record is not None
        assert record["path_length"] == 5  # 6 nodes, 5 relationships


def test_validate_graph(neo4j_storage):
    """Test validate_graph returns expected structure."""
    # Create simple hierarchy for validation
    neo4j_storage.upsert_textbook(textbook_id="test", name="Test")
    neo4j_storage.upsert_chapter(
        textbook_id="test", chapter_number="1", title="Test Chapter", version=1
    )

    report = neo4j_storage.validate_graph()

    # Check report structure
    assert "version_id" in report
    assert "node_counts" in report
    assert "relationship_counts" in report
    assert "orphan_paragraphs" in report
    assert "broken_next_chains" in report
    assert "broken_prev_chains" in report
    assert "duplicate_chunk_ids" in report
    assert "invalid_hierarchies" in report
    assert "status" in report

    # Check version_id for current graph
    assert report["version_id"] == "current"

    # Status should be valid or issues_found
    assert report["status"] in ["valid", "issues_found"]


def test_validate_graph_orphan_detection(neo4j_storage):
    """Test orphan paragraph detection."""
    # Create orphan paragraph using MERGE to avoid constraint errors
    with neo4j_storage.driver.session() as session:
        session.run(
            """
            MERGE (p:Paragraph {chunk_id: 'orphan:test:1'})
            SET p.text = 'Orphan text',
                p.number = '1',
                p.page = 1,
                p.bounds = [0, 0, 100, 100]
            """
        )

    report = neo4j_storage.validate_graph()

    # Should detect orphan
    assert report["orphan_paragraphs"] >= 1
    assert report["status"] == "issues_found"


def test_get_graph_stats(neo4j_storage):
    """Test get_graph_stats returns expected structure."""
    # Create simple data
    neo4j_storage.upsert_textbook(textbook_id="test", name="Test")
    chapter_id = "test:ch1"
    neo4j_storage.upsert_chapter(
        textbook_id="test", chapter_number="1", title="Test Chapter", version=1
    )
    section_id = f"{chapter_id}:s1"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number="1", title="Test Section"
    )
    neo4j_storage.upsert_paragraph(
        parent_id=section_id,
        paragraph_number="1",
        text="Sample paragraph text for testing statistics",
        chunk_id="test:ch1:1",
        page=1,
        bounds=[0, 0, 100, 100]
    )

    stats = neo4j_storage.get_graph_stats()

    # Should include validate_graph fields
    assert "version_id" in stats
    assert "node_counts" in stats
    assert "status" in stats

    # Should include additional stats
    assert "text_stats" in stats
    assert "top_chapters_by_paragraphs" in stats
    assert "paragraphs_with_cross_references" in stats

    # Check text_stats structure
    assert "avg" in stats["text_stats"]
    assert "min" in stats["text_stats"]
    assert "max" in stats["text_stats"]

    # Text stats should be reasonable
    assert stats["text_stats"]["avg"] > 0
    assert stats["text_stats"]["min"] > 0


def test_compare_with_qdrant_empty(neo4j_storage):
    """Test compare_with_qdrant with empty Qdrant set."""
    # Create a paragraph
    neo4j_storage.upsert_textbook(textbook_id="test", name="Test")
    chapter_id = "test:ch1"
    neo4j_storage.upsert_chapter(
        textbook_id="test", chapter_number="1", title="Test", version=1
    )
    section_id = f"{chapter_id}:s1"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number="1", title="Test"
    )
    neo4j_storage.upsert_paragraph(
        parent_id=section_id,
        paragraph_number="1",
        text="Test",
        chunk_id="test:ch1:1",
        page=1,
        bounds=[0, 0, 100, 100]
    )

    # Compare with empty Qdrant set
    comparison = neo4j_storage.compare_with_qdrant(set())

    assert comparison["neo4j_count"] >= 1
    assert comparison["qdrant_count"] == 0
    assert comparison["common_count"] == 0
    assert comparison["consistency"] == "mismatch"


def test_compare_with_qdrant_matching(neo4j_storage):
    """Test compare_with_qdrant with matching chunk_ids."""
    # Create a paragraph with unique chunk_id
    neo4j_storage.upsert_textbook(textbook_id="test", name="Test")
    chapter_id = "test:ch1"
    neo4j_storage.upsert_chapter(
        textbook_id="test", chapter_number="1", title="Test", version=1
    )
    section_id = f"{chapter_id}:s1"
    neo4j_storage.upsert_section(
        chapter_id=chapter_id, section_number="1", title="Test"
    )
    chunk_id = "test:ch1:1"
    neo4j_storage.upsert_paragraph(
        parent_id=section_id,
        paragraph_number="1",
        text="Test",
        chunk_id=chunk_id,
        page=1,
        bounds=[0, 0, 100, 100]
    )

    # Get all neo4j chunk_ids to simulate a full match scenario
    with neo4j_storage.driver.session() as session:
        result = session.run("MATCH (p:Paragraph) RETURN p.chunk_id as chunk_id")
        all_chunk_ids = {record["chunk_id"] for record in result}

    # Compare with all neo4j chunk_ids (simulating matching Qdrant)
    comparison = neo4j_storage.compare_with_qdrant(all_chunk_ids)

    assert comparison["neo4j_count"] >= 1
    assert comparison["qdrant_count"] == comparison["neo4j_count"]
    assert comparison["common_count"] == comparison["neo4j_count"]
    assert comparison["only_in_neo4j"] == 0
    assert comparison["only_in_qdrant"] == 0
    assert comparison["consistency"] == "pass"
