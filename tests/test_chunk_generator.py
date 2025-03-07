"""Tests for chunk generator with hierarchical paragraph extraction."""

import re

import pytest

from hybridflow.models import (
    Bounds,
    Chapter,
    Paragraph,
    Section,
    Subsection,
    Subsubsection,
    TextbookEnum,
)
from hybridflow.parsing.chunk_generator import ChunkGenerator


@pytest.fixture
def sample_chapter():
    """Create a sample chapter with hierarchical structure."""
    # Create paragraphs for different levels
    p1 = Paragraph(
        number="2.1.1",
        text="Direct paragraph in section",
        page=10,
        bounds=Bounds(x1=10.0, y1=10.0, x2=100.0, y2=20.0),
    )
    p2 = Paragraph(
        number="2.1.2",
        text="Another direct paragraph",
        page=10,
        bounds=Bounds(x1=10.0, y1=25.0, x2=100.0, y2=35.0),
    )
    p3 = Paragraph(
        number="2.2.1.1",
        text="Paragraph in subsection",
        page=11,
        bounds=Bounds(x1=10.0, y1=10.0, x2=100.0, y2=20.0),
    )
    p4 = Paragraph(
        number="2.2.2.1.1",
        text="Paragraph in subsubsection",
        page=12,
        bounds=Bounds(x1=10.0, y1=10.0, x2=100.0, y2=20.0),
    )

    # Create subsubsection
    subsubsection = Subsubsection(
        title="Deep Topic",
        number="2.2.2.1",
        paragraphs=[p4],
    )

    # Create subsections
    subsection1 = Subsection(
        title="Subtopic A",
        number="2.2.1",
        paragraphs=[p3],
        subsubsections=[],
    )
    subsection2 = Subsection(
        title="Subtopic B",
        number="2.2.2",
        paragraphs=[],
        subsubsections=[subsubsection],
    )

    # Create sections
    section1 = Section(
        title="Introduction",
        number="2.1",
        paragraphs=[p1, p2],
        subsections=[],
    )
    section2 = Section(
        title="Methods",
        number="2.2",
        paragraphs=[],
        subsections=[subsection1, subsection2],
    )

    # Create chapter
    chapter = Chapter(
        chapter_number="2",
        title="Shock and Blood Transfusion",
        sections=[section1, section2],
        textbook_id=TextbookEnum.BAILEY,
        source_file_path="/path/to/bailey/chapter_2.json",
    )

    return chapter


def test_generate_chunk_id():
    """Test chunk ID generation with correct format."""
    generator = ChunkGenerator()
    result = generator.generate_chunk_id("bailey", "2", "2.1.1")
    assert result == "bailey:ch2:2.1.1"


def test_extract_paragraphs_flat_section():
    """Test extracting paragraphs from flat section without subsections."""
    generator = ChunkGenerator()

    # Create flat section with only direct paragraphs
    p1 = Paragraph(
        number="1.1",
        text="First paragraph",
        page=5,
        bounds=Bounds(x1=10.0, y1=10.0, x2=100.0, y2=20.0),
    )
    p2 = Paragraph(
        number="1.2",
        text="Second paragraph",
        page=5,
        bounds=Bounds(x1=10.0, y1=25.0, x2=100.0, y2=35.0),
    )

    section = Section(
        title="Overview",
        number="1",
        paragraphs=[p1, p2],
        subsections=[],
    )

    # Extract paragraphs
    results = list(
        generator.extract_paragraphs_from_section(section, "bailey:ch1", ["Chapter Title"])
    )

    assert len(results) == 2
    assert results[0][2] == ["Chapter Title", "Overview"]
    assert results[1][2] == ["Chapter Title", "Overview"]


def test_extract_paragraphs_with_subsection():
    """Test extracting paragraphs from section with subsection."""
    generator = ChunkGenerator()

    # Create paragraph in subsection
    p1 = Paragraph(
        number="2.1.1",
        text="Subsection paragraph",
        page=10,
        bounds=Bounds(x1=10.0, y1=10.0, x2=100.0, y2=20.0),
    )

    subsection = Subsection(
        title="Details",
        number="2.1",
        paragraphs=[p1],
        subsubsections=[],
    )

    section = Section(
        title="Main Topic",
        number="2",
        paragraphs=[],
        subsections=[subsection],
    )

    # Extract paragraphs
    results = list(
        generator.extract_paragraphs_from_section(section, "bailey:ch2", ["Chapter Title"])
    )

    assert len(results) == 1
    assert results[0][2] == ["Chapter Title", "Main Topic", "Details"]


def test_extract_paragraphs_with_subsubsection():
    """Test extracting paragraphs from section with subsubsection."""
    generator = ChunkGenerator()

    # Create paragraph in subsubsection
    p1 = Paragraph(
        number="3.1.1.1",
        text="Deep paragraph",
        page=15,
        bounds=Bounds(x1=10.0, y1=10.0, x2=100.0, y2=20.0),
    )

    subsubsection = Subsubsection(
        title="Specific Detail",
        number="3.1.1",
        paragraphs=[p1],
    )

    subsection = Subsection(
        title="Subtopic",
        number="3.1",
        paragraphs=[],
        subsubsections=[subsubsection],
    )

    section = Section(
        title="Advanced Topic",
        number="3",
        paragraphs=[],
        subsections=[subsection],
    )

    # Extract paragraphs
    results = list(
        generator.extract_paragraphs_from_section(section, "bailey:ch3", ["Chapter Title"])
    )

    assert len(results) == 1
    assert len(results[0][2]) == 4
    assert results[0][2] == [
        "Chapter Title",
        "Advanced Topic",
        "Subtopic",
        "Specific Detail",
    ]


def test_generate_chunks_full_chapter(sample_chapter):
    """Test generating all chunks from complete chapter hierarchy."""
    generator = ChunkGenerator()
    chunks = generator.generate_chunks(sample_chapter)

    # Should have 4 total paragraphs (2 direct + 1 subsection + 1 subsubsection)
    assert len(chunks) == 4

    # Extract all chunk IDs
    chunk_ids = [chunk[0] for chunk in chunks]

    # All chunk IDs should be unique
    assert len(chunk_ids) == len(set(chunk_ids))


def test_chunk_id_format(sample_chapter):
    """Test that all chunk IDs match expected format."""
    generator = ChunkGenerator()
    chunks = generator.generate_chunks(sample_chapter)

    # Pattern: textbook:chNumber:paragraphNumber
    pattern = re.compile(r"^[a-z]+:ch\d+:[\d.]+$")

    for chunk_id, paragraph, hierarchy_path in chunks:
        assert pattern.match(chunk_id), f"Invalid chunk ID format: {chunk_id}"
        assert chunk_id.startswith("bailey:ch2:")
