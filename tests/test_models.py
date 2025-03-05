"""Tests for HybridFlow Pydantic models."""

import pytest

from hybridflow.models import (
    Bounds,
    Chapter,
    Figure,
    KeyPoint,
    Paragraph,
    Reference,
    Section,
    Subsection,
    Subsubsection,
    Table,
    TextbookEnum,
)


def test_bounds_creation():
    """Test creating a Bounds object with coordinate values."""
    bounds = Bounds(x1=10.5, y1=20.3, x2=100.7, y2=200.9)

    assert bounds.x1 == 10.5
    assert bounds.y1 == 20.3
    assert bounds.x2 == 100.7
    assert bounds.y2 == 200.9


def test_table_model():
    """Test creating a Table with all required fields."""
    bounds = Bounds(x1=10.0, y1=20.0, x2=100.0, y2=200.0)
    table = Table(
        table_number="1.1",
        file_png="table_1_1.png",
        file_xlsx="table_1_1.xlsx",
        description="Sample table description",
        page=5,
        bounds=bounds
    )

    assert table.table_number == "1.1"


def test_paragraph_with_tables():
    """Test creating a Paragraph with a tables list containing one Table."""
    bounds = Bounds(x1=10.0, y1=20.0, x2=100.0, y2=200.0)
    table = Table(
        table_number="1.1",
        file_png="table_1_1.png",
        file_xlsx="table_1_1.xlsx",
        description="Sample table",
        page=5,
        bounds=bounds
    )

    paragraph = Paragraph(
        number="1",
        text="Sample paragraph text",
        page=5,
        bounds=bounds,
        tables=[table]
    )

    assert len(paragraph.tables) == 1


def test_paragraph_with_figures():
    """Test creating a Paragraph with a figures list containing one Figure."""
    bounds = Bounds(x1=10.0, y1=20.0, x2=100.0, y2=200.0)
    figure = Figure(
        figure_number="1.1",
        file_png="figure_1_1.png",
        caption="Sample figure caption",
        page=5,
        bounds=bounds
    )

    paragraph = Paragraph(
        number="1",
        text="Sample paragraph text",
        page=5,
        bounds=bounds,
        figures=[figure]
    )

    assert len(paragraph.figures) == 1


def test_subsection_hierarchy():
    """Test creating a Subsection with subsubsections containing paragraphs."""
    bounds = Bounds(x1=10.0, y1=20.0, x2=100.0, y2=200.0)
    paragraph = Paragraph(
        number="1",
        text="Sample paragraph",
        page=5,
        bounds=bounds
    )

    subsubsection = Subsubsection(
        title="Subsubsection Title",
        number="1.1.1",
        paragraphs=[paragraph]
    )

    subsection = Subsection(
        title="Subsection Title",
        number="1.1",
        paragraphs=[],
        subsubsections=[subsubsection]
    )

    assert len(subsection.subsubsections) == 1
    assert len(subsection.subsubsections[0].paragraphs) == 1


def test_section_hierarchy():
    """Test creating a Section with subsections containing subsubsections and paragraphs."""
    bounds = Bounds(x1=10.0, y1=20.0, x2=100.0, y2=200.0)

    paragraph1 = Paragraph(
        number="1",
        text="Paragraph in subsubsection",
        page=5,
        bounds=bounds
    )

    paragraph2 = Paragraph(
        number="2",
        text="Paragraph in subsection",
        page=6,
        bounds=bounds
    )

    subsubsection = Subsubsection(
        title="Subsubsection Title",
        number="1.1.1",
        paragraphs=[paragraph1]
    )

    subsection = Subsection(
        title="Subsection Title",
        number="1.1",
        paragraphs=[paragraph2],
        subsubsections=[subsubsection]
    )

    section = Section(
        title="Section Title",
        number="1",
        paragraphs=[],
        subsections=[subsection]
    )

    assert len(section.subsections) == 1
    assert len(section.subsections[0].subsubsections) == 1
    assert len(section.subsections[0].paragraphs) == 1
    assert len(section.subsections[0].subsubsections[0].paragraphs) == 1


def test_chapter_bailey():
    """Test creating a Chapter with BAILEY textbook ID."""
    chapter = Chapter(
        chapter_number="1",
        title="Introduction to Surgery",
        sections=[],
        key_points=[],
        textbook_id=TextbookEnum.BAILEY,
        source_file_path="/path/to/bailey/chapter_01.json"
    )

    assert chapter.textbook_id == TextbookEnum.BAILEY
    assert chapter.chapter_number == "1"
    assert chapter.key_points == []
    assert chapter.authors is None


def test_chapter_sabiston():
    """Test creating a Chapter with SABISTON textbook ID."""
    chapter = Chapter(
        chapter_number="2",
        title="Surgical Infections",
        sections=[],
        key_points=None,
        textbook_id=TextbookEnum.SABISTON,
        source_file_path="/path/to/sabiston/chapter_02.json"
    )

    assert chapter.textbook_id == TextbookEnum.SABISTON
    assert chapter.chapter_number == "2"
    assert chapter.key_points is None
    assert chapter.authors is None


def test_chapter_schwartz():
    """Test creating a Chapter with SCHWARTZ textbook ID and authors."""
    bounds = Bounds(x1=10.0, y1=20.0, x2=100.0, y2=200.0)
    key_point = KeyPoint(
        label="KP1",
        content="Important surgical principle",
        page=10,
        bounds=bounds
    )

    chapter = Chapter(
        chapter_number="3",
        title="Wound Healing",
        sections=[],
        authors=["Dr. John Smith", "Dr. Jane Doe"],
        key_points=[key_point],
        textbook_id=TextbookEnum.SCHWARTZ,
        source_file_path="/path/to/schwartz/chapter_03.json"
    )

    assert chapter.textbook_id == TextbookEnum.SCHWARTZ
    assert chapter.authors is not None
    assert len(chapter.authors) == 2
    assert chapter.key_points is not None
    assert len(chapter.key_points) == 1


def test_chapter_serialization():
    """Test Chapter serialization with model_dump()."""
    chapter = Chapter(
        chapter_number="1",
        title="Test Chapter",
        sections=[],
        textbook_id=TextbookEnum.BAILEY,
        source_file_path="/path/to/test.json"
    )

    dumped = chapter.model_dump()

    assert isinstance(dumped, dict)
    assert "chapter_number" in dumped
    assert "title" in dumped
    assert "sections" in dumped
    assert "textbook_id" in dumped
    assert "source_file_path" in dumped
    assert dumped["chapter_number"] == "1"
    assert dumped["title"] == "Test Chapter"
