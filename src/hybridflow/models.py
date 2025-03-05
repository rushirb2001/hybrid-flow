"""Pydantic models for HybridFlow data structures."""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class TextbookEnum(str, Enum):
    """Enumeration of supported textbooks."""

    BAILEY = "bailey"
    SABISTON = "sabiston"
    SCHWARTZ = "schwartz"


class Bounds(BaseModel):
    """Bounding box coordinates for content location."""

    x1: float = Field(..., description="Left x-coordinate")
    y1: float = Field(..., description="Top y-coordinate")
    x2: float = Field(..., description="Right x-coordinate")
    y2: float = Field(..., description="Bottom y-coordinate")


class Table(BaseModel):
    """Table metadata and file references."""

    table_number: str = Field(..., description="Table identifier number")
    file_png: str = Field(..., description="Path to PNG image file")
    file_xlsx: str = Field(..., description="Path to Excel file")
    description: str = Field(..., description="Table description/caption")
    page: int = Field(..., description="Page number where table appears")
    bounds: Bounds = Field(..., description="Bounding box coordinates")


class Figure(BaseModel):
    """Figure metadata and file references."""

    figure_number: str = Field(..., description="Figure identifier number")
    file_png: str = Field(..., description="Path to PNG image file")
    caption: str = Field(..., description="Figure caption text")
    page: int = Field(..., description="Page number where figure appears")
    bounds: Bounds = Field(..., description="Bounding box coordinates")


class Paragraph(BaseModel):
    """Paragraph content with optional embedded tables and figures."""

    number: str = Field(..., description="Paragraph identifier number")
    text: str = Field(..., description="Paragraph text content")
    page: int = Field(..., description="Page number where paragraph appears")
    bounds: Bounds = Field(..., description="Bounding box coordinates")
    tables: Optional[List[Table]] = None
    figures: Optional[List[Figure]] = None


class Subsubsection(BaseModel):
    """Third-level section hierarchy."""

    title: str = Field(..., description="Subsubsection title")
    number: str = Field(..., description="Subsubsection number")
    paragraphs: List[Paragraph] = Field(default_factory=list, description="List of paragraphs")


class Subsection(BaseModel):
    """Second-level section hierarchy."""

    title: str = Field(..., description="Subsection title")
    number: str = Field(..., description="Subsection number")
    paragraphs: List[Paragraph] = Field(default_factory=list, description="List of paragraphs")
    subsubsections: List[Subsubsection] = Field(default_factory=list, description="List of subsubsections")


class Section(BaseModel):
    """Top-level section hierarchy."""

    title: str = Field(..., description="Section title")
    number: str = Field(..., description="Section number")
    paragraphs: List[Paragraph] = Field(default_factory=list, description="List of paragraphs")
    subsections: List[Subsection] = Field(default_factory=list, description="List of subsections")


class KeyPoint(BaseModel):
    """Key learning point from the chapter."""

    label: str = Field(..., description="Key point label/identifier")
    content: str = Field(..., description="Key point content text")
    page: int = Field(..., description="Page number where key point appears")
    bounds: Bounds = Field(..., description="Bounding box coordinates")


class Reference(BaseModel):
    """Bibliographic reference."""

    label: str = Field(..., description="Reference label/identifier")
    body: str = Field(..., description="Full reference citation text")
    is_key_reference: bool = Field(..., description="Whether this is a key reference")
    thematic_section: str = Field(..., description="Thematic category of reference")


class Chapter(BaseModel):
    """Complete chapter structure with all nested content."""

    chapter_number: str = Field(..., description="Chapter identifier number")
    title: str = Field(..., description="Chapter title")
    sections: List[Section] = Field(default_factory=list, description="List of sections")
    authors: Optional[List[str]] = None
    key_points: Optional[List[KeyPoint]] = None
    references: Optional[List[Reference]] = None
    textbook_id: TextbookEnum = Field(..., description="Source textbook identifier")
    source_file_path: str = Field(..., description="Path to source JSON file")
