"""Chunk generator for extracting paragraphs from hierarchical chapter structure."""

import re
from typing import Dict, List, Tuple

from hybridflow.models import Chapter, Paragraph, Section


class ChunkGenerator:
    """Generates chunks from chapter hierarchy for ingestion."""

    def __init__(self) -> None:
        """Initialize the chunk generator."""
        pass

    def generate_chunk_id(
        self, textbook_id: str, chapter_number: str, paragraph_number: str
    ) -> str:
        """Generate unique chunk ID from textbook, chapter, and paragraph identifiers.

        Args:
            textbook_id: Textbook identifier (e.g., "bailey")
            chapter_number: Chapter number (e.g., "2")
            paragraph_number: Paragraph number (e.g., "2.1.1")

        Returns:
            Chunk ID in format "textbook:chNumber:paragraphNumber"
        """
        return f"{textbook_id}:ch{chapter_number}:{paragraph_number}"

    def extract_references(self, text: str) -> List[Dict[str, str]]:
        """Extract figure and table cross-references from paragraph text.

        Handles multiple reference formats to ensure comprehensive extraction:
        - (Figure 60.5), [Figure 60.5], Figure 60.5
        - (Fig. 60.5), [Fig. 60.5], Fig. 60.5, Fig 60.5
        - (Table 60.3), [Table 60.3], Table 60.3

        This method is called during ingestion to pre-compute references,
        storing them in the Paragraph node's cross_references property for
        efficient retrieval later without re-parsing text.

        Args:
            text: The paragraph text to parse

        Returns:
            List of reference dictionaries with keys:
                - type: "figure" or "table"
                - number: The reference number (e.g., "60.5")

        Examples:
            >>> gen = ChunkGenerator()
            >>> gen.extract_references("See (Figure 60.1) and [Table 60.2]")
            [{"type": "figure", "number": "60.1"}, {"type": "table", "number": "60.2"}]

            >>> gen.extract_references("Compare Fig. 2.1 with Figure 2.2")
            [{"type": "figure", "number": "2.1"}, {"type": "figure", "number": "2.2"}]
        """
        references = []
        seen = set()  # Track seen references to avoid duplicates

        # Pattern for figures: optional brackets [(], Figure/Fig./Fig, optional space/dot, number
        # Matches: (Figure 60.5), [Figure 60.5], Figure 60.5, Fig. 60.5, Fig 60.5
        figure_pattern = r'[\(\[]?\s*(Figure|Fig\.?)\s+(\d+\.\d+)\s*[\)\]]?'
        for match in re.finditer(figure_pattern, text, re.IGNORECASE):
            number = match.group(2)
            ref_key = f"figure:{number}"
            if ref_key not in seen:
                references.append({"type": "figure", "number": number})
                seen.add(ref_key)

        # Pattern for tables: optional brackets [(], Table, optional space, number
        # Matches: (Table 60.3), [Table 60.3], Table 60.3
        table_pattern = r'[\(\[]?\s*Table\s+(\d+\.\d+)\s*[\)\]]?'
        for match in re.finditer(table_pattern, text, re.IGNORECASE):
            number = match.group(1)
            ref_key = f"table:{number}"
            if ref_key not in seen:
                references.append({"type": "table", "number": number})
                seen.add(ref_key)

        return references

    def extract_paragraphs_from_section(
        self, section: Section, chapter_id: str, hierarchy_path: List[str]
    ):
        """Recursively extract paragraphs from section hierarchy.

        Args:
            section: Section to extract from
            chapter_id: Base chapter identifier (e.g., "bailey:ch2")
            hierarchy_path: List of titles from chapter to current section

        Yields:
            Tuples of (chunk_id, paragraph, hierarchy_path)
        """
        # Current hierarchy includes this section
        current_path = hierarchy_path + [section.title]

        # Extract direct paragraphs from this section
        for paragraph in section.paragraphs:
            chunk_id = f"{chapter_id}:{paragraph.number}"
            yield (chunk_id, paragraph, current_path)

        # Recursively extract from subsections
        for subsection in section.subsections:
            subsection_path = current_path + [subsection.title]

            # Extract paragraphs from subsection
            for paragraph in subsection.paragraphs:
                chunk_id = f"{chapter_id}:{paragraph.number}"
                yield (chunk_id, paragraph, subsection_path)

            # Recursively extract from subsubsections
            for subsubsection in subsection.subsubsections:
                subsubsection_path = subsection_path + [subsubsection.title]

                # Extract paragraphs from subsubsection
                for paragraph in subsubsection.paragraphs:
                    chunk_id = f"{chapter_id}:{paragraph.number}"
                    yield (chunk_id, paragraph, subsubsection_path)

    def generate_chunks(
        self, chapter: Chapter
    ) -> List[Tuple[str, Paragraph, List[str]]]:
        """Generate all chunks from chapter hierarchy.

        Args:
            chapter: Chapter to process

        Returns:
            List of tuples (chunk_id, paragraph, hierarchy_path)
        """
        # Create base chapter identifier
        chapter_id = f"{chapter.textbook_id.value}:ch{chapter.chapter_number}"

        # Base hierarchy path starts with chapter title
        base_hierarchy = [chapter.title]

        chunks = []
        for section in chapter.sections:
            for chunk_tuple in self.extract_paragraphs_from_section(
                section, chapter_id, base_hierarchy
            ):
                chunks.append(chunk_tuple)

        return chunks
