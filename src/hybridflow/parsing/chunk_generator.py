"""Chunk generator for extracting paragraphs from hierarchical chapter structure."""

from typing import List, Tuple

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
