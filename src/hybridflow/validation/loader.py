"""JSON file loader with validation and normalization."""

import json
from pathlib import Path
from typing import Union

from hybridflow.models import Chapter, TextbookEnum


class JSONLoader:
    """Loads and parses JSON files into validated Chapter models."""

    def __init__(self) -> None:
        """Initialize the JSON loader."""
        pass

    def load_json(self, file_path: str) -> dict:
        """Load JSON file and return parsed dictionary.

        Args:
            file_path: Path to JSON file

        Returns:
            Parsed JSON as dictionary
        """
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def detect_textbook(self, file_path: str) -> TextbookEnum:
        """Detect textbook type from file path.

        Args:
            file_path: Path to JSON file

        Returns:
            TextbookEnum value

        Raises:
            ValueError: If textbook cannot be detected from path
        """
        path_lower = file_path.lower()

        if "bailey" in path_lower:
            return TextbookEnum.BAILEY
        elif "sabiston" in path_lower:
            return TextbookEnum.SABISTON
        elif "schwartz" in path_lower:
            return TextbookEnum.SCHWARTZ
        else:
            raise ValueError(f"Cannot detect textbook type from path: {file_path}")

    def normalize_chapter_number(self, raw_value: Union[str, int]) -> str:
        """Normalize chapter number to string format.

        Args:
            raw_value: Raw chapter number as string or integer

        Returns:
            Normalized chapter number as string
        """
        return str(raw_value).strip()

    def normalize_reference_label(self, label: Union[str, int]) -> str:
        """Normalize reference label by removing trailing periods.

        Args:
            label: Raw reference label as string or integer

        Returns:
            Normalized label as string
        """
        return str(label).rstrip(".").strip()

    def normalize_bounds(self, bounds) -> dict:
        """Normalize bounds from array to dictionary format.

        Args:
            bounds: Bounds as list [x1, y1, x2, y2] or dict

        Returns:
            Bounds as dictionary {"x1": ..., "y1": ..., "x2": ..., "y2": ...}
        """
        if isinstance(bounds, list) and len(bounds) == 4:
            return {"x1": bounds[0], "y1": bounds[1], "x2": bounds[2], "y2": bounds[3]}
        return bounds

    def normalize_structure_bounds(self, data: dict) -> None:
        """Recursively normalize bounds in chapter structure.

        Args:
            data: Dictionary containing chapter data to normalize in-place
        """
        # Normalize key_points bounds
        if "key_points" in data and data["key_points"]:
            for kp in data["key_points"]:
                if "bounds" in kp:
                    kp["bounds"] = self.normalize_bounds(kp["bounds"])

        # Normalize sections
        if "sections" in data:
            for section in data["sections"]:
                # Section paragraphs
                if "paragraphs" in section:
                    for para in section["paragraphs"]:
                        if "bounds" in para:
                            para["bounds"] = self.normalize_bounds(para["bounds"])
                        # Normalize tables and figures in paragraphs
                        if "tables" in para and para["tables"]:
                            for table in para["tables"]:
                                if "bounds" in table:
                                    table["bounds"] = self.normalize_bounds(table["bounds"])
                        if "figures" in para and para["figures"]:
                            for figure in para["figures"]:
                                if "bounds" in figure:
                                    figure["bounds"] = self.normalize_bounds(figure["bounds"])

                # Subsections
                if "subsections" in section:
                    for subsection in section["subsections"]:
                        # Subsection paragraphs
                        if "paragraphs" in subsection:
                            for para in subsection["paragraphs"]:
                                if "bounds" in para:
                                    para["bounds"] = self.normalize_bounds(para["bounds"])
                                if "tables" in para and para["tables"]:
                                    for table in para["tables"]:
                                        if "bounds" in table:
                                            table["bounds"] = self.normalize_bounds(table["bounds"])
                                if "figures" in para and para["figures"]:
                                    for figure in para["figures"]:
                                        if "bounds" in figure:
                                            figure["bounds"] = self.normalize_bounds(figure["bounds"])

                        # Subsubsections
                        if "subsubsections" in subsection:
                            for subsubsection in subsection["subsubsections"]:
                                if "paragraphs" in subsubsection:
                                    for para in subsubsection["paragraphs"]:
                                        if "bounds" in para:
                                            para["bounds"] = self.normalize_bounds(para["bounds"])
                                        if "tables" in para and para["tables"]:
                                            for table in para["tables"]:
                                                if "bounds" in table:
                                                    table["bounds"] = self.normalize_bounds(table["bounds"])
                                        if "figures" in para and para["figures"]:
                                            for figure in para["figures"]:
                                                if "bounds" in figure:
                                                    figure["bounds"] = self.normalize_bounds(figure["bounds"])

    def parse_chapter(self, file_path: str) -> Chapter:
        """Parse JSON file into validated Chapter model.

        Args:
            file_path: Path to JSON file

        Returns:
            Validated Chapter instance
        """
        # Load raw JSON
        raw_data = self.load_json(file_path)

        # Detect textbook
        textbook_id = self.detect_textbook(file_path)

        # Normalize chapter number if present
        if "chapter_number" in raw_data:
            raw_data["chapter_number"] = self.normalize_chapter_number(
                raw_data["chapter_number"]
            )

        # Handle missing key_points field
        if "key_points" not in raw_data:
            raw_data["key_points"] = []

        # Handle missing authors field
        if "authors" not in raw_data:
            raw_data["authors"] = None

        # Normalize reference labels
        if "references" in raw_data and raw_data["references"]:
            for ref in raw_data["references"]:
                if "label" in ref:
                    ref["label"] = self.normalize_reference_label(ref["label"])

        # Normalize all bounds throughout the structure
        self.normalize_structure_bounds(raw_data)

        # Add textbook_id and source_file_path
        raw_data["textbook_id"] = textbook_id
        raw_data["source_file_path"] = file_path

        # Create and return Chapter model
        return Chapter(**raw_data)
