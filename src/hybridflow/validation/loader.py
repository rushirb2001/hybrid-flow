"""JSON file loader with validation and normalization."""

import json
import logging
from pathlib import Path
from typing import Union

from pydantic import ValidationError

from hybridflow.models import Chapter, TextbookEnum
from hybridflow.validation.error_handler import (
    clean_paragraphs_array,
    normalize_chapter_data,
)

logger = logging.getLogger(__name__)


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

    def normalize_chapter_number(
        self, raw_value: Union[str, int], file_path: str = ""
    ) -> str:
        """Normalize chapter number to string format.

        Args:
            raw_value: Raw chapter number as string or integer
            file_path: Path to source file (used as fallback if raw_value is empty)

        Returns:
            Normalized chapter number as string

        Raises:
            ValueError: If both raw_value and filename extraction fail
        """
        normalized = str(raw_value).strip()

        # If empty, try to extract from filename
        if not normalized and file_path:
            import re

            match = re.search(r"/(\d+)_", file_path)
            if match:
                normalized = match.group(1)
                logger.warning(
                    f"Empty chapter_number in JSON, extracted from filename: {normalized}"
                )
            else:
                raise ValueError(
                    f"Cannot determine chapter_number: empty in JSON and "
                    f"cannot extract from filename: {file_path}"
                )

        return normalized

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

    def clean_structure_paragraphs(self, data: dict) -> None:
        """Recursively clean malformed paragraphs in chapter structure.

        Args:
            data: Dictionary containing chapter data to clean in-place
        """
        # Clean paragraphs in sections
        if "sections" in data:
            for section in data["sections"]:
                # Clean section paragraphs
                if "paragraphs" in section:
                    section["paragraphs"] = clean_paragraphs_array(section["paragraphs"])

                # Clean subsection paragraphs
                if "subsections" in section:
                    for subsection in section["subsections"]:
                        if "paragraphs" in subsection:
                            subsection["paragraphs"] = clean_paragraphs_array(
                                subsection["paragraphs"]
                            )

                        # Clean subsubsection paragraphs
                        if "subsubsections" in subsection:
                            for subsubsection in subsection["subsubsections"]:
                                if "paragraphs" in subsubsection:
                                    subsubsection["paragraphs"] = clean_paragraphs_array(
                                        subsubsection["paragraphs"]
                                    )

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

        Raises:
            ValueError: If chapter cannot be parsed after error handling attempts
        """
        try:
            # Load raw JSON
            raw_data = self.load_json(file_path)

            # Detect textbook
            textbook_id = self.detect_textbook(file_path)

            # Apply error handler normalizations
            raw_data = normalize_chapter_data(raw_data)

            # Normalize chapter number if present
            if "chapter_number" in raw_data:
                raw_data["chapter_number"] = self.normalize_chapter_number(
                    raw_data["chapter_number"], file_path
                )

            # Normalize reference labels
            if "references" in raw_data and raw_data["references"]:
                for ref in raw_data["references"]:
                    if "label" in ref:
                        ref["label"] = self.normalize_reference_label(ref["label"])

            # Clean malformed paragraphs throughout the structure
            self.clean_structure_paragraphs(raw_data)

            # Normalize all bounds throughout the structure
            self.normalize_structure_bounds(raw_data)

            # Add textbook_id and source_file_path
            raw_data["textbook_id"] = textbook_id
            raw_data["source_file_path"] = file_path

            # Create and return Chapter model
            return Chapter(**raw_data)

        except ValidationError as e:
            logger.error(
                f"Validation error parsing {file_path}: {e.error_count()} errors"
            )
            logger.debug(f"Validation details: {e}")
            raise ValueError(
                f"Cannot parse chapter from {file_path}: {e.error_count()} validation errors"
            ) from e

        except Exception as e:
            logger.error(f"Unexpected error parsing {file_path}: {e}")
            raise ValueError(f"Cannot parse chapter from {file_path}: {e}") from e
