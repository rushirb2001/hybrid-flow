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

        # Add textbook_id and source_file_path
        raw_data["textbook_id"] = textbook_id
        raw_data["source_file_path"] = file_path

        # Create and return Chapter model
        return Chapter(**raw_data)
