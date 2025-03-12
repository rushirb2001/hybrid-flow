"""Error handling utilities for validation failures."""

import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


def handle_missing_field(raw_dict: dict, field_name: str, default_value: Any) -> dict:
    """Handle missing field by setting default value.

    Args:
        raw_dict: Dictionary to modify
        field_name: Name of field to check
        default_value: Default value if field is missing

    Returns:
        Modified dictionary with field set to default if missing
    """
    if field_name not in raw_dict or raw_dict[field_name] is None:
        logger.debug(f"Field '{field_name}' missing, setting to default: {default_value}")
        raw_dict[field_name] = default_value
    return raw_dict


def handle_type_mismatch(
    raw_dict: dict, field_name: str, expected_type: type
) -> Optional[Any]:
    """Handle type mismatch by attempting conversion.

    Args:
        raw_dict: Dictionary containing the field
        field_name: Name of field to convert
        expected_type: Expected type for conversion

    Returns:
        Converted value or None if conversion fails
    """
    if field_name not in raw_dict:
        return None

    value = raw_dict[field_name]

    try:
        # Handle list type specially
        if expected_type == list:
            if isinstance(value, list):
                return value
            elif isinstance(value, str):
                # Convert string to single-element list
                logger.warning(
                    f"Field '{field_name}' is string, converting to list: {value}"
                )
                return [value]
            else:
                logger.warning(
                    f"Field '{field_name}' has unexpected type {type(value)}, converting to list"
                )
                return [value]
        else:
            return expected_type(value)
    except Exception as e:
        logger.warning(f"Cannot convert '{field_name}' to {expected_type}: {e}")
        return None


def safe_parse_bounds(bounds: Any) -> Optional[List[float]]:
    """Safely parse bounds to list of floats.

    Args:
        bounds: Bounds value to parse (list, dict, or other)

    Returns:
        List of 4 floats [x1, y1, x2, y2] or None if invalid
    """
    try:
        # Already a list
        if isinstance(bounds, list):
            if len(bounds) != 4:
                logger.warning(f"Bounds list has {len(bounds)} elements, expected 4")
                return None
            return [float(x) for x in bounds]

        # Dictionary format
        elif isinstance(bounds, dict):
            required_keys = ["x1", "y1", "x2", "y2"]
            if all(k in bounds for k in required_keys):
                return [
                    float(bounds["x1"]),
                    float(bounds["y1"]),
                    float(bounds["x2"]),
                    float(bounds["y2"]),
                ]
            else:
                logger.warning(f"Bounds dict missing required keys: {required_keys}")
                return None

        else:
            logger.warning(f"Bounds has unexpected type: {type(bounds)}")
            return None

    except (ValueError, TypeError) as e:
        logger.warning(f"Cannot parse bounds: {e}")
        return None


def normalize_authors_field(raw_dict: dict) -> dict:
    """Normalize authors field to ensure it's a list or None.

    Args:
        raw_dict: Dictionary containing authors field

    Returns:
        Modified dictionary with normalized authors field
    """
    if "authors" in raw_dict:
        authors = raw_dict["authors"]

        # If it's a string, convert to single-element list
        if isinstance(authors, str):
            logger.info(f"Converting authors string to list: {authors}")
            raw_dict["authors"] = [authors]

        # If it's empty string or empty list, set to None
        elif authors == "" or authors == []:
            raw_dict["authors"] = None

    return raw_dict


def clean_paragraphs_array(paragraphs: List) -> List:
    """Filter out malformed paragraph objects from paragraphs array.

    Removes any dict that doesn't have both 'number' and 'text' fields.
    This handles cases where figures/tables are incorrectly placed directly
    in the paragraphs array instead of being nested.

    Args:
        paragraphs: List of paragraph dictionaries to clean

    Returns:
        Cleaned list containing only valid paragraph objects
    """
    if not paragraphs:
        return paragraphs

    cleaned = []
    removed_count = 0

    for para in paragraphs:
        if isinstance(para, dict):
            # Check if it has both required fields
            if "number" in para and "text" in para:
                cleaned.append(para)
            else:
                removed_count += 1
                # Log what type of object was removed
                obj_type = para.get("type", "unknown")
                logger.warning(
                    f"Removed malformed paragraph object (type={obj_type}, "
                    f"has_number={'number' in para}, has_text={'text' in para})"
                )
        else:
            logger.warning(f"Removed non-dict object from paragraphs: {type(para)}")
            removed_count += 1

    if removed_count > 0:
        logger.info(
            f"Cleaned paragraphs array: removed {removed_count} malformed objects, "
            f"kept {len(cleaned)} valid paragraphs"
        )

    return cleaned


def normalize_chapter_data(raw_data: dict) -> dict:
    """Apply all normalization fixes to chapter data.

    Args:
        raw_data: Raw chapter dictionary from JSON

    Returns:
        Normalized chapter dictionary
    """
    # Normalize authors field
    raw_data = normalize_authors_field(raw_data)

    # Handle missing key_points field
    if "key_points" not in raw_data or raw_data["key_points"] is None:
        raw_data["key_points"] = []

    # Handle missing references field
    if "references" not in raw_data or raw_data["references"] is None:
        raw_data["references"] = []

    # Handle missing sections field
    if "sections" not in raw_data or raw_data["sections"] is None:
        raw_data["sections"] = []

    return raw_data
