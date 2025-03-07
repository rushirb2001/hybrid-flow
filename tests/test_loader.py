"""Tests for JSON loader with validation and normalization."""

import json

import pytest

from hybridflow.models import TextbookEnum
from hybridflow.validation.loader import JSONLoader


@pytest.fixture
def bailey_json_file(tmp_path):
    """Create a sample Bailey JSON file for testing."""
    data = {
        "chapter_number": "2",
        "title": "Shock and blood transfusion",
        "sections": [],
        "key_points": [],
    }
    file_path = tmp_path / "bailey" / "chapter_2.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return str(file_path)


@pytest.fixture
def sabiston_json_file(tmp_path):
    """Create a sample Sabiston JSON file without key_points field."""
    data = {
        "chapter_number": 2,
        "title": "Wound Healing",
        "sections": [],
    }
    file_path = tmp_path / "sabiston" / "chapter_2.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return str(file_path)


@pytest.fixture
def schwartz_json_file(tmp_path):
    """Create a sample Schwartz JSON file with authors and key_points."""
    data = {
        "chapter_number": "2",
        "title": "Fluid and Electrolyte Management",
        "sections": [],
        "authors": ["Dr. Smith", "Dr. Jones"],
        "key_points": [
            {
                "label": "KP1",
                "content": "Maintain fluid balance",
                "page": 42,
                "bounds": {"x1": 10.0, "y1": 20.0, "x2": 100.0, "y2": 50.0},
            },
            {
                "label": "KP2",
                "content": "Monitor electrolytes",
                "page": 45,
                "bounds": {"x1": 10.0, "y1": 60.0, "x2": 100.0, "y2": 90.0},
            },
        ],
        "references": [
            {
                "label": "1.",
                "body": "Smith et al. 2020",
                "is_key_reference": True,
                "thematic_section": "General",
            },
            {
                "label": "2.",
                "body": "Jones et al. 2021",
                "is_key_reference": False,
                "thematic_section": "Specific",
            },
        ],
    }
    file_path = tmp_path / "schwartz" / "chapter_2.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return str(file_path)


def test_detect_textbook_bailey():
    """Test textbook detection for Bailey."""
    loader = JSONLoader()
    result = loader.detect_textbook("/path/to/bailey/chapter_2.json")
    assert result == TextbookEnum.BAILEY


def test_detect_textbook_sabiston():
    """Test textbook detection for Sabiston."""
    loader = JSONLoader()
    result = loader.detect_textbook("/path/to/sabiston/chapter_2.json")
    assert result == TextbookEnum.SABISTON


def test_detect_textbook_schwartz():
    """Test textbook detection for Schwartz."""
    loader = JSONLoader()
    result = loader.detect_textbook("/path/to/schwartz/chapter_2.json")
    assert result == TextbookEnum.SCHWARTZ


def test_normalize_chapter_number_string():
    """Test chapter number normalization with string input."""
    loader = JSONLoader()
    result = loader.normalize_chapter_number("2")
    assert result == "2"


def test_normalize_chapter_number_int():
    """Test chapter number normalization with integer input."""
    loader = JSONLoader()
    result = loader.normalize_chapter_number(2)
    assert result == "2"


def test_normalize_reference_label_with_period():
    """Test reference label normalization removes trailing period."""
    loader = JSONLoader()
    result = loader.normalize_reference_label("26.")
    assert result == "26"


def test_normalize_reference_label_int():
    """Test reference label normalization with integer input."""
    loader = JSONLoader()
    result = loader.normalize_reference_label(-1)
    assert result == "-1"


def test_parse_chapter_bailey(bailey_json_file):
    """Test parsing Bailey chapter with empty key_points."""
    loader = JSONLoader()
    chapter = loader.parse_chapter(bailey_json_file)

    assert chapter.textbook_id == TextbookEnum.BAILEY
    assert isinstance(chapter.chapter_number, str)
    assert chapter.chapter_number == "2"
    assert chapter.key_points == []


def test_parse_chapter_sabiston(sabiston_json_file):
    """Test parsing Sabiston chapter without key_points field."""
    loader = JSONLoader()
    chapter = loader.parse_chapter(sabiston_json_file)

    assert chapter.textbook_id == TextbookEnum.SABISTON
    assert chapter.chapter_number == "2"
    assert chapter.key_points == []


def test_parse_chapter_schwartz(schwartz_json_file):
    """Test parsing Schwartz chapter with authors and key_points."""
    loader = JSONLoader()
    chapter = loader.parse_chapter(schwartz_json_file)

    assert chapter.textbook_id == TextbookEnum.SCHWARTZ
    assert chapter.authors is not None
    assert len(chapter.authors) == 2
    assert len(chapter.key_points) > 0
    assert len(chapter.key_points) == 2
