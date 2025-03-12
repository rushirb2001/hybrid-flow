#!/usr/bin/env python3
"""Fix JSON metadata by extracting chapter_number and title from filenames."""

import json
import re
from pathlib import Path


def extract_metadata_from_filename(filepath: Path) -> tuple[str, str]:
    """Extract chapter number and title from filename.

    Args:
        filepath: Path to JSON file

    Returns:
        Tuple of (chapter_number, title)
    """
    filename = filepath.stem  # Get filename without extension

    # Extract chapter number using regex
    match = re.search(r'^(\d+)_', filename)
    if not match:
        raise ValueError(f"Cannot extract chapter number from: {filename}")

    chapter_number = match.group(1)

    # Extract title: everything after first underscore, replace underscores with spaces
    title_part = filename[len(chapter_number) + 1:]  # Skip "NN_"
    title = title_part.replace('_', ' ')

    return chapter_number, title


def fix_json_file(filepath: Path, dry_run: bool = False) -> dict:
    """Fix metadata in a single JSON file.

    Args:
        filepath: Path to JSON file
        dry_run: If True, don't write changes

    Returns:
        Dictionary with fix details
    """
    # Extract metadata from filename
    chapter_number, title = extract_metadata_from_filename(filepath)

    # Load JSON
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check if fields need updating
    original_chapter = data.get('chapter_number', '')
    original_title = data.get('title', '')

    needs_update = False
    updated_fields = []

    if not original_chapter or original_chapter == '':
        data['chapter_number'] = chapter_number
        needs_update = True
        updated_fields.append('chapter_number')

    if not original_title or original_title == '':
        data['title'] = title
        needs_update = True
        updated_fields.append('title')

    # Write back if needed
    if needs_update and not dry_run:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    return {
        'file': str(filepath),
        'updated': needs_update,
        'fields': updated_fields,
        'chapter_number': chapter_number,
        'title': title,
    }


def main():
    """Main function to fix all JSON files."""
    data_dir = Path('data')

    if not data_dir.exists():
        print(f"Error: {data_dir} directory not found")
        return 1

    # Find all JSON files
    json_files = sorted(data_dir.glob('*/*.json'))

    print(f"Found {len(json_files)} JSON files")
    print()

    # Process each file
    updated_count = 0
    results = []

    for filepath in json_files:
        try:
            result = fix_json_file(filepath, dry_run=False)
            results.append(result)

            if result['updated']:
                updated_count += 1
                print(f"✓ Updated {filepath.name}")
                for field in result['fields']:
                    value = result[field]
                    print(f"  - {field}: '{value}'")
        except Exception as e:
            print(f"✗ Error processing {filepath.name}: {e}")

    print()
    print(f"Summary: Updated {updated_count} out of {len(json_files)} files")

    # List files that were updated
    if updated_count > 0:
        print()
        print("Updated files:")
        for result in results:
            if result['updated']:
                print(f"  - {Path(result['file']).name}")

    return 0


if __name__ == '__main__':
    exit(main())
