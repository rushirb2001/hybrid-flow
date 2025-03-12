#!/usr/bin/env python3
"""Fix Bailey chapter numbers that are off by +1 from filenames."""

import json
from pathlib import Path


def fix_bailey_chapters():
    """Fix chapter numbers in Bailey files from 63-92."""
    bailey_dir = Path('data/bailey')

    # Files that need fixing (off by +1)
    # 63-65: off by +1
    # 66-67: already correct
    # 68-92: off by +1

    files_to_fix = list(range(63, 66)) + list(range(68, 93))  # 63,64,65,68-92

    fixed_count = 0

    for file_num in files_to_fix:
        # Find file with this number
        files = list(bailey_dir.glob(f'{file_num}_*.json'))

        if not files:
            print(f"Warning: No file found for {file_num}")
            continue

        filepath = files[0]

        # Load JSON
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Check current chapter number
        current_chapter = data.get('chapter_number', '')
        expected_chapter = str(file_num)

        if current_chapter != expected_chapter:
            print(f"Fixing {filepath.name}: {current_chapter} -> {expected_chapter}")
            data['chapter_number'] = expected_chapter

            # Write back
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            fixed_count += 1
        else:
            print(f"Already correct: {filepath.name} ({current_chapter})")

    print(f"\nFixed {fixed_count} files")
    return fixed_count


if __name__ == '__main__':
    fix_bailey_chapters()
