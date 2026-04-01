import re
from pathlib import Path


def _safe_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def parse_markdown_file(file_path: Path) -> dict:
    text = _safe_read_text(file_path)
    lines = text.splitlines()

    headings = re.findall(r'^\s{0,3}#{1,6}\s+(.*)$', text, flags=re.MULTILINE)

    metadata = {
        "language": "markdown",
        "line_count": len(lines),
        "heading_count": len(headings),
        "headings": headings[:100],
        "word_count": len(text.split()),
        "preview": "\n".join(lines[:30]),
    }
    return metadata


def parse_text_file(file_path: Path) -> dict:
    text = _safe_read_text(file_path)
    lines = text.splitlines()

    metadata = {
        "language": "text",
        "line_count": len(lines),
        "word_count": len(text.split()),
        "preview": "\n".join(lines[:30]),
    }
    return metadata
