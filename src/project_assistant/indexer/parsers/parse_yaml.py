from pathlib import Path
import re


def parse_yaml_file(file_path: Path) -> dict:
    text = file_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    top_level_keys = []
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.startswith(" ") or line.startswith("\t"):
            continue
        m = re.match(r"^([A-Za-z0-9_.-]+)\s*:", line)
        if m:
            top_level_keys.append(m.group(1))

    return {
        "language": "yaml",
        "line_count": len(lines),
        "top_level_keys": list(dict.fromkeys(top_level_keys))[:50],
        "preview": "\n".join(lines[:40]),
    }