from pathlib import Path
import re


def parse_nextflow_config(file_path: Path) -> dict:
    text = file_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    params = re.findall(r"params\.([A-Za-z0-9_]+)\s*=\s*([^\n]+)", text)
    executors = re.findall(r"executor\s*=\s*[\"']?([^\"'\n]+)", text)
    containers = re.findall(r"container\s*=\s*[\"']([^\"']+)", text)
    profiles = re.findall(r"^\s*([A-Za-z0-9_-]+)\s*\{", text, flags=re.MULTILINE)

    return {
        "language": "nextflow_config",
        "line_count": len(lines),
        "params": [
            {"name": k.strip(), "value": v.strip()}
            for k, v in params[:100]
        ],
        "executors": list(dict.fromkeys([x.strip() for x in executors]))[:20],
        "containers": list(dict.fromkeys([x.strip() for x in containers]))[:20],
        "profile_like_blocks": list(dict.fromkeys([x.strip() for x in profiles]))[:50],
        "preview": "\n".join(lines[:60]),
    }