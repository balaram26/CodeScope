from pathlib import Path
import re


def parse_dockerfile(file_path: Path) -> dict:
    text = file_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    base_images = re.findall(r"^\s*FROM\s+([^\s]+)", text, flags=re.MULTILINE)
    run_cmds = re.findall(r"^\s*RUN\s+(.+)", text, flags=re.MULTILINE)
    copy_cmds = re.findall(r"^\s*(?:COPY|ADD)\s+(.+)", text, flags=re.MULTILINE)
    entrypoints = re.findall(r"^\s*ENTRYPOINT\s+(.+)", text, flags=re.MULTILINE)
    cmds = re.findall(r"^\s*CMD\s+(.+)", text, flags=re.MULTILINE)

    return {
        "language": "dockerfile",
        "line_count": len(lines),
        "base_images": base_images[:10],
        "run_commands": run_cmds[:15],
        "copy_commands": copy_cmds[:15],
        "entrypoints": entrypoints[:5],
        "cmds": cmds[:5],
        "preview": "\n".join(lines[:40]),
    }
