from pathlib import Path
import re


def parse_shell_file(file_path: Path) -> dict:
    text = file_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    commands = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        m = re.match(r"^([A-Za-z0-9_./-]+)", s)
        if m:
            commands.append(m.group(1))

    script_refs = re.findall(r"\b([^\s'\"`]+\.(?:py|R|r|sh|bash))\b", text)
    path_candidates = re.findall(r"""["']([^"']+\.(?:csv|tsv|txt|json|yaml|yml|fa|fasta|fq|fastq|gz|zip|tar|pdf|png|rds|RData|npz|npy|pkl))["']""", text)

    return {
        "language": "shell",
        "line_count": len(lines),
        "commands": list(dict.fromkeys(commands))[:30],
        "script_refs": list(dict.fromkeys(script_refs))[:30],
        "path_candidates": list(dict.fromkeys(path_candidates))[:50],
        "preview": "\n".join(lines[:40]),
    }