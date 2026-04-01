import re
from pathlib import Path


def _safe_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _unique_keep_order(items):
    seen = set()
    out = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _extract_path_like_strings(text: str) -> list[str]:
    pattern = r"""["']([^"']+\.(?:nf|py|r|R|sh|bash|csv|tsv|txt|json|yaml|yml|fa|fasta|fq|fastq|gz|zip|tar|pdf|png|rds|RData|npz|npy|pkl))["']"""
    vals = [m.group(1).strip() for m in re.finditer(pattern, text, flags=re.IGNORECASE)]
    return _unique_keep_order(vals)


def parse_nextflow_file(file_path: Path) -> dict:
    text = _safe_read_text(file_path)
    lines = text.splitlines()

    process_names = re.findall(r'^\s*process\s+([A-Za-z0-9_]+)\s*\{', text, flags=re.MULTILINE)
    workflow_names = re.findall(r'^\s*workflow\s*([A-Za-z0-9_]*)\s*\{', text, flags=re.MULTILINE)

    include_matches = re.findall(
        r'^\s*include\s+\{([^}]+)\}\s+from\s+[\'"]([^\'"]+)[\'"]',
        text,
        flags=re.MULTILINE
    )
    includes = []
    for names, src in include_matches:
        includes.append({
            "symbols": [x.strip() for x in names.split(";") if x.strip()] if ";" in names else [x.strip() for x in names.split(",") if x.strip()],
            "source": src.strip(),
        })

    params_used = re.findall(r'\bparams\.([A-Za-z0-9_]+)\b', text)
    publish_dirs = re.findall(r'publishDir\s+([^\n]+)', text)

    python_refs = re.findall(r'\bpython(?:3)?\s+([^\s\'"]+\.py)\b', text)
    r_refs = re.findall(r'\bRscript\s+([^\s\'"]+\.R)\b', text)
    shell_refs = re.findall(r'\b(?:bash|sh)\s+([^\s\'"]+\.(?:sh|bash))\b', text)

    channel_ops = re.findall(r'\bChannel\.([A-Za-z0-9_]+)\b', text)

    metadata = {
        "language": "nextflow",
        "line_count": len(lines),
        "function_names": [],
        "class_names": [],
        "process_names": _unique_keep_order(process_names),
        "workflow_names": _unique_keep_order([x for x in workflow_names if x]),
        "includes": includes,
        "params_used": _unique_keep_order(params_used),
        "publish_dirs": _unique_keep_order([x.strip() for x in publish_dirs]),
        "python_refs": _unique_keep_order(python_refs),
        "r_refs": _unique_keep_order(r_refs),
        "shell_refs": _unique_keep_order(shell_refs),
        "channel_ops": _unique_keep_order(channel_ops),
        "path_candidates": _extract_path_like_strings(text),
        "preview": "\n".join(lines[:30]),
    }

    return metadata
