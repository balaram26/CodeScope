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
    pattern = r"""["']([^"']+\.(?:cpp|cc|cxx|hpp|h|cu|cuh|csv|tsv|txt|json|yaml|yml|fa|fasta|fq|fastq|gz|bin|dat|rds|npz|npy|pkl|png|pdf))["']"""
    vals = [m.group(1).strip() for m in re.finditer(pattern, text, flags=re.IGNORECASE)]
    return _unique_keep_order(vals)


def parse_cpp_file(file_path: Path) -> dict:
    text = _safe_read_text(file_path)
    lines = text.splitlines()

    includes = re.findall(r'^\s*#include\s+[<"]([^>"]+)[>"]', text, flags=re.MULTILINE)
    namespaces = re.findall(r'^\s*namespace\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{', text, flags=re.MULTILINE)
    classes = re.findall(r'^\s*(?:class|struct)\s+([A-Za-z_][A-Za-z0-9_]*)\b', text, flags=re.MULTILINE)

    # lightweight function signature detection
    function_names = re.findall(
        r'^\s*(?:template\s*<[^>]+>\s*)?(?:inline\s+)?(?:static\s+)?(?:virtual\s+)?(?:[A-Za-z_][A-Za-z0-9_:<>*&\s]+)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\([^;]*\)\s*(?:const\s*)?\{',
        text,
        flags=re.MULTILINE
    )

    io_refs = []
    for token in ["ifstream", "ofstream", "fstream", "fopen", "open", "read", "write"]:
        if token in text:
            io_refs.append(token)

    metadata = {
        "language": "cpp",
        "line_count": len(lines),
        "function_names": _unique_keep_order(function_names),
        "class_names": _unique_keep_order(classes),
        "libraries": [],
        "imports": _unique_keep_order(includes),
        "namespaces": _unique_keep_order(namespaces),
        "io_refs": _unique_keep_order(io_refs),
        "path_candidates": _extract_path_like_strings(text),
        "has_main": bool(re.search(r'\bint\s+main\s*\(', text)),
        "preview": "\n".join(lines[:30]),
    }
    return metadata
