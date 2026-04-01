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


def _clean_path_candidate(s: str) -> str | None:
    s = s.strip()

    if not s:
        return None
    if len(s) > 220:
        return None
    if "\n" in s or "\r" in s:
        return None
    if "%s" in s or "{:" in s or "{}" in s:
        return None
    if s.count(" ") > 4 and "/" not in s and "\\" not in s:
        return None

    return s


def _extract_first_arg_strings(text: str, func_names: list[str]) -> list[str]:
    results = []
    func_pat = "|".join(re.escape(fn) for fn in func_names)
    pattern = rf"""
        (?<![\w:])(?:{func_pat})
        \s*\(\s*
        (?:file\s*=\s*)?
        ["']([^"']+)["']
    """
    for m in re.finditer(pattern, text, flags=re.VERBOSE):
        cleaned = _clean_path_candidate(m.group(1))
        if cleaned:
            results.append(cleaned)
    return _unique_keep_order(results)


def _extract_source_calls(text: str) -> list[str]:
    pattern = r"""(?<![\w:])source\s*\(\s*["']([^"']+)["']"""
    vals = [_clean_path_candidate(m.group(1)) for m in re.finditer(pattern, text)]
    return _unique_keep_order([v for v in vals if v])


def _extract_generic_file_refs(text: str) -> list[str]:
    pattern = r"""["']([^"']+\.(?:csv|tsv|txt|json|yaml|yml|rds|rdata|RData|pdf|png|jpg|jpeg|svg|tiff|xlsx|xls|npz|npy|pkl|vcf|gz))["']"""
    vals = [_clean_path_candidate(m.group(1)) for m in re.finditer(pattern, text, flags=re.IGNORECASE)]
    return _unique_keep_order([v for v in vals if v])[:200]


def parse_r_file(file_path: Path) -> dict:
    text = _safe_read_text(file_path)
    lines = text.splitlines()

    function_names = re.findall(
        r'^\s*([A-Za-z0-9_.]+)\s*<-\s*function\s*\(',
        text,
        flags=re.MULTILINE
    )

    libraries = re.findall(
        r'^\s*(?:library|require)\s*\(\s*([A-Za-z0-9_.]+)\s*\)',
        text,
        flags=re.MULTILINE
    )

    read_calls = [
        "readRDS", "load", "fread", "read.csv", "read.table", "read.delim",
        "read_tsv", "read_csv", "read_excel"
    ]
    write_calls = [
        "saveRDS", "save", "write.csv", "write.table", "write_tsv",
        "write_csv", "fwrite", "save.image"
    ]
    plot_calls = ["ggsave", "pdf", "png", "jpeg", "jpg", "tiff", "svg"]

    checkpoint_inputs = _extract_first_arg_strings(text, ["readRDS", "load"])
    checkpoint_outputs = _extract_first_arg_strings(text, ["saveRDS", "save", "save.image"])

    input_files = _extract_first_arg_strings(
        text,
        ["fread", "read.csv", "read.table", "read.delim", "read_tsv", "read_csv", "read_excel"]
    )
    output_files = _extract_first_arg_strings(
        text,
        ["write.csv", "write.table", "write_tsv", "write_csv", "fwrite"]
    )

    plot_outputs = _extract_first_arg_strings(text, plot_calls)
    source_references = _extract_source_calls(text)
    generic_file_references = _extract_generic_file_refs(text)

    cli_args = []
    if "commandArgs(" in text:
        cli_args.append("commandArgs")

    metadata = {
        "language": "r",
        "line_count": len(lines),
        "function_names": _unique_keep_order(function_names),
        "class_names": [],
        "libraries": _unique_keep_order(libraries),
        "imports": [],
        "cli_args": cli_args,
        "source_dependencies": source_references,
        "input_files": input_files,
        "output_files": output_files,
        "checkpoint_inputs": checkpoint_inputs,
        "checkpoint_outputs": checkpoint_outputs,
        "plot_outputs": plot_outputs,
        "generic_file_references": generic_file_references,
        "read_functions_used": [fn for fn in read_calls if re.search(rf'(?<![\w:]){re.escape(fn)}\s*\(', text)],
        "write_functions_used": [fn for fn in write_calls if re.search(rf'(?<![\w:]){re.escape(fn)}\s*\(', text)],
        "preview": "\n".join(lines[:20]),
    }

    return metadata