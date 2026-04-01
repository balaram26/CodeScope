import ast
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
    if "{:" in s or "{}" in s:
        return None
    if s.count(" ") > 4 and "/" not in s and "\\" not in s:
        return None

    return s


def _extract_call_first_arg_strings(text: str, func_names: list[str]) -> list[str]:
    results = []
    func_pat = "|".join(re.escape(fn) for fn in func_names)
    pattern = rf"""
        (?<![\w.])
        (?:{func_pat})
        \s*\(\s*
        (?:file\s*=\s*)?
        ["']([^"']+)["']
    """
    for m in re.finditer(pattern, text, flags=re.VERBOSE):
        cleaned = _clean_path_candidate(m.group(1))
        if cleaned:
            results.append(cleaned)
    return _unique_keep_order(results)


def _extract_generic_file_refs(text: str) -> list[str]:
    pattern = r"""["']([^"']+\.(?:csv|tsv|txt|json|yaml|yml|rds|rdata|pdf|png|jpg|jpeg|svg|tiff|xlsx|xls|npz|npy|pkl|pickle|pt|pth|joblib|vcf|gz))["']"""
    vals = [_clean_path_candidate(m.group(1)) for m in re.finditer(pattern, text, flags=re.IGNORECASE)]
    return _unique_keep_order([v for v in vals if v])[:200]


def _extract_imports(tree: ast.AST) -> tuple[list[str], list[str]]:
    imports = []
    local_deps = []

    std_or_common = (
        "os", "sys", "json", "re", "math", "pathlib", "collections",
        "itertools", "typing", "logging", "argparse", "subprocess",
        "shutil", "tempfile", "csv", "pickle", "gzip", "bz2",
        "lzma", "sqlite3", "datetime", "time", "glob",
        "numpy", "pandas", "matplotlib", "sklearn", "scipy",
        "seaborn", "joblib", "torch"
    )

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
                if not alias.name.startswith(std_or_common):
                    local_deps.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod:
                imports.append(mod)
                if not mod.startswith(std_or_common):
                    local_deps.append(mod)

    return _unique_keep_order(imports), _unique_keep_order(local_deps)


def parse_python_file(file_path: Path) -> dict:
    text = _safe_read_text(file_path)
    lines = text.splitlines()

    metadata = {
        "language": "python",
        "line_count": len(lines),
        "function_names": [],
        "class_names": [],
        "libraries": [],
        "imports": [],
        "cli_args": [],
        "source_dependencies": [],
        "input_files": [],
        "output_files": [],
        "checkpoint_inputs": [],
        "checkpoint_outputs": [],
        "plot_outputs": [],
        "generic_file_references": [],
        "read_functions_used": [],
        "write_functions_used": [],
        "has_main_guard": "__name__ == '__main__'" in text or '__name__ == "__main__"' in text,
        "preview": "\n".join(lines[:20]),
    }

    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        metadata["parse_error"] = f"SyntaxError: {exc}"
        return metadata

    functions = []
    classes = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            functions.append(node.name)
        elif isinstance(node, ast.AsyncFunctionDef):
            functions.append(node.name)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)

    imports, local_deps = _extract_imports(tree)

    cli_patterns = [
        r'add_argument\(\s*[\'"]([^\'"]+)',
        r'ArgumentParser\(',
        r'sys\.argv',
        r'typer\.',
        r'click\.command',
    ]
    cli_matches = []
    for pattern in cli_patterns:
        cli_matches.extend(re.findall(pattern, text))

    input_files = _extract_call_first_arg_strings(
        text,
        [
            "pd.read_csv", "pd.read_table", "pd.read_excel",
            "read_csv", "read_table", "read_excel",
            "open", "np.load", "np.loadtxt",
            "pickle.load", "joblib.load", "torch.load"
        ]
    )

    output_files = _extract_call_first_arg_strings(
        text,
        [
            "np.save", "np.savez", "np.savez_compressed",
            "pickle.dump", "joblib.dump", "torch.save",
            "open"
        ]
    )

    checkpoint_inputs = _extract_call_first_arg_strings(
        text,
        ["np.load", "pickle.load", "joblib.load", "torch.load"]
    )
    checkpoint_outputs = _extract_call_first_arg_strings(
        text,
        ["np.save", "np.savez", "np.savez_compressed", "pickle.dump", "joblib.dump", "torch.save"]
    )

    plot_outputs = _extract_call_first_arg_strings(
        text,
        ["plt.savefig", "fig.savefig", "savefig"]
    )

    generic_file_references = _extract_generic_file_refs(text)

    open_calls = re.findall(
        r"""open\(\s*["']([^"']+)["']\s*,\s*["']([^"']+)["']""",
        text
    )
    for path_str, mode in open_calls:
        cleaned = _clean_path_candidate(path_str)
        if not cleaned:
            continue
        if "r" in mode and cleaned not in input_files:
            input_files.append(cleaned)
        if any(m in mode for m in ["w", "a", "x"]) and cleaned not in output_files:
            output_files.append(cleaned)

    df_to_csv = re.findall(r"""\.to_csv\(\s*["']([^"']+)["']""", text)
    df_to_excel = re.findall(r"""\.to_excel\(\s*["']([^"']+)["']""", text)
    for p in df_to_csv + df_to_excel:
        cleaned = _clean_path_candidate(p)
        if cleaned and cleaned not in output_files:
            output_files.append(cleaned)

    read_functions_used = [
        fn for fn in [
            "pd.read_csv", "pd.read_table", "pd.read_excel",
            "np.load", "np.loadtxt", "pickle.load", "joblib.load", "torch.load", "open"
        ] if fn in text
    ]

    write_functions_used = [
        fn for fn in [
            "to_csv", "to_excel", "np.save", "np.savez", "np.savez_compressed",
            "pickle.dump", "joblib.dump", "torch.save", "open", "savefig"
        ] if fn in text
    ]

    metadata["function_names"] = _unique_keep_order(functions)
    metadata["class_names"] = _unique_keep_order(classes)
    metadata["imports"] = imports
    metadata["libraries"] = []
    metadata["cli_args"] = _unique_keep_order(cli_matches)
    metadata["source_dependencies"] = local_deps
    metadata["input_files"] = _unique_keep_order(input_files)
    metadata["output_files"] = _unique_keep_order(output_files)
    metadata["checkpoint_inputs"] = _unique_keep_order(checkpoint_inputs)
    metadata["checkpoint_outputs"] = _unique_keep_order(checkpoint_outputs)
    metadata["plot_outputs"] = _unique_keep_order(plot_outputs)
    metadata["generic_file_references"] = generic_file_references
    metadata["read_functions_used"] = _unique_keep_order(read_functions_used)
    metadata["write_functions_used"] = _unique_keep_order(write_functions_used)

    return metadata