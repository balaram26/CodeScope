import ast
from pathlib import Path

from project_assistant.indexer.schema_extractors.common import (
    extract_string_path_candidates,
    unique_keep_order,
)


def _safe_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


class FunctionCallCollector(ast.NodeVisitor):
    def __init__(self):
        self.calls = []

    def visit_Call(self, node):
        name = None

        if isinstance(node.func, ast.Name):
            name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            parts = []
            cur = node.func
            while isinstance(cur, ast.Attribute):
                parts.append(cur.attr)
                cur = cur.value
            if isinstance(cur, ast.Name):
                parts.append(cur.id)
                name = ".".join(reversed(parts))

        if name:
            self.calls.append(name)

        self.generic_visit(node)


def extract_python_ir(file_path: Path) -> dict:
    text = _safe_read_text(file_path)
    lines = text.splitlines()

    tree = ast.parse(text)

    imports = []
    functions = []
    function_names = []
    file_call_edges = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)

    top_level_functions = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    function_names = [fn.name for fn in top_level_functions]
    function_name_set = set(function_names)

    for fn in top_level_functions:
        collector = FunctionCallCollector()
        collector.visit(fn)

        called = unique_keep_order(collector.calls)
        internal_calls = [c for c in called if c in function_name_set and c != fn.name]
        external_calls = [c for c in called if c not in function_name_set]

        start_line = getattr(fn, "lineno", None)
        end_line = getattr(fn, "end_lineno", None)
        fn_text = ""
        if start_line and end_line:
            fn_text = "\n".join(lines[start_line - 1:end_line])

        path_candidates = extract_string_path_candidates(fn_text)

        functions.append({
            "function_name": fn.name,
            "start_line": start_line,
            "end_line": end_line,
            "function_text": fn_text,
            "called_symbols": called,
            "internal_calls": internal_calls,
            "external_calls": external_calls,
            "path_candidates": path_candidates,
            "doc_hint": ast.get_docstring(fn) or "",
        })

        for c in internal_calls:
            file_call_edges.append([fn.name, c])

    entry_points = []
    if "__name__ == '__main__'" in text or '__name__ == "__main__"' in text:
        entry_points.append("__main__")
    if "main" in function_name_set:
        entry_points.append("main")

    return {
        "language": "python",
        "file_level": {
            "defined_functions": function_names,
            "entry_points": unique_keep_order(entry_points),
            "imports_or_libraries": unique_keep_order(imports),
            "source_dependencies": [],
            "string_path_candidates": extract_string_path_candidates(text),
        },
        "functions": functions,
        "call_edges": unique_keep_order(tuple(edge) for edge in file_call_edges),
    }
