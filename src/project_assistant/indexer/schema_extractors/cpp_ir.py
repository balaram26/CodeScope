import re
from pathlib import Path

from project_assistant.indexer.schema_extractors.common import (
    extract_string_path_candidates,
    unique_keep_order,
)


def _safe_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _find_cpp_functions(text: str):
    lines = text.splitlines()
    results = []

    pattern = re.compile(
        r'^\s*(?:template\s*<[^>]+>\s*)?(?:inline\s+)?(?:static\s+)?(?:virtual\s+)?(?:[A-Za-z_][A-Za-z0-9_:<>*&\s]+)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\([^;]*\)\s*(?:const\s*)?\{',
        re.MULTILINE
    )

    line_offsets = []
    pos = 0
    for line in lines:
        line_offsets.append(pos)
        pos += len(line) + 1

    def pos_to_line(char_pos: int) -> int:
        line_no = 1
        for i, start in enumerate(line_offsets):
            if start <= char_pos:
                line_no = i + 1
            else:
                break
        return line_no

    for m in pattern.finditer(text):
        fn_name = m.group(1)
        brace_start = text.find("{", m.end() - 1)
        if brace_start == -1:
            continue

        depth = 0
        end_pos = None
        for i in range(brace_start, len(text)):
            ch = text[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end_pos = i
                    break

        if end_pos is None:
            continue

        body = text[m.start(): end_pos + 1]
        results.append({
            "function_name": fn_name,
            "start_line": pos_to_line(m.start()),
            "end_line": pos_to_line(end_pos),
            "body_text": body,
        })

    return results


def _extract_called_symbols(fn_text: str) -> list[str]:
    candidates = re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\s*\(', fn_text)
    blacklist = {
        "if", "for", "while", "switch", "return", "catch", "sizeof"
    }
    return unique_keep_order([c for c in candidates if c not in blacklist])


def extract_cpp_ir(file_path: Path) -> dict:
    text = _safe_read_text(file_path)

    fn_blocks = _find_cpp_functions(text)
    fn_names = [f["function_name"] for f in fn_blocks]
    fn_name_set = set(fn_names)

    functions = []
    call_edges = []

    for fn in fn_blocks:
        called = _extract_called_symbols(fn["body_text"])
        internal_calls = [c for c in called if c in fn_name_set and c != fn["function_name"]]
        external_calls = [c for c in called if c not in fn_name_set]

        functions.append({
            "function_name": fn["function_name"],
            "start_line": fn["start_line"],
            "end_line": fn["end_line"],
            "function_text": fn["body_text"],
            "called_symbols": called,
            "internal_calls": internal_calls,
            "external_calls": external_calls,
            "path_candidates": extract_string_path_candidates(fn["body_text"]),
            "doc_hint": "",
        })

        for c in internal_calls:
            call_edges.append([fn["function_name"], c])

    includes = re.findall(r'^\s*#include\s+[<"]([^>"]+)[>"]', text, flags=re.MULTILINE)

    return {
        "language": "cpp",
        "file_level": {
            "defined_functions": fn_names,
            "entry_points": ["main"] if "main" in fn_name_set else [],
            "imports_or_libraries": unique_keep_order(includes),
            "source_dependencies": [],
            "string_path_candidates": extract_string_path_candidates(text),
        },
        "functions": functions,
        "call_edges": call_edges,
    }
