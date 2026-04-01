import re
from pathlib import Path

from project_assistant.indexer.schema_extractors.common import (
    extract_string_path_candidates,
    unique_keep_order,
)


def _safe_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _find_blocks(text: str, keyword: str):
    """
    Finds blocks like:
      process NAME { ... }
      workflow NAME { ... }
      workflow { ... }  # unnamed workflow
    """
    results = []
    if keyword == "workflow":
        pattern = re.compile(r'^\s*workflow\s*([A-Za-z0-9_]*)\s*\{', re.MULTILINE)
    else:
        pattern = re.compile(r'^\s*process\s+([A-Za-z0-9_]+)\s*\{', re.MULTILINE)

    lines = text.splitlines()

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
        name = m.group(1).strip() if m.group(1) else ""
        block_start = text.find("{", m.end() - 1)
        if block_start == -1:
            continue

        depth = 0
        end_pos = None
        for i in range(block_start, len(text)):
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
            "name": name if name else "__main__workflow__",
            "start_line": pos_to_line(m.start()),
            "end_line": pos_to_line(end_pos),
            "body_text": body,
        })

    return results


def _extract_called_processes_or_workflows(block_text: str) -> list[str]:
    # crude but useful: lines that look like process/workflow invocation
    candidates = re.findall(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(', block_text, flags=re.MULTILINE)
    return unique_keep_order(candidates)


def _extract_script_refs(block_text: str) -> dict:
    py = re.findall(r'\bpython(?:3)?\s+([^\s\'"]+\.py)\b', block_text)
    rr = re.findall(r'\bRscript\s+([^\s\'"]+\.R)\b', block_text)
    sh = re.findall(r'\b(?:bash|sh)\s+([^\s\'"]+\.(?:sh|bash))\b', block_text)
    return {
        "python_refs": unique_keep_order(py),
        "r_refs": unique_keep_order(rr),
        "shell_refs": unique_keep_order(sh),
    }


def extract_nextflow_ir(file_path: Path) -> dict:
    text = _safe_read_text(file_path)

    process_blocks = _find_blocks(text, "process")
    workflow_blocks = _find_blocks(text, "workflow")

    all_names = [b["name"] for b in process_blocks + workflow_blocks]
    all_name_set = set(all_names)

    nodes = []
    call_edges = []

    for block in workflow_blocks + process_blocks:
        called = _extract_called_processes_or_workflows(block["body_text"])
        internal_calls = [c for c in called if c in all_name_set and c != block["name"]]
        external_calls = [c for c in called if c not in all_name_set]

        refs = _extract_script_refs(block["body_text"])

        nodes.append({
            "node_type": "workflow" if block in workflow_blocks else "process",
            "name": block["name"],
            "start_line": block["start_line"],
            "end_line": block["end_line"],
            "function_text": block["body_text"],
            "called_symbols": called,
            "internal_calls": internal_calls,
            "external_calls": external_calls,
            "path_candidates": extract_string_path_candidates(block["body_text"]),
            "python_refs": refs["python_refs"],
            "r_refs": refs["r_refs"],
            "shell_refs": refs["shell_refs"],
        })

        for c in internal_calls:
            call_edges.append([block["name"], c])

    include_matches = re.findall(
        r'^\s*include\s+\{([^}]+)\}\s+from\s+[\'"]([^\'"]+)[\'"]',
        text,
        flags=re.MULTILINE
    )
    include_modules = []
    for names, src in include_matches:
        include_modules.append({
            "symbols": [x.strip() for x in re.split(r"[;,]", names) if x.strip()],
            "source": src.strip(),
        })

    return {
        "language": "nextflow",
        "file_level": {
            "defined_functions": [],
            "defined_processes": [b["name"] for b in process_blocks],
            "defined_workflows": [b["name"] for b in workflow_blocks],
            "entry_points": [b["name"] for b in workflow_blocks if b["name"] == "__main__workflow__"] or [b["name"] for b in workflow_blocks[:1]],
            "imports_or_libraries": [],
            "source_dependencies": [x["source"] for x in include_modules],
            "include_modules": include_modules,
            "string_path_candidates": extract_string_path_candidates(text),
            "params_used": unique_keep_order(re.findall(r'\bparams\.([A-Za-z0-9_]+)\b', text)),
        },
        "functions": [],
        "nodes": nodes,
        "call_edges": call_edges,
    }
