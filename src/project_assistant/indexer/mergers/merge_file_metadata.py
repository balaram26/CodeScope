import json
import re


ROLE_PRIORITY = {
    "orchestration": 100,
    "analysis": 90,
    "preprocessing": 80,
    "modeling": 70,
    "export": 60,
    "qc": 55,
    "plotting": 40,
    "io_helper": 20,
    "utility": 10,
    "unknown": 0,
}

WEAK_NAMES = {
    "path", "out", "file", "data", "out_png", "out_csv"
}


def _unique_keep_order(items):
    seen = set()
    out = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _normalize_artifact_name(name: str) -> str:
    if not name:
        return name
    name = name.strip()
    name = name.replace("%s", "*")
    name = name.replace("{}", "*")
    name = re.sub(r"\s+", " ", name)
    return name


def _simplify_path(name: str) -> str:
    if not name:
        return name
    if "/" in name:
        return name.split("/")[-1]
    return name


def _is_useful_name(name: str) -> bool:
    if not name:
        return False
    return name.lower() not in WEAK_NAMES


def _choose_dominant_role(roles: list[str], language: str | None = None) -> str:
    if not roles:
        if language == "nextflow":
            return "orchestration"
        return "unknown"

    roles = [r for r in roles if r]

    for preferred in ["orchestration", "analysis", "preprocessing", "modeling", "export", "qc"]:
        if preferred in roles:
            return preferred

    counts = {}
    for r in roles:
        counts[r] = counts.get(r, 0) + 1

    ranked = sorted(
        counts.items(),
        key=lambda kv: (-kv[1], -ROLE_PRIORITY.get(kv[0], 0), kv[0])
    )
    return ranked[0][0]


def _normalize_name_list(items: list[str]) -> list[str]:
    out = []
    for x in items:
        x = _normalize_artifact_name(x)
        x = _simplify_path(x)
        if _is_useful_name(x):
            out.append(x)
    return _unique_keep_order(out)


def merge_file_metadata(
    parser_metadata: dict,
    ir_obj: dict,
    function_llm_rows: list,
) -> dict:
    parser_metadata = parser_metadata or {}
    ir_obj = ir_obj or {}
    function_llm_rows = function_llm_rows or []

    language = parser_metadata.get("language") or ir_obj.get("language")

    fn_llm = []
    roles = []
    notes = []

    llm_inputs_raw = []
    llm_outputs_raw = []

    for row in function_llm_rows:
        try:
            obj = json.loads(row["metadata_json"])
        except Exception:
            continue

        fn_llm.append(obj)

        role = obj.get("role")
        if role:
            roles.append(role)

        for x in obj.get("likely_inputs", []):
            if isinstance(x, dict) and x.get("name"):
                x2 = dict(x)
                x2["name"] = _simplify_path(_normalize_artifact_name(x2["name"]))
                llm_inputs_raw.append(x2)

        for x in obj.get("likely_outputs", []):
            if isinstance(x, dict) and x.get("name"):
                x2 = dict(x)
                x2["name"] = _simplify_path(_normalize_artifact_name(x2["name"]))
                llm_outputs_raw.append(x2)

        for n in obj.get("notes", []):
            if n:
                notes.append(n)

    dominant_role = _choose_dominant_role(roles, language=language)

    final_file_inputs = []
    final_file_outputs = []
    final_checkpoint_inputs = []
    final_checkpoint_outputs = []
    final_plot_outputs = []
    final_in_memory_inputs = []
    final_in_memory_outputs = []
    final_parameters = []

    for item in llm_inputs_raw:
        name = item["name"]
        kind = (item.get("kind") or "unknown").strip().lower()
        if not _is_useful_name(name):
            continue

        if kind == "file_input":
            final_file_inputs.append(name)
        elif kind == "checkpoint_input":
            final_checkpoint_inputs.append(name)
        elif kind == "in_memory_object":
            final_in_memory_inputs.append(name)
        elif kind == "parameter":
            final_parameters.append(name)

    for item in llm_outputs_raw:
        name = item["name"]
        kind = (item.get("kind") or "unknown").strip().lower()
        if not _is_useful_name(name):
            continue

        if kind == "file_output":
            final_file_outputs.append(name)
        elif kind == "checkpoint_output":
            final_checkpoint_outputs.append(name)
        elif kind == "plot_output":
            final_plot_outputs.append(name)
        elif kind == "in_memory_object":
            final_in_memory_outputs.append(name)

    parser_input_files = _normalize_name_list(parser_metadata.get("input_files", []))
    parser_output_files = _normalize_name_list(parser_metadata.get("output_files", []))
    parser_checkpoint_inputs = _normalize_name_list(parser_metadata.get("checkpoint_inputs", []))
    parser_checkpoint_outputs = _normalize_name_list(parser_metadata.get("checkpoint_outputs", []))
    parser_plot_outputs = _normalize_name_list(parser_metadata.get("plot_outputs", []))
    ir_string_path_candidates = _normalize_name_list(
        ir_obj.get("file_level", {}).get("string_path_candidates", [])
    )

    # Nextflow-specific fields from parser and IR
    parser_process_names = _unique_keep_order(parser_metadata.get("process_names", []))
    parser_workflow_names = _unique_keep_order(parser_metadata.get("workflow_names", []))
    parser_includes = parser_metadata.get("includes", []) or []
    parser_params_used = _unique_keep_order(parser_metadata.get("params_used", []))
    parser_python_refs = _normalize_name_list(parser_metadata.get("python_refs", []))
    parser_r_refs = _normalize_name_list(parser_metadata.get("r_refs", []))
    parser_shell_refs = _normalize_name_list(parser_metadata.get("shell_refs", []))
    parser_channel_ops = _unique_keep_order(parser_metadata.get("channel_ops", []))

    ir_file_level = ir_obj.get("file_level", {}) or {}
    ir_defined_processes = _unique_keep_order(ir_file_level.get("defined_processes", []))
    ir_defined_workflows = _unique_keep_order(ir_file_level.get("defined_workflows", []))
    ir_include_modules = ir_file_level.get("include_modules", []) or []
    ir_params_used = _unique_keep_order(ir_file_level.get("params_used", []))
    ir_nodes = ir_obj.get("nodes", []) or []

    merged = {
        "language": language,
        "line_count": parser_metadata.get("line_count"),
        "defined_functions": ir_file_level.get("defined_functions", []),
        "entry_points": ir_file_level.get("entry_points", []),
        "imports_or_libraries": _unique_keep_order(
            (parser_metadata.get("libraries", []) or [])
            + (parser_metadata.get("imports", []) or [])
            + (ir_file_level.get("imports_or_libraries", []) or [])
        ),
        "source_dependencies": _normalize_name_list(
            (parser_metadata.get("source_dependencies", []) or [])
            + (ir_file_level.get("source_dependencies", []) or [])
        ),
        "parser_input_files": parser_input_files,
        "parser_output_files": parser_output_files,
        "parser_checkpoint_inputs": parser_checkpoint_inputs,
        "parser_checkpoint_outputs": parser_checkpoint_outputs,
        "parser_plot_outputs": parser_plot_outputs,
        "ir_string_path_candidates": ir_string_path_candidates,
        "call_edges": ir_obj.get("call_edges", []),

        # Nextflow-aware merged fields
        "defined_processes": ir_defined_processes or parser_process_names,
        "defined_workflows": ir_defined_workflows or parser_workflow_names,
        "include_modules": ir_include_modules or parser_includes,
        "params_used": ir_params_used or parser_params_used,
        "python_refs": parser_python_refs,
        "r_refs": parser_r_refs,
        "shell_refs": parser_shell_refs,
        "channel_ops": parser_channel_ops,
        "nodes": ir_nodes,

        "dominant_role": dominant_role,
        "secondary_roles": _unique_keep_order(roles),
        "llm_inputs_raw": llm_inputs_raw,
        "llm_outputs_raw": llm_outputs_raw,
        "llm_notes": _unique_keep_order(notes),
        "function_level_llm_count": len(fn_llm),
        "function_level_llm": fn_llm,

        "final_file_inputs": _unique_keep_order(parser_input_files + final_file_inputs),
        "final_file_outputs": _unique_keep_order(parser_output_files + final_file_outputs),
        "final_checkpoint_inputs": _unique_keep_order(parser_checkpoint_inputs + final_checkpoint_inputs),
        "final_checkpoint_outputs": _unique_keep_order(parser_checkpoint_outputs + final_checkpoint_outputs),
        "final_plot_outputs": _unique_keep_order(parser_plot_outputs + final_plot_outputs),
        "final_in_memory_inputs": _unique_keep_order(final_in_memory_inputs),
        "final_in_memory_outputs": _unique_keep_order(final_in_memory_outputs),
        "final_parameters": _unique_keep_order(final_parameters),
    }

    return merged