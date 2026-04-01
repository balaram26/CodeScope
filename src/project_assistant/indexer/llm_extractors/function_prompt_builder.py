import json


FUNCTION_EXTRACTION_SCHEMA = {
    "function_name": "string",
    "purpose": "string",
    "role": "one of: preprocessing, analysis, modeling, plotting, export, io_helper, utility, orchestration, qc, unknown",
    "likely_inputs": [
        {
            "name": "string",
            "kind": "one of: file_input, checkpoint_input, parameter, in_memory_object, unknown",
            "likely_content": "string"
        }
    ],
    "likely_outputs": [
        {
            "name": "string",
            "kind": "one of: file_output, checkpoint_output, plot_output, in_memory_object, unknown",
            "likely_content": "string"
        }
    ],
    "depends_on_internal_functions": ["string"],
    "depends_on_external_functions": ["string"],
    "notes": ["string"],
    "confidence": "one of: low, medium, high"
}


def build_function_extraction_prompt(
    file_row,
    ir_obj: dict,
    function_obj: dict,
    parser_metadata: dict | None = None,
) -> str:
    file_name = file_row["file_name"]
    rel_path = file_row["relative_path"]
    language = ir_obj.get("language", "unknown")

    file_level = ir_obj.get("file_level", {})
    parser_metadata = parser_metadata or {}

    payload = {
        "file_name": file_name,
        "relative_path": rel_path,
        "language": language,
        "file_defined_functions": file_level.get("defined_functions", []),
        "file_entry_points": file_level.get("entry_points", []),
        "file_imports_or_libraries": file_level.get("imports_or_libraries", []),
        "file_source_dependencies": file_level.get("source_dependencies", []),
        "file_string_path_candidates": file_level.get("string_path_candidates", []),
        "parser_metadata_subset": {
            "line_count": parser_metadata.get("line_count"),
            "input_files": parser_metadata.get("input_files", []),
            "output_files": parser_metadata.get("output_files", []),
            "checkpoint_inputs": parser_metadata.get("checkpoint_inputs", []),
            "checkpoint_outputs": parser_metadata.get("checkpoint_outputs", []),
            "plot_outputs": parser_metadata.get("plot_outputs", []),
            "read_functions_used": parser_metadata.get("read_functions_used", []),
            "write_functions_used": parser_metadata.get("write_functions_used", []),
        },
        "function": {
            "function_name": function_obj.get("function_name"),
            "start_line": function_obj.get("start_line"),
            "end_line": function_obj.get("end_line"),
            "called_symbols": function_obj.get("called_symbols", []),
            "internal_calls": function_obj.get("internal_calls", []),
            "external_calls": function_obj.get("external_calls", []),
            "path_candidates": function_obj.get("path_candidates", []),
            "doc_hint": function_obj.get("doc_hint", ""),
            "function_text": function_obj.get("function_text", ""),
        },
    }

    prompt = f"""
You are extracting structured metadata for ONE function from a scientific codebase.

Return ONLY a valid JSON object.
Do not include markdown fences.
Do not include explanations outside JSON.

Use this exact schema shape:
{json.dumps(FUNCTION_EXTRACTION_SCHEMA, ensure_ascii=False, indent=2)}

Rules:
- Be conservative and grounded in the provided function text and context.
- Prefer names/path candidates explicitly present in the code.
- If a likely input/output is inferred rather than explicit, include it only if reasonably supported.
- For helper functions like save/load/log wrappers, use role "io_helper" or "utility".
- For main orchestration functions, use role "orchestration".
- If unclear, use role "unknown".
- Keep notes short.
- confidence should reflect certainty of the extraction.
- Preserve the exact function_name.

Context JSON:
{json.dumps(payload, ensure_ascii=False, indent=2)}
""".strip()

    return prompt