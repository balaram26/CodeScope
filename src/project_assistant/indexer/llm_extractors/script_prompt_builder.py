import json


SCRIPT_EXTRACTION_SCHEMA = {
    "script_name": "string",
    "purpose": "string",
    "role": "one of: preprocessing, analysis, modeling, plotting, export, orchestration, qc, utility, unknown",
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
    "notes": ["string"],
    "confidence": "one of: low, medium, high"
}


def build_script_extraction_prompt(
    file_row,
    ir_obj: dict,
    parser_metadata: dict | None = None,
) -> str:
    parser_metadata = parser_metadata or {}

    payload = {
        "file_name": file_row["file_name"],
        "relative_path": file_row["relative_path"],
        "language": ir_obj.get("language", "unknown"),
        "file_level": ir_obj.get("file_level", {}),
        "parser_metadata_subset": {
            "line_count": parser_metadata.get("line_count"),
            "input_files": parser_metadata.get("input_files", []),
            "output_files": parser_metadata.get("output_files", []),
            "checkpoint_inputs": parser_metadata.get("checkpoint_inputs", []),
            "checkpoint_outputs": parser_metadata.get("checkpoint_outputs", []),
            "plot_outputs": parser_metadata.get("plot_outputs", []),
            "generic_file_references": parser_metadata.get("generic_file_references", []),
            "read_functions_used": parser_metadata.get("read_functions_used", []),
            "write_functions_used": parser_metadata.get("write_functions_used", []),
            "preview": parser_metadata.get("preview", ""),
        },
    }

    prompt = f"""
You are extracting structured metadata for ONE scientific script that may not define explicit functions.

Return ONLY a valid JSON object.
Do not include markdown fences.
Do not include explanations outside JSON.

Use this exact schema shape:
{json.dumps(SCRIPT_EXTRACTION_SCHEMA, ensure_ascii=False, indent=2)}

Rules:
- Be conservative and grounded in the provided script context.
- Prefer path names explicitly present in the code/metadata.
- Infer likely inputs/outputs only when reasonably supported.
- Keep notes short.
- confidence should reflect certainty.

Context JSON:
{json.dumps(payload, ensure_ascii=False, indent=2)}
""".strip()

    return prompt
