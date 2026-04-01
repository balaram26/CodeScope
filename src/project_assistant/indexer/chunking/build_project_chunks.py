import argparse
import json
from pathlib import Path

from project_assistant.indexer.config import GLOBAL_INDEX_DIR
from project_assistant.indexer.config import GENERATED_DIR
from project_assistant.indexer.db import db_cursor, init_db


def _safe_load_json(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        return {}


def _norm_list(x):
    if not x:
        return []
    return [str(v) for v in x if v]


def _make_text_from_file_chunk(project_name: str, file_row, merged: dict, summary_text: str | None) -> str:
    parts = [
        f"Project: {project_name}",
        f"Chunk type: file_summary",
        f"File: {file_row['file_name']}",
        f"Relative path: {file_row['relative_path']}",
        f"Language/type: {merged.get('language') or file_row['file_type']}",
        f"Dominant role: {merged.get('dominant_role', 'unknown')}",
    ]

    funcs = _norm_list(merged.get("defined_functions"))
    if funcs:
        parts.append("Defined functions: " + ", ".join(funcs[:12]))

    libs = _norm_list(merged.get("imports_or_libraries"))
    if libs:
        parts.append("Libraries/imports: " + ", ".join(libs[:12]))

    file_inputs = _norm_list(merged.get("final_file_inputs"))
    if file_inputs:
        parts.append("File inputs: " + ", ".join(file_inputs[:10]))

    file_outputs = _norm_list(merged.get("final_file_outputs"))
    if file_outputs:
        parts.append("File outputs: " + ", ".join(file_outputs[:10]))

    checkpoint_inputs = _norm_list(merged.get("final_checkpoint_inputs"))
    if checkpoint_inputs:
        parts.append("Checkpoint inputs: " + ", ".join(checkpoint_inputs[:10]))

    checkpoint_outputs = _norm_list(merged.get("final_checkpoint_outputs"))
    if checkpoint_outputs:
        parts.append("Checkpoint outputs: " + ", ".join(checkpoint_outputs[:10]))

    plot_outputs = _norm_list(merged.get("final_plot_outputs"))
    if plot_outputs:
        parts.append("Plot outputs: " + ", ".join(plot_outputs[:10]))

    params = _norm_list(merged.get("final_parameters"))
    if params:
        parts.append("Important parameters: " + ", ".join(params[:10]))

    notes = _norm_list(merged.get("llm_notes"))
    if notes:
        parts.append("Notes: " + " | ".join(notes[:6]))

    if summary_text:
        parts.append("Summary: " + summary_text)

    return "\n".join(parts)


def _make_text_from_function_chunk(project_name: str, file_row, fn_obj: dict) -> str:
    parts = [
        f"Project: {project_name}",
        f"Chunk type: function_summary",
        f"File: {file_row['file_name']}",
        f"Relative path: {file_row['relative_path']}",
        f"Function: {fn_obj.get('function_name', '__unknown__')}",
        f"Role: {fn_obj.get('role', 'unknown')}",
        f"Purpose: {fn_obj.get('purpose', '')}",
    ]

    ins = []
    for x in fn_obj.get("likely_inputs", []):
        if isinstance(x, dict) and x.get("name"):
            ins.append(f"{x['name']} ({x.get('kind', 'unknown')})")
    if ins:
        parts.append("Likely inputs: " + ", ".join(ins[:10]))

    outs = []
    for x in fn_obj.get("likely_outputs", []):
        if isinstance(x, dict) and x.get("name"):
            outs.append(f"{x['name']} ({x.get('kind', 'unknown')})")
    if outs:
        parts.append("Likely outputs: " + ", ".join(outs[:10]))

    deps_i = _norm_list(fn_obj.get("depends_on_internal_functions"))
    if deps_i:
        parts.append("Internal dependencies: " + ", ".join(deps_i[:10]))

    deps_e = _norm_list(fn_obj.get("depends_on_external_functions"))
    if deps_e:
        parts.append("External dependencies: " + ", ".join(deps_e[:12]))

    notes = _norm_list(fn_obj.get("notes"))
    if notes:
        parts.append("Notes: " + " | ".join(notes[:6]))

    return "\n".join(parts)


def _make_text_from_dataset_chunk(project_name: str, file_row, meta: dict, summary_text: str | None) -> str:
    parts = [
        f"Project: {project_name}",
        f"Chunk type: dataset_summary",
        f"Dataset file: {file_row['file_name']}",
        f"Relative path: {file_row['relative_path']}",
        f"Format: {meta.get('table_format') or file_row['file_ext'] or 'table'}",
        f"Rows: {meta.get('row_count', 'unknown')}",
        f"Columns: {meta.get('column_count', 'unknown')}",
    ]

    cols = _norm_list(meta.get("columns"))
    if cols:
        parts.append("Columns: " + ", ".join(cols[:20]))

    if summary_text:
        parts.append("Summary: " + summary_text)

    return "\n".join(parts)


def _make_text_from_doc_chunk(project_name: str, doc_name: str, doc_text: str) -> str:
    return "\n".join([
        f"Project: {project_name}",
        "Chunk type: project_doc",
        f"Document: {doc_name}",
        doc_text[:6000],
    ])


def build_chunks(project_name: str, output_jsonl: Path):
    chunks = []

    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT p.project_name, f.file_id, f.file_name, f.relative_path, f.file_type, f.file_ext
            FROM files f
            JOIN projects p ON f.project_id = p.project_id
            WHERE p.project_name = ?
              AND COALESCE(f.is_duplicate, 0) = 0
            ORDER BY f.file_id ASC
        """, (project_name,))
        files = cur.fetchall()

    for file_row in files:
        file_id = file_row["file_id"]

        merged = {}
        summary_text = None
        parser_meta = {}

        with db_cursor() as (_, cur):
            cur.execute("""
                SELECT metadata_json
                FROM file_metadata_merged
                WHERE file_id = ?
                ORDER BY merged_id DESC
                LIMIT 1
            """, (file_id,))
            row = cur.fetchone()
            if row:
                merged = _safe_load_json(row["metadata_json"])

            cur.execute("""
                SELECT metadata_json
                FROM file_metadata
                WHERE file_id = ?
                ORDER BY metadata_id DESC
                LIMIT 1
            """, (file_id,))
            row = cur.fetchone()
            if row:
                parser_meta = _safe_load_json(row["metadata_json"])

            cur.execute("""
                SELECT summary_text
                FROM file_summaries
                WHERE file_id = ?
                ORDER BY summary_id DESC
                LIMIT 1
            """, (file_id,))
            row = cur.fetchone()
            if row:
                summary_text = row["summary_text"]

        # file-level chunk
        chunk_text = _make_text_from_file_chunk(project_name, file_row, merged, summary_text)
        chunks.append({
            "chunk_id": f"{project_name}::file::{file_id}",
            "project_name": project_name,
            "chunk_type": "file_summary",
            "file_id": file_id,
            "file_name": file_row["file_name"],
            "relative_path": file_row["relative_path"],
            "text": chunk_text,
            "metadata": {
                "dominant_role": merged.get("dominant_role"),
                "file_type": file_row["file_type"],
                "language": merged.get("language"),
                "file_inputs": merged.get("final_file_inputs", []),
                "file_outputs": merged.get("final_file_outputs", []),
                "checkpoint_inputs": merged.get("final_checkpoint_inputs", []),
                "checkpoint_outputs": merged.get("final_checkpoint_outputs", []),
                "plot_outputs": merged.get("final_plot_outputs", []),
                "parameters": merged.get("final_parameters", []),
            }
        })

        # function-level chunks
        fn_rows = []
        with db_cursor() as (_, cur):
            cur.execute("""
                SELECT function_name, metadata_json
                FROM function_metadata_llm
                WHERE file_id = ?
                ORDER BY llm_function_id ASC
            """, (file_id,))
            fn_rows = cur.fetchall()

        for fn_row in fn_rows:
            fn_obj = _safe_load_json(fn_row["metadata_json"])
            fn_name = fn_obj.get("function_name") or fn_row["function_name"] or "__unknown__"
            fn_text = _make_text_from_function_chunk(project_name, file_row, fn_obj)
            chunks.append({
                "chunk_id": f"{project_name}::function::{file_id}::{fn_name}",
                "project_name": project_name,
                "chunk_type": "function_summary",
                "file_id": file_id,
                "file_name": file_row["file_name"],
                "relative_path": file_row["relative_path"],
                "function_name": fn_name,
                "text": fn_text,
                "metadata": {
                    "role": fn_obj.get("role"),
                    "confidence": fn_obj.get("confidence"),
                    "likely_inputs": fn_obj.get("likely_inputs", []),
                    "likely_outputs": fn_obj.get("likely_outputs", []),
                }
            })

        # dataset chunk for results
        if file_row["file_type"] in {"result", "report", "table", "metadata"} or file_row["file_ext"] in {".csv", ".tsv", ".xlsx", ".xls"}:
            ds_text = _make_text_from_dataset_chunk(project_name, file_row, parser_meta, summary_text)
            chunks.append({
                "chunk_id": f"{project_name}::dataset::{file_id}",
                "project_name": project_name,
                "chunk_type": "dataset_summary",
                "file_id": file_id,
                "file_name": file_row["file_name"],
                "relative_path": file_row["relative_path"],
                "text": ds_text,
                "metadata": {
                    "columns": parser_meta.get("columns", [])[:20],
                    "row_count": parser_meta.get("row_count"),
                    "column_count": parser_meta.get("column_count"),
                }
            })

    # generated docs
    gen_dir = GENERATED_DIR / project_name

    if gen_dir.exists():
        for doc_path in sorted(gen_dir.rglob("*.md")):
            try:
                doc_text = doc_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue

            doc_name = doc_path.name.lower()
            doc_rel = str(doc_path).lower()

            if doc_name.endswith(".dossier.md") and "/dossiers/workflows/" in doc_rel:
                chunk_type = "workflow_dossier"
                role = "workflow"
            elif doc_name.endswith(".dossier.md") and "/dossiers/scripts/" in doc_rel:
                chunk_type = "script_dossier"
                role = "script"
            else:
                chunk_type = "project_doc"
                role = "summary"

            chunks.append({
                "chunk_id": f"{project_name}::doc::{doc_path.name}",
                "project_name": project_name,
                "chunk_type": chunk_type,
                "file_id": None,
                "file_name": doc_path.name,
                "relative_path": str(doc_path),
                "text": _make_text_from_doc_chunk(project_name, doc_path.name, doc_text),
                "metadata": {
                    "doc_role": role,
                    "generated_doc_name": doc_path.name,
                }
            })

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with output_jsonl.open("w", encoding="utf-8") as f:
        for ch in chunks:
            f.write(json.dumps(ch, ensure_ascii=False) + "\n")

    print(f"[OK] Wrote {len(chunks)} chunks to {output_jsonl}")


def main():
    parser = argparse.ArgumentParser(description="Build structured project chunks from DB.")
    parser.add_argument("--project-name", required=True)
    parser.add_argument(
        "--output-jsonl",
        default=str(GLOBAL_INDEX_DIR / "chunks_project.jsonl"),
    )
    args = parser.parse_args()

    init_db()
    build_chunks(args.project_name, Path(args.output_jsonl))


if __name__ == "__main__":
    main()
