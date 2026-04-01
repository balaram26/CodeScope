from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from project_assistant.indexer.config import DB_PATH
from project_assistant.indexer.config import GENERATED_DIR
GENERATED_ROOT = GENERATED_DIR

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def _first_present(cols: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


def _safe_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def _fmt_list(title: str, value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        items = [str(x) for x in value if x not in (None, "", [])]
        if not items:
            return ""
        return f"### {title}\n" + "\n".join(f"- {x}" for x in items) + "\n\n"
    return f"### {title}\n- {value}\n\n"


def _fmt_text(title: str, value: Any) -> str:
    if value in (None, ""):
        return ""
    return f"### {title}\n{value}\n\n"


def _parse_file_ids_arg(file_ids_arg: str | None) -> set[int]:
    if not file_ids_arg:
        return set()
    out: set[int] = set()
    for token in file_ids_arg.split(","):
        token = token.strip()
        if not token:
            continue
        out.add(int(token))
    return out


def _load_project_id(conn: sqlite3.Connection, project_name: str) -> int:
    row = conn.execute(
        "SELECT project_id FROM projects WHERE project_name = ?",
        (project_name,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Project not found: {project_name}")
    return int(row["project_id"])


def _load_file_rows(conn: sqlite3.Connection, project_id: int) -> list[sqlite3.Row]:
    file_cols = _columns(conn, "files")
    rel_col = _first_present(file_cols, ["relative_path", "rel_path", "path"])
    ext_col = _first_present(file_cols, ["file_ext", "ext"])
    dup_col = _first_present(file_cols, ["is_duplicate"])
    name_col = _first_present(file_cols, ["file_name", "name"])

    if rel_col is None or ext_col is None:
        raise RuntimeError("files table is missing required path/ext columns")

    where = [f"project_id = ?"]
    params: list[Any] = [project_id]

    where.append(f"lower({ext_col}) IN ('.nf', '.r', '.py', '.sh')")
    if dup_col:
        where.append(f"coalesce({dup_col}, 0) = 0")

    sql = f"""
        SELECT file_id, {rel_col} AS relative_path, {ext_col} AS file_ext
        {f", {name_col} AS file_name" if name_col else ""}
        FROM files
        WHERE {" AND ".join(where)}
        ORDER BY relative_path
    """
    return conn.execute(sql, params).fetchall()


def _load_file_summaries(conn: sqlite3.Connection) -> dict[int, str]:
    if not _table_exists(conn, "file_summaries"):
        return {}

    cols = _columns(conn, "file_summaries")
    text_col = _first_present(cols, ["summary_text", "summary_md", "summary", "text"])
    if text_col is None or "file_id" not in cols:
        return {}

    rows = conn.execute(
        f"SELECT file_id, {text_col} AS summary_text FROM file_summaries"
    ).fetchall()
    return {int(r["file_id"]): (r["summary_text"] or "") for r in rows}


def _load_merged_metadata(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    if not _table_exists(conn, "file_metadata_merged"):
        return {}

    cols = _columns(conn, "file_metadata_merged")
    if "file_id" not in cols or "metadata_json" not in cols:
        return {}

    version_col = _first_present(cols, ["merged_id", "id"])
    if version_col is None:
        return {}

    rows = conn.execute(
        f"""
        SELECT m.file_id, m.metadata_json
        FROM file_metadata_merged m
        JOIN (
            SELECT file_id, max({version_col}) AS max_ver
            FROM file_metadata_merged
            GROUP BY file_id
        ) latest
          ON latest.file_id = m.file_id
         AND latest.max_ver = m.{version_col}
        """
    ).fetchall()

    out: dict[int, dict[str, Any]] = {}
    for r in rows:
        out[int(r["file_id"])] = _safe_json(r["metadata_json"]) or {}
    return out


def _load_function_metadata(conn: sqlite3.Connection) -> dict[int, list[dict[str, Any]]]:
    if not _table_exists(conn, "function_metadata_llm"):
        return {}

    cols = _columns(conn, "function_metadata_llm")
    if "file_id" not in cols:
        return {}

    rows = conn.execute("SELECT * FROM function_metadata_llm").fetchall()
    grouped: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        d = dict(r)
        grouped.setdefault(int(d["file_id"]), []).append(d)
    return grouped


def _render_function_section(func_rows: list[dict[str, Any]]) -> str:
    if not func_rows:
        return ""

    chunks = ["## Function-level evidence\n"]
    for row in func_rows[:30]:
        fn = row.get("function_name") or row.get("name") or "(unknown function)"
        purpose = row.get("purpose") or row.get("summary") or row.get("description") or row.get("notes")
        inputs = _safe_json(row.get("inputs_json") or row.get("inputs"))
        outputs = _safe_json(row.get("outputs_json") or row.get("outputs"))

        chunks.append(f"### {fn}\n")
        if purpose:
            chunks.append(f"- Purpose: {purpose}\n")
        if inputs:
            if isinstance(inputs, list):
                chunks.append("- Inputs: " + ", ".join(map(str, inputs)) + "\n")
            else:
                chunks.append(f"- Inputs: {inputs}\n")
        if outputs:
            if isinstance(outputs, list):
                chunks.append("- Outputs: " + ", ".join(map(str, outputs)) + "\n")
            else:
                chunks.append(f"- Outputs: {outputs}\n")
        chunks.append("\n")

    return "".join(chunks) + "\n"


def _build_dossier(
    relative_path: str,
    file_ext: str,
    summary_text: str,
    metadata: dict[str, Any],
    func_rows: list[dict[str, Any]],
) -> tuple[str, str]:
    language = metadata.get("language")
    imports_ = metadata.get("imports_or_libraries") or metadata.get("imports")
    deps = metadata.get("source_dependencies")
    entry_points = metadata.get("entry_points")
    defined_functions = metadata.get("defined_functions")

    is_workflow = (file_ext or "").lower() == ".nf" or str(language or "").lower() == "nextflow"
    title = "WORKFLOW DOSSIER" if is_workflow else "SCRIPT DOSSIER"

    body = []
    body.append(f"# {title}\n\n")
    body.append(f"## File\n- Path: {relative_path}\n- Extension: {file_ext}\n")
    if language:
        body.append(f"- Language: {language}\n")
    body.append("\n")

    body.append(_fmt_text("File summary", summary_text))
    body.append(_fmt_list("Entry points", entry_points))
    body.append(_fmt_list("Imports / libraries", imports_))
    body.append(_fmt_list("Declared dependencies / linked files", deps))
    body.append(_fmt_list("Defined functions", defined_functions))
    body.append(_render_function_section(func_rows))

    if is_workflow:
        body.append(
            "## Interpretation hints\n"
            "- This file should be treated as orchestration/workflow context.\n"
            "- When answering questions, connect this dossier with called modules, stage outputs, and linked artifacts.\n\n"
        )
        doc_kind = "workflow_dossier"
    else:
        body.append(
            "## Interpretation hints\n"
            "- This file should be treated as a script-level unit of reasoning.\n"
            "- When answering questions, combine this dossier with related result tables, metadata sheets, and metrics files.\n\n"
        )
        doc_kind = "script_dossier"

    return doc_kind, "".join(body)


def _safe_slug(relative_path: str) -> str:
    safe = relative_path.replace("\\", "__").replace("/", "__")
    safe = safe.replace(":", "_").replace(" ", "_")
    return safe


def generate_file_dossiers(project_name: str, file_ids: set[int] | None = None) -> dict[str, Any]:
    out_dir = GENERATED_ROOT / project_name / "dossiers"
    script_dir = out_dir / "scripts"
    workflow_dir = out_dir / "workflows"
    script_dir.mkdir(parents=True, exist_ok=True)
    workflow_dir.mkdir(parents=True, exist_ok=True)

    conn = _connect()
    try:
        project_id = _load_project_id(conn, project_name)
        file_rows = _load_file_rows(conn, project_id)
        if file_ids:
            file_rows = [row for row in file_rows if int(row["file_id"]) in file_ids]

        file_summaries = _load_file_summaries(conn)
        merged_metadata = _load_merged_metadata(conn)
        function_metadata = _load_function_metadata(conn)
    finally:
        conn.close()

    workflow_count = 0
    script_count = 0
    manifest_lines: list[str] = []

    for row in file_rows:
        file_id = int(row["file_id"])
        relative_path = row["relative_path"]
        file_ext = row["file_ext"] or ""

        summary_text = file_summaries.get(file_id, "")
        metadata = merged_metadata.get(file_id, {}) or {}
        func_rows = function_metadata.get(file_id, []) or []

        doc_kind, text = _build_dossier(
            relative_path=relative_path,
            file_ext=file_ext,
            summary_text=summary_text,
            metadata=metadata,
            func_rows=func_rows,
        )

        safe_name = _safe_slug(relative_path) + ".dossier.md"

        if doc_kind == "workflow_dossier":
            doc_path = workflow_dir / safe_name
            workflow_count += 1
        else:
            doc_path = script_dir / safe_name
            script_count += 1

        doc_path.write_text(text, encoding="utf-8")

        manifest_lines.append(json.dumps({
            "file_id": file_id,
            "relative_path": relative_path,
            "doc_kind": doc_kind,
            "doc_path": str(doc_path),
        }, ensure_ascii=False))

    manifest_path = out_dir / "DOSSIER_MANIFEST.jsonl"
    manifest_path.write_text("\n".join(manifest_lines) + ("\n" if manifest_lines else ""), encoding="utf-8")

    return {
        "project_name": project_name,
        "workflow_dossiers": workflow_count,
        "script_dossiers": script_count,
        "dossier_root": str(out_dir),
        "manifest_path": str(manifest_path),
        "incremental_file_ids": sorted(file_ids) if file_ids else [],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-name", required=True)
    ap.add_argument("--file-ids", default="", help="Optional comma-separated file_ids for incremental dossier regeneration")
    args = ap.parse_args()

    file_ids = _parse_file_ids_arg(args.file_ids)
    out = generate_file_dossiers(args.project_name, file_ids=file_ids or None)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()