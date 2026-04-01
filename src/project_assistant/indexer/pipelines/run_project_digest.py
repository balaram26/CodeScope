from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from project_assistant.indexer.config import GLOBAL_INDEX_DIR
from project_assistant.indexer.db import db_cursor, get_project_by_name, init_db


STAGES = [
    "ingest",
    "parse",
    "dedup",
    "ir",
    "llm",
    "merge",
    "reset_summary_status",
    "summaries",
    "dossiers",
    "docs",
    "chunks",
    "index",
]


def _run_cmd(cmd: list[str], env: dict[str, str] | None = None) -> None:
    print(f"\n[RUN] {' '.join(cmd)}")
    subprocess.run(cmd, check=True, env=env)


def _python_module_cmd(module: str, *args: str) -> list[str]:
    return [sys.executable, "-m", module, *args]


def _stage_selected(stage: str, from_stage: str, to_stage: str) -> bool:
    i = STAGES.index(stage)
    return STAGES.index(from_stage) <= i <= STAGES.index(to_stage)


def _ensure_project_exists(project_name: str) -> None:
    row = get_project_by_name(project_name)
    if not row:
        raise ValueError(f"Project not found in DB: {project_name}")


def _parse_file_ids_arg(file_ids: str | None) -> list[int]:
    if not file_ids:
        return []
    out: list[int] = []
    for token in file_ids.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(int(token))
    return out


def _append_file_ids(cmd: list[str], file_ids: list[int]) -> list[str]:
    if file_ids:
        cmd.extend(["--file-ids", ",".join(str(x) for x in file_ids)])
    return cmd


def _reset_canonical_files_to_parsed(project_name: str, file_ids: list[int] | None = None) -> None:
    file_ids = file_ids or []

    with db_cursor() as (_, cur):
        if file_ids:
            placeholders = ",".join("?" for _ in file_ids)
            params = [project_name, *file_ids]
            cur.execute(
                f"""
                UPDATE files
                SET status = 'parsed'
                WHERE project_id = (
                    SELECT project_id
                    FROM projects
                    WHERE project_name = ?
                )
                AND COALESCE(is_duplicate, 0) = 0
                AND file_id IN ({placeholders})
                """,
                params,
            )
            print(f"[OK] Reset targeted canonical files to status='parsed' for project: {project_name} file_ids={file_ids}")
        else:
            cur.execute(
                """
                UPDATE files
                SET status = 'parsed'
                WHERE project_id = (
                    SELECT project_id
                    FROM projects
                    WHERE project_name = ?
                )
                AND COALESCE(is_duplicate, 0) = 0
                """,
                (project_name,),
            )
            print(f"[OK] Reset canonical files to status='parsed' for project: {project_name}")


def run_project_digest(
    *,
    project_name: str,
    input_path: str | None,
    source_kind: str,
    bundle_name: str | None,
    create_if_missing: bool,
    config_path: str,
    task_name: str,
    model_label: str,
    max_tokens: int,
    embedding_model: str,
    from_stage: str,
    to_stage: str,
    skip_llm: bool,
    skip_index: bool,
    merge_version: str,
    ext: str | None = None,
    force_ir: bool = False,
    force_llm: bool = False,
    output_jsonl: str | None = None,
    index_out: str | None = None,
    embeddings_out: str | None = None,
    meta_out: str | None = None,
    file_ids: list[int] | None = None,
    rebuild_docs: bool = False,
) -> dict:
    init_db()

    env = os.environ.copy()
    file_ids = file_ids or []

    chunks_jsonl = output_jsonl or str(GLOBAL_INDEX_DIR / f"{project_name}_chunks.jsonl")
    faiss_index = index_out or str(GLOBAL_INDEX_DIR / f"{project_name}.faiss.index")
    embeddings_npy = embeddings_out or str(GLOBAL_INDEX_DIR / f"{project_name}.embeddings.npy")
    chunks_meta = meta_out or str(GLOBAL_INDEX_DIR / f"{project_name}_chunks_meta.jsonl")

    if _stage_selected("ingest", from_stage, to_stage) and input_path:
        cmd = _python_module_cmd(
            "project_assistant.indexer.run_project_ingest",
            "--input", input_path,
            "--project-name", project_name,
            "--source-kind", source_kind,
            "--bundle-name", bundle_name or "initial_upload",
        )
        if create_if_missing:
            cmd.append("--create-if-missing")
        _run_cmd(cmd, env=env)

    _ensure_project_exists(project_name)

    if _stage_selected("parse", from_stage, to_stage):
        cmd = _python_module_cmd(
            "project_assistant.indexer.processors.process_registered_files",
            "--project-name", project_name,
        )
        _append_file_ids(cmd, file_ids)
        _run_cmd(cmd, env=env)

    if _stage_selected("dedup", from_stage, to_stage):
        cmd = _python_module_cmd(
            "project_assistant.indexer.processors.deduplicate_files",
            "--project-name", project_name,
        )
        _append_file_ids(cmd, file_ids)
        _run_cmd(cmd, env=env)

    if _stage_selected("ir", from_stage, to_stage):
        cmd = _python_module_cmd(
            "project_assistant.indexer.processors.build_script_ir",
            "--project-name", project_name,
        )
        _append_file_ids(cmd, file_ids)
        if ext:
            cmd.extend(["--ext", ext])
        if force_ir:
            cmd.append("--force")
        _run_cmd(cmd, env=env)

    if _stage_selected("llm", from_stage, to_stage) and not skip_llm:
        cmd = _python_module_cmd(
            "project_assistant.indexer.llm_extractors.run_function_llm_extract",
            "--project-name", project_name,
            "--config-path", config_path,
            "--task-name", task_name,
            "--model-label", model_label,
            "--max-tokens", str(max_tokens),
        )
        _append_file_ids(cmd, file_ids)
        if ext:
            cmd.extend(["--ext", ext])
        if force_llm:
            cmd.append("--force")
        _run_cmd(cmd, env=env)

    if _stage_selected("merge", from_stage, to_stage):
        cmd = _python_module_cmd(
            "project_assistant.indexer.processors.build_merged_file_metadata",
            "--project-name", project_name,
            "--model-name", model_label,
            "--merge-version", merge_version,
        )
        _append_file_ids(cmd, file_ids)
        _run_cmd(cmd, env=env)

    if _stage_selected("reset_summary_status", from_stage, to_stage) or from_stage == "summaries":
        _reset_canonical_files_to_parsed(project_name, file_ids=file_ids)

    if _stage_selected("summaries", from_stage, to_stage):
        cmd = _python_module_cmd(
            "project_assistant.indexer.processors.summarize_parsed_files",
            "--project-name", project_name,
        )
        _append_file_ids(cmd, file_ids)
        _run_cmd(cmd, env=env)

    if _stage_selected("dossiers", from_stage, to_stage):
        cmd = _python_module_cmd(
            "project_assistant.indexer.generators.generate_file_dossiers",
            "--project-name", project_name,
        )
        _append_file_ids(cmd, file_ids)
        _run_cmd(cmd, env=env)

    # Project docs can stay full-project by default; only rebuild when explicitly requested.
    if _stage_selected("docs", from_stage, to_stage) and (rebuild_docs or not file_ids):
        _run_cmd(
            _python_module_cmd(
                "project_assistant.indexer.generators.generate_project_docs",
                "--project-name", project_name,
            ),
            env=env,
        )

    if _stage_selected("chunks", from_stage, to_stage):
        _run_cmd(
            _python_module_cmd(
                "project_assistant.indexer.chunking.build_project_chunks",
                "--project-name", project_name,
                "--output-jsonl", chunks_jsonl,
            ),
            env=env,
        )

    if _stage_selected("index", from_stage, to_stage) and not skip_index:
        _run_cmd(
            _python_module_cmd(
                "project_assistant.indexer.chunking.build_project_index",
                "--chunks-jsonl", chunks_jsonl,
                "--index-out", faiss_index,
                "--embeddings-out", embeddings_npy,
                "--meta-out", chunks_meta,
                "--embedding-model", embedding_model,
            ),
            env=env,
        )

    result = {
        "project_name": project_name,
        "chunks_jsonl": chunks_jsonl,
        "faiss_index": faiss_index,
        "embeddings_npy": embeddings_npy,
        "chunks_meta": chunks_meta,
        "from_stage": from_stage,
        "to_stage": to_stage,
        "skip_llm": skip_llm,
        "skip_index": skip_index,
        "merge_version": merge_version,
        "ext": ext,
        "force_ir": force_ir,
        "force_llm": force_llm,
        "file_ids": file_ids,
        "rebuild_docs": rebuild_docs,
    }

    print("\n[OK] Project digestion completed")
    print(json.dumps(result, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description="Run digestion pipeline for one project.")
    parser.add_argument("--project-name", required=True, help="Project name")
    parser.add_argument("--input", default=None, help="Optional input bundle/folder/zip to ingest first")
    parser.add_argument("--source-kind", default="mixed", help="Source kind for ingest")
    parser.add_argument("--bundle-name", default="initial_upload", help="Bundle name for ingest")
    parser.add_argument("--create-if-missing", action="store_true", help="Create project if missing during ingest")

    parser.add_argument("--config-path", required=True, help="YAML config path for LLMService")
    parser.add_argument("--task-name", default="function_metadata_extract", help="LLM task name from YAML")
    parser.add_argument("--model-label", default="local_llm_v1", help="Stored model label")
    parser.add_argument("--max-tokens", type=int, default=1200, help="Max tokens for LLM extraction")
    parser.add_argument("--embedding-model", default="BAAI/bge-base-en-v1.5", help="Embedding model for FAISS build")

    parser.add_argument("--from-stage", choices=STAGES, default="parse")
    parser.add_argument("--to-stage", choices=STAGES, default="index")
    parser.add_argument("--skip-llm", action="store_true")
    parser.add_argument("--skip-index", action="store_true")
    parser.add_argument("--merge-version", default="merge_v4")

    parser.add_argument("--output-jsonl", default=None)
    parser.add_argument("--index-out", default=None)
    parser.add_argument("--embeddings-out", default=None)
    parser.add_argument("--meta-out", default=None)

    parser.add_argument("--ext", default=None, help="Optional extension filter for targeted rebuilds, e.g. .nf")
    parser.add_argument("--force-ir", action="store_true")
    parser.add_argument("--force-llm", action="store_true")
    parser.add_argument("--file-ids", default="", help="Comma-separated file_ids for incremental digest")
    parser.add_argument("--rebuild-docs", action="store_true", help="Also rebuild project docs during incremental runs")

    args = parser.parse_args()

    run_project_digest(
        project_name=args.project_name,
        input_path=args.input,
        source_kind=args.source_kind,
        bundle_name=args.bundle_name,
        create_if_missing=args.create_if_missing,
        config_path=args.config_path,
        task_name=args.task_name,
        model_label=args.model_label,
        max_tokens=args.max_tokens,
        embedding_model=args.embedding_model,
        from_stage=args.from_stage,
        to_stage=args.to_stage,
        skip_llm=args.skip_llm,
        skip_index=args.skip_index,
        merge_version=args.merge_version,
        ext=args.ext,
        force_ir=args.force_ir,
        force_llm=args.force_llm,
        output_jsonl=args.output_jsonl,
        index_out=args.index_out,
        embeddings_out=args.embeddings_out,
        meta_out=args.meta_out,
        file_ids=_parse_file_ids_arg(args.file_ids),
        rebuild_docs=args.rebuild_docs,
    )


if __name__ == "__main__":
    main()