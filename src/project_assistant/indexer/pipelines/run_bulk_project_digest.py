from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from project_assistant.indexer.config import PROJECTS_INBOX_DIR
from project_assistant.indexer.db import db_cursor, init_db
from project_assistant.indexer.pipelines.run_project_digest import run_project_digest


def _slugify_project_name(name: str) -> str:
    name = Path(name).stem
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9._-]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name or "project"


def _list_existing_projects() -> list[str]:
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT project_name
            FROM projects
            ORDER BY project_name ASC
        """)
        return [row["project_name"] for row in cur.fetchall()]


def _discover_inbox_inputs(inbox_dir: Path) -> list[Path]:
    items = []
    if not inbox_dir.exists():
        return items

    for p in sorted(inbox_dir.iterdir()):
        if p.name.startswith("."):
            continue
        if p.is_dir():
            items.append(p)
        elif p.suffix.lower() in {".zip", ".tar", ".gz", ".tgz"}:
            items.append(p)
    return items


def main():
    parser = argparse.ArgumentParser(description="Run digestion pipeline for many projects.")
    parser.add_argument("--config-path", required=True, help="YAML config path for LLMService")
    parser.add_argument("--task-name", default="function_metadata_extract")
    parser.add_argument("--model-label", default="local_llm_v1")
    parser.add_argument("--max-tokens", type=int, default=1200)
    parser.add_argument("--embedding-model", default="BAAI/bge-base-en-v1.5")
    parser.add_argument("--source-kind", default="mixed")
    parser.add_argument("--create-if-missing", action="store_true")

    parser.add_argument("--from-stage", default="parse")
    parser.add_argument("--to-stage", default="index")
    parser.add_argument("--skip-llm", action="store_true")
    parser.add_argument("--skip-index", action="store_true")
    parser.add_argument("--merge-version", default="merge_v4")

    parser.add_argument("--project-names", nargs="*", default=None, help="Explicit existing project names to digest")
    parser.add_argument("--from-inbox", action="store_true", help="Discover bundles/folders in inbox and ingest+digest")
    parser.add_argument("--inbox-dir", default=str(PROJECTS_INBOX_DIR))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--report-json", default=None)

    args = parser.parse_args()
    init_db()

    jobs: list[dict] = []

    if args.from_inbox:
        inbox_items = _discover_inbox_inputs(Path(args.inbox_dir))
        for p in inbox_items:
            jobs.append({
                "project_name": _slugify_project_name(p.name),
                "input_path": str(p),
                "bundle_name": p.stem,
            })

    elif args.project_names:
        for name in args.project_names:
            jobs.append({
                "project_name": name,
                "input_path": None,
                "bundle_name": None,
            })

    else:
        for name in _list_existing_projects():
            jobs.append({
                "project_name": name,
                "input_path": None,
                "bundle_name": None,
            })

    if args.limit is not None:
        jobs = jobs[:args.limit]

    if not jobs:
        raise SystemExit("No projects found to digest.")

    results = []
    failures = 0

    for i, job in enumerate(jobs, start=1):
        project_name = job["project_name"]
        print(f"\n========== [{i}/{len(jobs)}] {project_name} ==========")

        try:
            result = run_project_digest(
                project_name=project_name,
                input_path=job["input_path"],
                source_kind=args.source_kind,
                bundle_name=job["bundle_name"],
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
            )
            result["status"] = "ok"
            results.append(result)

        except Exception as exc:
            failures += 1
            err = {
                "project_name": project_name,
                "status": "error",
                "error": str(exc),
            }
            results.append(err)
            print(f"[ERROR] {project_name}: {exc}")
            if not args.continue_on_error:
                break

    summary = {
        "total_jobs": len(jobs),
        "ok": sum(1 for r in results if r.get("status") == "ok"),
        "errors": sum(1 for r in results if r.get("status") == "error"),
        "results": results,
    }

    print("\n========== BULK DIGEST SUMMARY ==========")
    print(json.dumps(summary, indent=2))

    if args.report_json:
        out = Path(args.report_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"[OK] Wrote report: {out}")


if __name__ == "__main__":
    main()
