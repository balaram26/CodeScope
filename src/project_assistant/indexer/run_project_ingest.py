import argparse
import shutil
from pathlib import Path

from project_assistant.indexer.classifier import classify_file
from project_assistant.indexer.config import (
    IGNORE_DIR_NAMES,
    IGNORE_FILE_NAMES,
    INBOX_DIR,
    SOURCE_ORIGIN_UPLOAD,
)
from project_assistant.indexer.copier import copy_bundle_to_managed
from project_assistant.indexer.db import (
    create_ingest_run,
    create_source,
    finalize_ingest_run,
    get_or_create_project,
    init_db,
    register_file,
    db_cursor,
)
from project_assistant.indexer.hasher import sha256_file
from project_assistant.indexer.resolver import (
    resolve_bundle_name,
    resolve_project_name,
    resolve_source_kind,
)
from project_assistant.indexer.scanner import find_bundle_dirs
from project_assistant.indexer.input_resolver import (
    cleanup_resolved_input,
    resolve_input_path,
)


def iter_files(root: Path):
    for path in root.rglob("*"):
        if path.is_dir():
            continue

        if path.name in IGNORE_FILE_NAMES:
            continue

        if any(part in IGNORE_DIR_NAMES for part in path.parts):
            continue

        yield path


def get_existing_managed_bundle_dir(project_id: int, bundle_name: str) -> Path | None:
    with db_cursor() as (_, cur):
        cur.execute(
            """
            SELECT managed_path
            FROM sources
            WHERE project_id = ?
              AND bundle_name = ?
              AND managed_path IS NOT NULL
            ORDER BY source_id DESC
            LIMIT 1
            """,
            (project_id, bundle_name),
        )
        row = cur.fetchone()

    if not row:
        return None

    managed_path = row["managed_path"]
    if not managed_path:
        return None

    p = Path(managed_path)
    return p if p.exists() else None


def merge_bundle_into_managed(
    bundle_dir: Path,
    managed_bundle_dir: Path,
    merge_mode: str,
) -> dict[str, int]:
    if merge_mode not in {"overwrite_existing", "add_new_only"}:
        raise ValueError(f"Unsupported merge_mode: {merge_mode}")

    managed_bundle_dir.mkdir(parents=True, exist_ok=True)

    added = 0
    overwritten = 0
    skipped = 0

    for src in iter_files(bundle_dir):
        rel_path = src.relative_to(bundle_dir)
        dst = managed_bundle_dir / rel_path
        dst.parent.mkdir(parents=True, exist_ok=True)

        if dst.exists():
            if merge_mode == "add_new_only":
                skipped += 1
                continue

            shutil.copy2(src, dst)
            overwritten += 1
        else:
            shutil.copy2(src, dst)
            added += 1

    return {
        "added": added,
        "overwritten": overwritten,
        "skipped": skipped,
    }


def ingest_bundle(
    bundle_dir: Path,
    project_name: str | None = None,
    source_kind: str | None = None,
    bundle_name: str | None = None,
    create_if_missing: bool = True,
    merge_mode: str = "overwrite_existing",
) -> None:
    resolved_project_name = resolve_project_name(bundle_dir, explicit_project_name=project_name)
    resolved_source_kind = resolve_source_kind(bundle_dir, explicit_source_kind=source_kind)
    resolved_bundle_name = resolve_bundle_name(bundle_dir, explicit_bundle_name=bundle_name)

    if merge_mode not in {"overwrite_existing", "add_new_only"}:
        raise ValueError(f"Unsupported merge_mode: {merge_mode}")

    project_id = get_or_create_project(resolved_project_name)

    existing_managed_bundle_dir = get_existing_managed_bundle_dir(
        project_id=project_id,
        bundle_name=resolved_bundle_name,
    )

    if existing_managed_bundle_dir is None:
        managed_bundle_dir = copy_bundle_to_managed(
            bundle_dir=bundle_dir,
            project_name=resolved_project_name,
            bundle_name=resolved_bundle_name,
        )
        merge_stats = {
            "added": sum(1 for _ in iter_files(managed_bundle_dir)),
            "overwritten": 0,
            "skipped": 0,
        }
    else:
        managed_bundle_dir = existing_managed_bundle_dir
        merge_stats = merge_bundle_into_managed(
            bundle_dir=bundle_dir,
            managed_bundle_dir=managed_bundle_dir,
            merge_mode=merge_mode,
        )

    source_id = create_source(
        project_id=project_id,
        source_origin=SOURCE_ORIGIN_UPLOAD,
        source_kind=resolved_source_kind,
        bundle_name=resolved_bundle_name,
        original_path=str(bundle_dir.resolve()),
        managed_path=str(managed_bundle_dir.resolve()),
    )

    file_count = 0
    for file_path in iter_files(managed_bundle_dir):
        rel_path = file_path.relative_to(managed_bundle_dir)
        file_type = classify_file(file_path)
        sha256 = sha256_file(file_path)
        size_bytes = file_path.stat().st_size

        register_file(
            source_id=source_id,
            project_id=project_id,
            relative_path=str(rel_path),
            absolute_path=str(file_path.resolve()),
            file_name=file_path.name,
            file_ext=file_path.suffix.lower(),
            file_type=file_type,
            size_bytes=size_bytes,
            sha256=sha256,
            status="registered",
        )
        file_count += 1

    print(f"[OK] Ingested bundle: {bundle_dir}")
    print(f"     Project: {resolved_project_name}")
    print(f"     Source kind: {resolved_source_kind}")
    print(f"     Bundle name: {resolved_bundle_name}")
    print(f"     Managed path: {managed_bundle_dir}")
    print(f"     Merge mode: {merge_mode}")
    print(f"     Merge stats: {merge_stats}")
    print(f"     Registered files in managed bundle: {file_count}")


def main():
    parser = argparse.ArgumentParser(description="Ingest project bundles into managed storage and DB.")
    parser.add_argument("--inbox", type=str, default=str(INBOX_DIR), help="Inbox directory containing bundle folders")
    parser.add_argument("--input", type=str, help="Single bundle folder to ingest")
    parser.add_argument("--project-name", type=str, default=None, help="Optional target project name")
    parser.add_argument("--source-kind", type=str, default=None, help="Optional source kind: code/results/docs/notes/mixed")
    parser.add_argument("--bundle-name", type=str, default=None, help="Optional bundle label")
    parser.add_argument("--create-if-missing", action="store_true", help="Create project if missing")
    parser.add_argument(
        "--merge-mode",
        type=str,
        default="overwrite_existing",
        choices=["overwrite_existing", "add_new_only"],
        help="How to merge into an existing managed project bundle",
    )
    args = parser.parse_args()

    init_db()
    run_id = create_ingest_run(notes="project ingest started")

    try:
        if args.input:
            resolved = resolve_input_path(Path(args.input))
            try:
                ingest_bundle(
                    bundle_dir=resolved.bundle_dir,
                    project_name=args.project_name,
                    source_kind=args.source_kind,
                    bundle_name=args.bundle_name,
                    create_if_missing=args.create_if_missing,
                    merge_mode=args.merge_mode,
                )
            finally:
                cleanup_resolved_input(resolved)
        else:
            inbox_dir = Path(args.inbox)
            inputs = find_bundle_dirs(inbox_dir)
            if not inputs:
                print(f"[INFO] No bundle folders or archives found in: {inbox_dir}")
            else:
                for item in inputs:
                    resolved = resolve_input_path(item)
                    try:
                        ingest_bundle(
                            bundle_dir=resolved.bundle_dir,
                            project_name=args.project_name,
                            source_kind=args.source_kind,
                            bundle_name=args.bundle_name,
                            create_if_missing=args.create_if_missing,
                            merge_mode=args.merge_mode,
                        )
                    finally:
                        cleanup_resolved_input(resolved)

        finalize_ingest_run(run_id, status="success", notes="project ingest completed")
    except Exception as exc:
        finalize_ingest_run(run_id, status="error", notes=str(exc))
        raise


if __name__ == "__main__":
    main()