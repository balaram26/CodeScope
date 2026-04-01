import argparse
from pathlib import Path

from project_assistant.indexer.db import (
    get_files_for_ir,
    init_db,
    upsert_file_ir,
)
from project_assistant.indexer.schema_extractors.python_ir import extract_python_ir
from project_assistant.indexer.schema_extractors.r_ir import extract_r_ir
from project_assistant.indexer.schema_extractors.nextflow_ir import extract_nextflow_ir
from project_assistant.indexer.schema_extractors.cpp_ir import extract_cpp_ir


def parse_file_ids_arg(file_ids_arg: str | None) -> set[int]:
    if not file_ids_arg:
        return set()
    out: set[int] = set()
    for token in file_ids_arg.split(","):
        token = token.strip()
        if not token:
            continue
        out.add(int(token))
    return out


def main():
    parser = argparse.ArgumentParser(description="Build structured script IR for supported code files.")
    parser.add_argument("--project-name", type=str, default=None, help="Optional project filter")
    parser.add_argument("--limit", type=int, default=None, help="Optional max files")
    parser.add_argument("--ext", type=str, default=None, help="Optional extension filter, e.g. .nf")
    parser.add_argument("--force", action="store_true", help="Rebuild IR even if already present")
    parser.add_argument("--file-ids", type=str, default="", help="Optional comma-separated file_ids for incremental IR")
    args = parser.parse_args()

    init_db()

    target_file_ids = parse_file_ids_arg(args.file_ids)

    rows = get_files_for_ir(
        project_name=args.project_name,
        limit=args.limit,
        ext_filter=args.ext,
        only_missing=(not args.force),
    )
    if target_file_ids:
        rows = [row for row in rows if int(row["file_id"]) in target_file_ids]

    if not rows:
        print("[INFO] No eligible code files found for IR.")
        return

    ok = 0
    err = 0

    for row in rows:
        file_id = row["file_id"]
        file_path = Path(row["absolute_path"])
        ext = (row["file_ext"] or "").lower()

        try:
            if ext == ".py":
                ir = extract_python_ir(file_path)
                ir_type = "script_ir_v1"
            elif ext == ".r":
                ir = extract_r_ir(file_path)
                ir_type = "script_ir_v1"
            elif ext == ".nf":
                ir = extract_nextflow_ir(file_path)
                ir_type = "script_ir_v1"
            elif ext in {".cpp", ".cc", ".cxx", ".hpp", ".h"}:
                ir = extract_cpp_ir(file_path)
                ir_type = "script_ir_v1"
            else:
                continue

            upsert_file_ir(file_id=file_id, ir_type=ir_type, ir_obj=ir)
            ok += 1
            print(f"[OK] Built IR for file_id={file_id} file={row['file_name']}")
        except Exception as exc:
            err += 1
            print(f"[ERROR] file_id={file_id} file={row['file_name']}: {exc}")

    print(f"\nDone. Built={ok}, Errors={err}")


if __name__ == "__main__":
    main()