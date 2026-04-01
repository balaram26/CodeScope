import argparse
import json


from project_assistant.indexer.db import (
    get_file_ir,
    get_files_for_merge,
    get_function_metadata_llm_for_file,
    get_latest_file_metadata,
    init_db,
    upsert_file_metadata_merged,
)
from project_assistant.indexer.mergers.merge_file_metadata import merge_file_metadata


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
    parser = argparse.ArgumentParser(description="Build merged file metadata from parser + IR + LLM.")
    parser.add_argument("--project-name", type=str, default=None, help="Optional project filter")
    parser.add_argument("--limit", type=int, default=None, help="Optional file limit")
    parser.add_argument("--model-name", type=str, default=None, help="Optional LLM model label filter")
    parser.add_argument("--merge-version", type=str, default="merge_v1", help="Merge version label")
    parser.add_argument("--file-ids", type=str, default="", help="Optional comma-separated file_ids for incremental merge")
    args = parser.parse_args()

    init_db()

    target_file_ids = parse_file_ids_arg(args.file_ids)

    rows = get_files_for_merge(project_name=args.project_name, limit=args.limit)
    if target_file_ids:
        rows = [row for row in rows if int(row["file_id"]) in target_file_ids]

    if not rows:
        print("[INFO] No files found for merged metadata build.")
        return

    ok = 0
    err = 0

    for row in rows:
        file_id = row["file_id"]

        try:
            parser_row = get_latest_file_metadata(file_id)
            ir_row = get_file_ir(file_id)
            fn_rows = get_function_metadata_llm_for_file(file_id, model_name=args.model_name)

            parser_metadata = json.loads(parser_row["metadata_json"]) if parser_row else {}
            ir_obj = json.loads(ir_row["ir_json"]) if ir_row else {}

            merged = merge_file_metadata(
                parser_metadata=parser_metadata,
                ir_obj=ir_obj,
                function_llm_rows=fn_rows,
            )

            upsert_file_metadata_merged(
                file_id=file_id,
                merge_version=args.merge_version,
                metadata=merged,
            )

            ok += 1
            print(f"[OK] Merged metadata for file_id={file_id} file={row['file_name']}")
        except Exception as exc:
            err += 1
            print(f"[ERROR] file_id={file_id} file={row['file_name']}: {exc}")

    print(f"\nDone. Merged={ok}, Errors={err}")


if __name__ == "__main__":
    main()