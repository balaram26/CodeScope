import argparse
import json

from project_assistant.indexer.db import (
    get_files_to_summarize,
    get_latest_file_metadata,
    get_latest_file_metadata_merged,
    init_db,
    update_file_status,
    upsert_file_summary,
)
from project_assistant.indexer.summarizers.build_file_summary import build_summary_text


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
    parser = argparse.ArgumentParser(description="Generate summaries for parsed non-duplicate files.")
    parser.add_argument("--project-name", type=str, default=None, help="Optional project name filter")
    parser.add_argument("--limit", type=int, default=None, help="Optional max files")
    parser.add_argument("--file-ids", type=str, default="", help="Optional comma-separated file_ids for incremental summarize")
    args = parser.parse_args()

    init_db()

    target_file_ids = parse_file_ids_arg(args.file_ids)

    files = get_files_to_summarize(project_name=args.project_name, limit=args.limit)
    if target_file_ids:
        files = [row for row in files if int(row["file_id"]) in target_file_ids]

    if not files:
        print("[INFO] No parsed non-duplicate files pending summary.")
        return

    ok_count = 0
    err_count = 0

    for row in files:
        file_id = row["file_id"]
        file_name = row["file_name"]

        try:
            merged_row = get_latest_file_metadata_merged(file_id)
            if merged_row:
                metadata = json.loads(merged_row["metadata_json"])
            else:
                meta_row = get_latest_file_metadata(file_id)
                if not meta_row:
                    print(f"[WARN] No metadata found for file_id={file_id} file={file_name}")
                    continue
                metadata = json.loads(meta_row["metadata_json"])

            summary_text = build_summary_text(row, metadata)

            upsert_file_summary(
                file_id=file_id,
                summary_type="basic",
                summary_text=summary_text,
                model_name="heuristic_v1",
            )
            update_file_status(file_id, "summarized")
            ok_count += 1
            print(f"[OK] Summarized file_id={file_id} file={file_name}")
        except Exception as exc:
            update_file_status(file_id, "error")
            err_count += 1
            print(f"[ERROR] file_id={file_id} file={file_name}: {exc}")

    print(f"\nDone. Summarized={ok_count}, Errors={err_count}")


if __name__ == "__main__":
    main()