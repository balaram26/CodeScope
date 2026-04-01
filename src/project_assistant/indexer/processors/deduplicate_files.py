import argparse
from collections import defaultdict

from project_assistant.indexer.db import (
    get_project_files_for_dedup,
    init_db,
    mark_file_as_canonical,
    mark_file_as_duplicate,
)


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
    parser = argparse.ArgumentParser(description="Mark duplicate files within each project using sha256.")
    parser.add_argument("--project-name", type=str, default=None, help="Optional project name filter")
    parser.add_argument("--file-ids", type=str, default="", help="Optional comma-separated file_ids for incremental dedup")
    args = parser.parse_args()

    init_db()

    target_file_ids = parse_file_ids_arg(args.file_ids)

    rows = get_project_files_for_dedup(project_name=args.project_name)
    if not rows:
        print("[INFO] No files found for dedup.")
        return

    groups = defaultdict(list)
    for row in rows:
        key = (row["project_id"], row["sha256"])
        groups[key].append(row)

    canonical_count = 0
    duplicate_count = 0
    unique_groups = 0
    skipped_groups = 0

    for (_, _), file_rows in groups.items():
        if not file_rows:
            continue

        # For incremental mode:
        # only process groups that contain at least one targeted file_id.
        if target_file_ids:
            group_file_ids = {int(r["file_id"]) for r in file_rows}
            if group_file_ids.isdisjoint(target_file_ids):
                skipped_groups += 1
                continue

        unique_groups += 1
        canonical = file_rows[0]
        mark_file_as_canonical(canonical["file_id"])
        canonical_count += 1

        for dup in file_rows[1:]:
            mark_file_as_duplicate(dup["file_id"], canonical["file_id"])
            duplicate_count += 1
            print(
                f"[DUP] file_id={dup['file_id']} -> canonical_file_id={canonical['file_id']} "
                f"file={dup['file_name']}"
            )

    print(
        f"\nDone. Unique groups={unique_groups}, "
        f"canonicals={canonical_count}, duplicates={duplicate_count}, "
        f"skipped_groups={skipped_groups}"
    )


if __name__ == "__main__":
    main()