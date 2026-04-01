import argparse
import shutil
from pathlib import Path

from project_assistant.indexer.config import GENERATED_DIR, MANAGED_DIR
from project_assistant.indexer.db import (
    delete_project_derived_data,
    delete_project_from_db,
    get_project_by_name,
    init_db,
)


def remove_path_if_exists(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def main():
    parser = argparse.ArgumentParser(description="Delete or reset one indexed project.")
    parser.add_argument("--project-name", required=True, help="Project name")
    parser.add_argument(
        "--mode",
        choices=["soft", "hard"],
        default="soft",
        help="soft = remove derived data only, hard = remove project completely",
    )
    args = parser.parse_args()

    init_db()

    row = get_project_by_name(args.project_name)
    if not row:
        raise ValueError(f"Project not found: {args.project_name}")

    if args.mode == "soft":
        delete_project_derived_data(args.project_name)
        remove_path_if_exists(GENERATED_DIR / args.project_name)
        (GENERATED_DIR / args.project_name).mkdir(parents=True, exist_ok=True)
        print(f"[OK] Soft reset completed for project: {args.project_name}")
        return

    result = delete_project_from_db(args.project_name)

    # remove canonical project folders
    remove_path_if_exists(MANAGED_DIR / args.project_name)
    remove_path_if_exists(GENERATED_DIR / args.project_name)

    # remove any source managed paths if still present
    for p in result["managed_paths"]:
        try:
            remove_path_if_exists(Path(p))
        except Exception:
            pass

    print(f"[OK] Hard delete completed for project: {args.project_name}")


if __name__ == "__main__":
    main()
