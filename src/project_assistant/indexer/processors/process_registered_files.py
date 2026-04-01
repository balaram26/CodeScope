import argparse
from pathlib import Path

from project_assistant.indexer.db import (
    get_files_to_parse,
    init_db,
    update_file_status,
    upsert_file_metadata,
)
from project_assistant.indexer.parsers.parse_markdown import (
    parse_markdown_file,
    parse_text_file,
)
from project_assistant.indexer.parsers.parse_python import parse_python_file
from project_assistant.indexer.parsers.parse_r import parse_r_file
from project_assistant.indexer.parsers.parse_table import parse_table_file
from project_assistant.indexer.parsers.parse_nextflow import parse_nextflow_file
from project_assistant.indexer.parsers.parse_cpp import parse_cpp_file
from project_assistant.indexer.parsers.parse_yaml import parse_yaml_file
from project_assistant.indexer.parsers.parse_dockerfile import parse_dockerfile
from project_assistant.indexer.parsers.parse_shell import parse_shell_file
from project_assistant.indexer.parsers.parse_nextflow_config import parse_nextflow_config


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


def parse_file_by_type(file_row) -> tuple[str, dict]:
    ext = (file_row["file_ext"] or "").lower()
    file_name = file_row["file_name"]
    file_path = Path(file_row["absolute_path"])

    if file_name == "nextflow.config":
        metadata = parse_nextflow_config(file_path)
        parser_name = "nextflow_config"

    elif file_name == "Dockerfile":
        metadata = parse_dockerfile(file_path)
        parser_name = "docker_basic"

    elif ext == ".py":
        metadata = parse_python_file(file_path)
        parser_name = "python_ast"

    elif ext == ".r":
        metadata = parse_r_file(file_path)
        parser_name = "r_regex"

    elif ext == ".nf":
        metadata = parse_nextflow_file(file_path)
        parser_name = "nextflow_regex"

    elif ext in {".cpp", ".cc", ".cxx", ".hpp", ".h"}:
        metadata = parse_cpp_file(file_path)
        parser_name = "cpp_regex"

    elif ext in {".csv", ".tsv", ".txt"}:
        metadata = parse_table_file(file_path)
        parser_name = "table_basic"

    elif ext in {".yaml", ".yml"}:
        metadata = parse_yaml_file(file_path)
        parser_name = "yaml_basic"

    elif ext in {".sh", ".bash"}:
        metadata = parse_shell_file(file_path)
        parser_name = "shell_basic"

    else:
        metadata = {
            "parser": "unsupported",
            "reason": f"Unsupported extension: {ext}",
            "file_type": file_row["file_type"],
        }
        parser_name = "unsupported"

    return parser_name, metadata


def main():
    parser = argparse.ArgumentParser(description="Parse registered project files and store metadata.")
    parser.add_argument("--project-name", type=str, default=None, help="Optional project name filter")
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of files to process")
    parser.add_argument("--file-ids", type=str, default="", help="Optional comma-separated file_ids for incremental parse")
    args = parser.parse_args()

    init_db()

    target_file_ids = parse_file_ids_arg(args.file_ids)

    files = get_files_to_parse(project_name=args.project_name, limit=args.limit)
    if target_file_ids:
        files = [row for row in files if int(row["file_id"]) in target_file_ids]

    if not files:
        print("[INFO] No registered files pending parse.")
        return

    ok_count = 0
    err_count = 0

    for row in files:
        file_id = row["file_id"]
        file_name = row["file_name"]

        try:
            parser_name, metadata = parse_file_by_type(row)
            upsert_file_metadata(file_id=file_id, parser_name=parser_name, metadata=metadata)
            update_file_status(file_id, "parsed")
            ok_count += 1
            print(f"[OK] Parsed file_id={file_id} file={file_name} parser={parser_name}")
        except Exception as exc:
            update_file_status(file_id, "error")
            err_count += 1
            print(f"[ERROR] file_id={file_id} file={file_name}: {exc}")

    print(f"\nDone. Parsed={ok_count}, Errors={err_count}")


if __name__ == "__main__":
    main()