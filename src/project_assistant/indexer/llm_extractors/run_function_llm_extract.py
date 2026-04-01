import argparse
import json

from project_assistant.ai.llm_service import LLMService
from project_assistant.indexer.db import (
    get_file_ir,
    get_files_for_function_llm,
    get_latest_file_metadata,
    init_db,
    upsert_function_metadata_llm,
)
from project_assistant.indexer.llm_extractors.function_prompt_builder import (
    build_function_extraction_prompt,
)
from project_assistant.indexer.llm_extractors.script_prompt_builder import (
    build_script_extraction_prompt,
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
    parser = argparse.ArgumentParser(description="Run function-level LLM extraction on script IR.")
    parser.add_argument("--project-name", type=str, default=None, help="Optional project filter")
    parser.add_argument("--limit-files", type=int, default=None, help="Optional file limit")
    parser.add_argument("--limit-functions", type=int, default=None, help="Optional total function limit")
    parser.add_argument(
        "--config-path",
        required=True,
        help="Path to the YAML model config used by project_assistant.ai.llm_service.LLMService",
    )
    parser.add_argument(
        "--task-name",
        default="function_metadata_extract",
        help="Task name from YAML config",
    )
    parser.add_argument(
        "--model-label",
        default="local_llm_v1",
        help="Label stored in DB for this extraction run",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=1200,
        help="Override max tokens for extraction",
    )
    parser.add_argument("--ext", type=str, default=None, help="Optional extension filter, e.g. .nf")
    parser.add_argument("--force", action="store_true", help="Run even for files that already have LLM extraction")
    parser.add_argument("--file-ids", type=str, default="", help="Optional comma-separated file_ids for incremental LLM extraction")
    args = parser.parse_args()

    init_db()
    llm = LLMService.from_yaml(args.config_path)

    target_file_ids = parse_file_ids_arg(args.file_ids)

    rows = get_files_for_function_llm(
        project_name=args.project_name,
        limit=args.limit_files,
        ext_filter=args.ext,
        only_missing=(not args.force),
        model_name=args.model_label,
    )
    if target_file_ids:
        rows = [row for row in rows if int(row["file_id"]) in target_file_ids]

    if not rows:
        print("[INFO] No files available for function-level LLM extraction.")
        return

    done = 0
    errors = 0

    for row in rows:
        file_id = row["file_id"]
        ir_row = get_file_ir(file_id)
        meta_row = get_latest_file_metadata(file_id)

        if not ir_row:
            print(f"[WARN] No IR found for file_id={file_id} file={row['file_name']}")
            continue

        ir_obj = json.loads(ir_row["ir_json"])
        parser_metadata = json.loads(meta_row["metadata_json"]) if meta_row else {}

        functions = ir_obj.get("functions", [])
        if not functions:
            try:
                prompt = build_script_extraction_prompt(
                    file_row=row,
                    ir_obj=ir_obj,
                    parser_metadata=parser_metadata,
                )

                result = llm.extract_json(
                    task=args.task_name,
                    prompt=prompt,
                    max_tokens=args.max_tokens,
                )

                if not result["ok_json"] or not isinstance(result["parsed"], dict):
                    raise ValueError(
                        f"Failed to parse JSON. Error={result['error']}. "
                        f"Output={result['output_text'][:500]}"
                    )

                parsed = result["parsed"]
                parsed["function_name"] = "__script__"

                upsert_function_metadata_llm(
                    file_id=file_id,
                    function_name="__script__",
                    model_name=args.model_label,
                    metadata=parsed,
                )
                done += 1
                print(f"[OK] file_id={file_id} script_fallback={row['file_name']}")
            except Exception as exc:
                errors += 1
                print(f"[ERROR] file_id={file_id} script_fallback={row['file_name']}: {exc}")

            continue

        for function_obj in functions:
            if args.limit_functions is not None and done >= args.limit_functions:
                print(f"\nDone. Extracted={done}, Errors={errors}")
                return

            fn_name = function_obj.get("function_name", "")
            fn_text = function_obj.get("function_text", "")
            if not fn_name:
                continue

            if fn_text and len(fn_text.splitlines()) < 2:
                continue

            prompt = build_function_extraction_prompt(
                file_row=row,
                ir_obj=ir_obj,
                function_obj=function_obj,
                parser_metadata=parser_metadata,
            )

            try:
                result = llm.extract_json(
                    task=args.task_name,
                    prompt=prompt,
                    max_tokens=args.max_tokens,
                )

                if not result["ok_json"] or not isinstance(result["parsed"], dict):
                    raise ValueError(
                        f"Failed to parse JSON. Error={result['error']}. "
                        f"Output={result['output_text'][:500]}"
                    )

                parsed = result["parsed"]
                parsed["function_name"] = fn_name

                upsert_function_metadata_llm(
                    file_id=file_id,
                    function_name=fn_name,
                    model_name=args.model_label,
                    metadata=parsed,
                )
                done += 1
                print(f"[OK] file_id={file_id} function={fn_name}")
            except Exception as exc:
                errors += 1
                print(f"[ERROR] file_id={file_id} function={fn_name}: {exc}")

    print(f"\nDone. Extracted={done}, Errors={errors}")


if __name__ == "__main__":
    main()