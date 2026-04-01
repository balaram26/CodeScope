import argparse
from collections import defaultdict
from pathlib import Path

from project_assistant.indexer.config import GENERATED_DIR
# from project_assistant.indexer.db import (
#     get_file_metadata_for_file_ids,
#     get_project_by_name_or_raise,
#     get_summarized_files_for_project,
#     init_db,
# )
from project_assistant.indexer.db import (
    get_project_by_name_or_raise,
    get_summarized_files_for_project,
    get_file_metadata_for_file_ids,
    init_db,
)


def ensure_project_generated_dir(project_name: str) -> Path:
    out_dir = GENERATED_DIR / project_name
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

def infer_code_bucket(row, metadata: dict) -> str:
    name = row["file_name"].lower()
    summary = (row["summary_text"] or "").lower()
    libs = [str(x).lower() for x in (metadata.get("libraries", []) or metadata.get("imports", []))]
    plot_outputs = metadata.get("plot_outputs", [])
    checkpoint_outputs = metadata.get("checkpoint_outputs", [])

    if any(k in name for k in ["diagnostic", "bacon", "qq", "pca", "inflation"]):
        return "QC and diagnostics"
    if any(k in name for k in ["plot", "manhattan", "figure", "panel", "volcano"]) or plot_outputs:
        return "Plotting and figures"
    if any(k in name for k in ["export", "annotated"]):
        return "Export and annotation"
    if any(k in name for k in ["train", "predict", "elasticnet", "model"]):
        return "Modeling and prediction"
    if any(k in name for k in ["preprocess", "prepare", "norm", "noob", "batch"]) or checkpoint_outputs:
        return "Preprocessing and normalization"
    if "limma" in libs or "ewas" in name or "ewas" in summary:
        return "EWAS analysis"
    return "General code"


def infer_result_bucket(row, metadata: dict) -> str:
    name = row["file_name"].lower()
    columns = [str(c).lower() for c in metadata.get("columns", [])]

    if "samplesheet" in name:
        return "Sample sheets"
    if "study_sheet" in name:
        return "Study and phenotype tables"
    if "binary" in name:
        return "Derived phenotype tables"
    if {"age", "sex", "sample_name"} & set(columns):
        return "Sample and phenotype metadata"
    return "Other result tables"


def render_code_summary(project_name: str, rows, metadata_map: dict[int, dict]) -> str:
    buckets = defaultdict(list)

    for row in rows:
        if row["file_type"] != "code":
            continue
        metadata = metadata_map.get(row["file_id"], {})
        bucket = infer_code_bucket(row, metadata)
        buckets[bucket].append((row, metadata))

    lines = []
    lines.append(f"# CODE_SUMMARY.auto.md")
    lines.append("")
    lines.append(f"Project: `{project_name}`")
    lines.append("")
    lines.append("This file was generated automatically from summarized canonical project files.")
    lines.append("")

    total_code = sum(len(v) for v in buckets.values())
    lines.append(f"Total canonical code files: {total_code}")
    lines.append("")

    for bucket in sorted(buckets.keys()):
        lines.append(f"## {bucket}")
        lines.append("")

        for row, metadata in sorted(buckets[bucket], key=lambda x: x[0]["file_name"].lower()):
            lines.append(f"### {row['file_name']}")
            lines.append("")
            lines.append(f"- Relative path: `{row['relative_path']}`")
            if metadata.get("line_count") is not None:
                lines.append(f"- Lines: {metadata['line_count']}")
            funcs = metadata.get("function_names", [])
            if funcs:
                lines.append(f"- Functions: {', '.join(funcs[:10])}")
            libs = metadata.get("libraries", []) or metadata.get("imports", [])
            if libs:
                lines.append(f"- Libraries/imports: {', '.join(libs[:12])}")
            file_refs = metadata.get("file_references", [])
            if file_refs:
                lines.append(f"- File references: {', '.join(file_refs[:8])}")
            in_files = metadata.get("input_files", [])
            out_files = metadata.get("output_files", [])
            chk_in = metadata.get("checkpoint_inputs", [])
            chk_out = metadata.get("checkpoint_outputs", [])
            plot_out = metadata.get("plot_outputs", [])

            if in_files:
                lines.append(f"- Likely inputs: {', '.join(in_files[:8])}")
            if out_files:
                lines.append(f"- Likely outputs: {', '.join(out_files[:8])}")
            if chk_in:
                lines.append(f"- Checkpoint inputs: {', '.join(chk_in[:6])}")
            if chk_out:
                lines.append(f"- Checkpoint outputs: {', '.join(chk_out[:6])}")
            if plot_out:
                lines.append(f"- Plot outputs: {', '.join(plot_out[:6])}")

            lines.append(f"- Summary: {row['summary_text']}")
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def render_results_summary(project_name: str, rows, metadata_map: dict[int, dict]) -> str:
    buckets = defaultdict(list)

    for row in rows:
        if row["file_type"] not in {"result", "report"}:
            continue
        metadata = metadata_map.get(row["file_id"], {})
        bucket = infer_result_bucket(row, metadata)
        buckets[bucket].append((row, metadata))

    lines = []
    lines.append(f"# RESULTS_SUMMARY.auto.md")
    lines.append("")
    lines.append(f"Project: `{project_name}`")
    lines.append("")
    lines.append("This file was generated automatically from summarized canonical result/report files.")
    lines.append("")

    total_res = sum(len(v) for v in buckets.values())
    lines.append(f"Total canonical result/report files: {total_res}")
    lines.append("")

    for bucket in sorted(buckets.keys()):
        lines.append(f"## {bucket}")
        lines.append("")

        for row, metadata in sorted(buckets[bucket], key=lambda x: x[0]["file_name"].lower()):
            lines.append(f"### {row['file_name']}")
            lines.append("")
            lines.append(f"- Relative path: `{row['relative_path']}`")
            row_count = metadata.get("row_count")
            col_count = metadata.get("column_count")
            if row_count is not None and col_count is not None:
                lines.append(f"- Shape: {row_count} rows x {col_count} columns")
            cols = metadata.get("columns", [])
            if cols:
                lines.append(f"- Columns: {', '.join(cols[:12])}")
            lines.append(f"- Summary: {row['summary_text']}")
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def main():
    parser = argparse.ArgumentParser(description="Generate project-level auto summary docs.")
    parser.add_argument("--project-name", required=True, help="Project name")
    args = parser.parse_args()

    init_db()
    get_project_by_name_or_raise(args.project_name)

    rows = get_summarized_files_for_project(args.project_name)
    if not rows:
        print(f"[INFO] No summarized canonical files found for project: {args.project_name}")
        return

    file_ids = [row["file_id"] for row in rows]
    metadata_map = get_file_metadata_for_file_ids(file_ids)

    out_dir = ensure_project_generated_dir(args.project_name)

    code_md = render_code_summary(args.project_name, rows, metadata_map)
    results_md = render_results_summary(args.project_name, rows, metadata_map)

    code_path = out_dir / "CODE_SUMMARY.auto.md"
    results_path = out_dir / "RESULTS_SUMMARY.auto.md"

    code_path.write_text(code_md, encoding="utf-8")
    results_path.write_text(results_md, encoding="utf-8")

    print(f"[OK] Wrote: {code_path}")
    print(f"[OK] Wrote: {results_path}")


if __name__ == "__main__":
    main()
