def build_code_summary(file_row, metadata: dict) -> str:
    file_name = file_row["file_name"]
    rel_path = file_row["relative_path"]
    language = metadata.get("language", "unknown")

    # ✅ INSERT THIS BLOCK RIGHT HERE
    if language == "nextflow":
        workflows = metadata.get("defined_workflows", [])
        processes = metadata.get("defined_processes", [])
        include_modules = metadata.get("include_modules", [])
        params_used = metadata.get("params_used", [])
        python_refs = metadata.get("python_refs", [])
        r_refs = metadata.get("r_refs", [])
        shell_refs = metadata.get("shell_refs", [])
        line_count = metadata.get("line_count")

        parts = [
            f"File `{file_name}` is a Nextflow pipeline/module located at `{rel_path}`."
        ]

        if line_count:
            parts.append(f"It has approximately {line_count} lines.")

        if workflows:
            parts.append(f"Defined workflows: {', '.join(workflows[:8])}.")

        if processes:
            parts.append(f"Defined processes: {', '.join(processes[:10])}.")

        if include_modules:
            rendered = []
            for item in include_modules[:6]:
                syms = ", ".join(item.get("symbols", [])[:4])
                src = item.get("source", "")
                if syms and src:
                    rendered.append(f"{syms} from {src}")
                elif src:
                    rendered.append(src)
            if rendered:
                parts.append(f"Includes modules: {'; '.join(rendered)}.")

        if params_used:
            parts.append(f"Params used: {', '.join(params_used[:10])}.")

        script_refs = list(dict.fromkeys(
            (python_refs or []) + (r_refs or []) + (shell_refs or [])
        ))
        if script_refs:
            parts.append(f"Referenced scripts: {', '.join(script_refs[:10])}.")

        return " ".join(parts)

    # 🔽 EXISTING CODE CONTINUES BELOW (unchanged)

    funcs = metadata.get("function_names", [])
    libs = metadata.get("libraries", []) or metadata.get("imports", [])
    file_refs = metadata.get("file_references", [])
    line_count = metadata.get("line_count")

    parts = [
        f"File `{file_name}` is a {language} script located at `{rel_path}`."
    ]

    if line_count:
        parts.append(f"It has approximately {line_count} lines.")

    if funcs:
        parts.append(f"Detected functions: {', '.join(funcs[:8])}.")

    if libs:
        parts.append(f"Detected libraries/imports: {', '.join(libs[:10])}.")

    if file_refs:
        parts.append(f"Referenced files include: {', '.join(file_refs[:10])}.")

    lower_name = file_name.lower()
    role_hints = []
    if "plot" in lower_name or "manhattan" in lower_name or "figure" in lower_name:
        role_hints.append("plotting or figure generation")
    if "export" in lower_name:
        role_hints.append("exporting processed results")
    if "train" in lower_name or "predict" in lower_name:
        role_hints.append("model training or prediction")
    if "preprocess" in lower_name or "prepare" in lower_name or "norm" in lower_name:
        role_hints.append("preprocessing or normalization")
    if "ewas" in lower_name:
        role_hints.append("EWAS-related analysis")

    if role_hints:
        parts.append("Likely role: " + ", ".join(role_hints) + ".")

    return " ".join(parts)




def build_table_summary(file_row, metadata: dict) -> str:
    file_name = file_row["file_name"]
    rel_path = file_row["relative_path"]
    fmt = metadata.get("table_format", "table")
    row_count = metadata.get("row_count")
    col_count = metadata.get("column_count")
    columns = metadata.get("columns", [])

    parts = [
        f"File `{file_name}` is a {fmt} table located at `{rel_path}`."
    ]

    if row_count is not None and col_count is not None:
        parts.append(f"It contains {row_count} data rows and {col_count} columns.")

    if columns:
        parts.append(f"Columns include: {', '.join(columns[:12])}.")

    lower_name = file_name.lower()
    role_hints = []
    if "samplesheet" in lower_name:
        role_hints.append("sample metadata or sheet")
    if "study_sheet" in lower_name:
        role_hints.append("study or phenotype metadata")
    if "binary" in lower_name:
        role_hints.append("binary phenotype or derived labels")

    if role_hints:
        parts.append("Likely role: " + ", ".join(role_hints) + ".")

    return " ".join(parts)


def build_markdown_summary(file_row, metadata: dict) -> str:
    file_name = file_row["file_name"]
    rel_path = file_row["relative_path"]
    headings = metadata.get("headings", [])
    word_count = metadata.get("word_count")

    parts = [
        f"File `{file_name}` is a documentation or note file located at `{rel_path}`."
    ]

    if word_count:
        parts.append(f"It contains about {word_count} words.")

    if headings:
        parts.append(f"Headings include: {', '.join(headings[:10])}.")

    return " ".join(parts)


def build_fallback_summary(file_row, metadata: dict) -> str:
    return (
        f"File `{file_row['file_name']}` located at `{file_row['relative_path']}` "
        f"was parsed with limited support."
    )


def build_summary_text(file_row, metadata: dict) -> str:
    ext = (file_row["file_ext"] or "").lower()
    file_name = file_row["file_name"]

    if ext in {".py", ".r", ".nf", ".cpp", ".cc", ".cxx", ".hpp", ".h", ".sh", ".bash"}:
        return build_code_summary(file_row, metadata)

    if ext in {".csv", ".tsv", ".xlsx", ".xls"}:
        return build_table_summary(file_row, metadata)

    if file_name == "nextflow.config":
        return build_code_summary(file_row, metadata)

    if ext in {".yaml", ".yml"} or file_name == "Dockerfile":
        return build_fallback_summary(file_row, metadata)

    if ext in {".md", ".txt"}:
        return build_markdown_summary(file_row, metadata)

    return build_fallback_summary(file_row, metadata)


def build_code_summary(file_row, metadata: dict) -> str:
    file_name = file_row["file_name"]
    rel_path = file_row["relative_path"]
    language = metadata.get("language", "unknown")

    funcs = metadata.get("defined_functions", []) or metadata.get("function_names", [])
    libs = metadata.get("imports_or_libraries", []) or metadata.get("libraries", []) or metadata.get("imports", [])
    line_count = metadata.get("line_count")
    dominant_role = metadata.get("dominant_role")

    file_inputs = metadata.get("final_file_inputs", [])
    file_outputs = metadata.get("final_file_outputs", [])
    checkpoint_inputs = metadata.get("final_checkpoint_inputs", [])
    checkpoint_outputs = metadata.get("final_checkpoint_outputs", [])
    plot_outputs = metadata.get("final_plot_outputs", [])
    parameters = metadata.get("final_parameters", [])
    source_dependencies = metadata.get("source_dependencies", [])
    call_edges = metadata.get("call_edges", [])

    parts = [
        f"File `{file_name}` is a {language} script located at `{rel_path}`."
    ]

    if dominant_role:
        parts.append(f"Dominant role: {dominant_role}.")

    if line_count:
        parts.append(f"It has approximately {line_count} lines.")

    if funcs:
        parts.append(f"Defined functions: {', '.join(funcs[:8])}.")

    if libs:
        parts.append(f"Detected libraries/imports: {', '.join(libs[:10])}.")

    if file_inputs:
        parts.append(f"Likely file inputs: {', '.join(file_inputs[:5])}.")

    if file_outputs:
        parts.append(f"Likely file outputs: {', '.join(file_outputs[:5])}.")

    if checkpoint_inputs:
        parts.append(f"Likely checkpoint inputs: {', '.join(checkpoint_inputs[:5])}.")

    if checkpoint_outputs:
        parts.append(f"Likely checkpoint outputs: {', '.join(checkpoint_outputs[:5])}.")

    if plot_outputs:
        parts.append(f"Likely plot outputs: {', '.join(plot_outputs[:5])}.")

    if parameters:
        parts.append(f"Important parameters: {', '.join(parameters[:5])}.")

    if source_dependencies:
        parts.append(f"Source dependencies include: {', '.join(source_dependencies[:5])}.")

    if call_edges:
        shown = [f"{a}->{b}" for a, b in call_edges[:5]]
        parts.append(f"Internal call edges include: {', '.join(shown)}.")

    return " ".join(parts)