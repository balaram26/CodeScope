from __future__ import annotations

from typing import Any

import streamlit as st

from project_assistant.indexer.db import init_db
from project_assistant.services.models import QueryRequest
from project_assistant.services.service_factory import build_services

init_db()

st.set_page_config(
    page_title="Project Assistant",
    page_icon="📁",
    layout="wide",
)

services = build_services()
project_helper = services["project_helper"]
project_import_service = services["project_import_service"]
project_delete_service = services["project_delete_service"]
llm_adapter = services["llm_adapter"]


def _safe_get_status(project_name: str) -> dict[str, Any]:
    try:
        return project_helper.get_status(project_name)
    except Exception as exc:
        return {
            "status": "error",
            "current_stage": "status",
            "short_error": str(exc),
            "stats": {},
        }


def _list_projects_with_status() -> list[dict[str, Any]]:
    rows = project_helper.list_projects_for_ui()
    out = []
    for row in rows:
        project_name = row["source_system_id"]
        status_payload = _safe_get_status(project_name)
        out.append(
            {
                "project_name": project_name,
                "display_name": row.get("display_name", project_name),
                "status": status_payload.get("status", "unknown"),
                "current_stage": status_payload.get("current_stage"),
                "stats": status_payload.get("stats", {}),
                "short_error": status_payload.get("short_error"),
            }
        )
    return out


def _build_answer_prompt(question: str, project_names: list[str], evidence: list[dict[str, Any]]) -> str:
    evidence_blocks = []
    for i, item in enumerate(evidence, start=1):
        project_name = item.get("source_system_id", "")
        file_path = item.get("file_path") or "unknown_file"
        score = item.get("score")
        chunk_id = item.get("chunk_id") or ""
        text = (item.get("text") or "").strip()

        evidence_blocks.append(
            f"""[Evidence {i}]
Project: {project_name}
File: {file_path}
Chunk: {chunk_id}
Score: {score}

{text}
"""
        )

    evidence_text = "\n\n".join(evidence_blocks) if evidence_blocks else "No evidence retrieved."

    return f"""You are a project codebase assistant.

Answer the user's question using only the provided evidence.
Do not speculate. Do not use words like "likely", "probably", or "appears" unless the evidence explicitly says so.
Do not repeat yourself.
Do not add multiple notes, revisions, or repeated conclusions.

Selected projects:
{", ".join(project_names)}

User question:
{question}

Evidence:
{evidence_text}

Output format:
1. Short answer (maximum 4 sentences)
2. Key files
3. If needed, one sentence of uncertainty. Otherwise omit it.

Rules:
- Mention only files that are directly supported by the evidence.
- Keep the answer concise.
- Each section must appear only once.
- No "Final Note", "Revised Note", or repeated summaries.
"""


def _answer_question(req: QueryRequest) -> dict[str, Any]:
    evidence = project_helper.search(
        question=req.question,
        source_system_ids=req.project_names,
        top_k=req.top_k,
    )

    prompt = _build_answer_prompt(
        question=req.question,
        project_names=req.project_names,
        evidence=evidence,
    )

    answer = llm_adapter.generate(prompt, max_tokens=900)

    return {
        "answer": answer,
        "evidence": evidence,
        "used_projects": req.project_names,
    }


def _project_label(row: dict[str, Any]) -> str:
    stats = row.get("stats", {})
    total_files = stats.get("total_files", 0)
    return f"{row['display_name']} [{row['status']}/{row.get('current_stage')}] · files={total_files}"


def _render_projects_table(items: list[dict[str, Any]]) -> None:
    if not items:
        st.info("No projects registered yet.")
        return

    table_rows = []
    for row in items:
        stats = row.get("stats", {})
        table_rows.append(
            {
                "Project": row["display_name"],
                "Status": row["status"],
                "Stage": row.get("current_stage"),
                "Files": stats.get("total_files", 0),
                "Parsed": stats.get("parsed_files", 0),
                "Summaries": stats.get("summarized_files", 0),
                "Function metadata": stats.get("function_metadata_rows", 0),
                "Merged metadata": stats.get("merged_metadata_rows", 0),
                "FAISS": stats.get("has_faiss_index", False),
                "Chunks meta": stats.get("has_chunks_meta", False),
            }
        )
    st.dataframe(table_rows, width="stretch", hide_index=True)


def _render_evidence(evidence: list[dict[str, Any]]) -> None:
    if not evidence:
        st.info("No evidence returned.")
        return

    for i, item in enumerate(evidence, start=1):
        title = f"{i}. {item.get('source_system_id', 'unknown')} — {item.get('file_path') or 'unknown_file'}"
        with st.expander(title):
            c1, c2, c3 = st.columns(3)
            c1.write(f"**Chunk ID:** {item.get('chunk_id') or '-'}")
            c2.write(f"**Score:** {item.get('score')}")
            c3.write(f"**File:** {item.get('file_path') or '-'}")

            metadata = item.get("metadata") or {}
            if metadata:
                st.write("**Metadata**")
                st.json(metadata, expanded=False)

            st.write("**Text**")
            st.code(item.get("text", ""), language=None)


def _render_sidebar(items: list[dict[str, Any]]) -> None:
    st.sidebar.title("Project Assistant")
    st.sidebar.caption("Index and query local research or software projects.")

    try:
        llm_info = llm_adapter.describe()
    except Exception:
        llm_info = {}

    with st.sidebar.expander("LLM settings", expanded=False):
        st.json(llm_info, expanded=False)

    with st.sidebar.expander("Runtime summary", expanded=True):
        st.write(f"Projects: **{len(items)}**")
        ready = sum(1 for x in items if x["status"] == "ready")
        st.write(f"Ready: **{ready}**")


def main() -> None:
    st.title("📁 Project Assistant")

    items = _list_projects_with_status()
    _render_sidebar(items)

    tab_projects, tab_import, tab_query, tab_delete = st.tabs(
        ["Projects", "Import / Update", "Ask", "Delete"]
    )

    with tab_projects:
        st.subheader("Indexed projects")

        c1, c2, c3 = st.columns(3)
        c1.metric("Projects", len(items))
        c2.metric("Ready", sum(1 for x in items if x["status"] == "ready"))
        c3.metric("With errors", sum(1 for x in items if x["status"] == "error"))

        if st.button("Refresh project status", width="content"):
            st.rerun()

        _render_projects_table(items)

        with st.expander("Raw project status", expanded=False):
            st.json(items, expanded=False)

    with tab_import:
        st.subheader("Import or update a project")

        mode = st.radio(
            "Mode",
            options=["Import new project", "Update existing project"],
            horizontal=True,
        )

        if mode == "Import new project":
            with st.form("import_project_form"):
                project_name = st.text_input("Project name")
                source_path = st.text_input("Source path")
                submitted = st.form_submit_button("Import project")

                if submitted:
                    if not project_name.strip():
                        st.warning("Project name is required.")
                    elif not source_path.strip():
                        st.warning("Source path is required.")
                    else:
                        try:
                            result = project_import_service.import_project(
                                project_name=project_name.strip(),
                                source_path=source_path.strip(),
                            )
                            st.success("Project imported and indexed successfully.")
                            st.json(result, expanded=False)
                        except Exception as exc:
                            st.error(f"Project import failed: {exc}")

        else:
            if not items:
                st.info("No existing projects available to update.")
            else:
                project_options = {row["display_name"]: row["project_name"] for row in items}
                with st.form("update_project_form"):
                    selected_label = st.selectbox("Existing project", list(project_options.keys()))
                    source_path = st.text_input("Updated source path")
                    merge_mode = st.selectbox(
                        "Merge mode",
                        options=["overwrite_existing", "skip_existing"],
                        index=0,
                    )
                    submitted = st.form_submit_button("Update project")

                    if submitted:
                        if not source_path.strip():
                            st.warning("Source path is required.")
                        else:
                            try:
                                source_system_id = project_options[selected_label]
                                result = project_import_service.update_project(
                                    source_system_id=source_system_id,
                                    source_path=source_path.strip(),
                                    merge_mode=merge_mode,
                                )
                                st.success("Project updated successfully.")
                                st.json(result, expanded=False)
                            except Exception as exc:
                                st.error(f"Project update failed: {exc}")

    with tab_query:
        st.subheader("Ask a question about indexed projects")

        if not items:
            st.info("Import a project first.")
        else:
            selectable = [_project_label(x) for x in items]
            label_to_project = {_project_label(x): x["project_name"] for x in items}

            default_labels = [_project_label(x) for x in items[:1]]

            selected_labels = st.multiselect(
                "Select project(s)",
                options=selectable,
                default=default_labels,
            )

            question = st.text_area(
                "Question",
                placeholder="Ask about scripts, workflow logic, configs, outputs, dependencies, or modeling used in the project...",
                height=140,
            )

            top_k = st.slider("Top-k evidence chunks", min_value=3, max_value=20, value=8)

            ask_clicked = st.button("Ask", type="primary")

            if ask_clicked:
                if not selected_labels:
                    st.warning("Select at least one project.")
                elif not question.strip():
                    st.warning("Enter a question.")
                else:
                    req = QueryRequest(
                        question=question.strip(),
                        project_names=[label_to_project[x] for x in selected_labels],
                        top_k=top_k,
                        include_evidence=True,
                    )

                    with st.spinner("Searching indexed project evidence and generating answer..."):
                        try:
                            result = _answer_question(req)
                        except Exception as exc:
                            st.error(f"Query failed: {exc}")
                        else:
                            st.markdown("### Answer")
                            st.write(result["answer"])

                            st.markdown("### Evidence")
                            _render_evidence(result["evidence"])

    with tab_delete:
        st.subheader("Delete a project")

        if not items:
            st.info("No projects available.")
        else:
            project_options = {row["display_name"]: row["project_name"] for row in items}
            selected_label = st.selectbox("Select project", list(project_options.keys()))
            selected_project_name = project_options[selected_label]

            st.caption(
                "This deletes the indexed project state and managed project copy for the selected project."
            )

            confirm = st.checkbox(
                f"I understand that '{selected_project_name}' will be deleted."
            )

            if st.button("Delete project", type="secondary"):
                if not confirm:
                    st.warning("Please confirm deletion first.")
                else:
                    try:
                        project_delete_service.delete_project(selected_project_name)
                        st.success(f"Deleted project: {selected_project_name}")
                    except Exception as exc:
                        st.error(f"Delete failed: {exc}")


if __name__ == "__main__":
    main()