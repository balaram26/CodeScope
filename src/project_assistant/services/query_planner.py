from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass


@dataclass(frozen=True)
class QueryPlan:
    mode: str
    expanded_query: str
    boost_terms: list[str]


WORKFLOW_TERMS = {
    "workflow", "pipeline", "process", "stages", "stage", "design",
    "end-to-end", "end to end", "flow", "orchestration",
}
DATA_TERMS = {
    "sample", "samples", "n", "count", "rows", "metadata", "cohort",
    "group", "sheet", "table", "samplesheet", "metasheet", "dataset",
}
METHOD_TERMS = {
    "model", "modeling", "modelling", "train", "test", "split",
    "validation", "cross-validation", "cv", "lambda", "alpha",
    "coefficients", "coefficient", "glmnet", "limma", "regression",
    "classifier", "metrics", "mae", "accuracy", "r2", "r", "predicted",
}
RESULT_TERMS = {
    "result", "results", "output", "outputs", "metric", "metrics",
    "reported", "performance", "hits", "significant", "summary",
}


MODE_BOOSTS = {
    "workflow": [
        "workflow", "pipeline", "stage", "input", "output", "manifest", "summary", "nextflow"
    ],
    "data": [
        "sample", "samples", "rows", "metadata", "samplesheet", "metasheet",
        "cohort", "group", "table", "n"
    ],
    "method": [
        "model", "train", "test", "split", "validation", "cv", "lambda",
        "alpha", "coefficient", "glmnet", "limma", "regression", "metrics",
        "mae", "predicted"
    ],
    "result": [
        "results", "metrics", "summary", "hits", "significant", "performance",
        "output", "reported"
    ],
    "general": [],
}


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def detect_query_mode(question: str) -> str:
    q = normalize_text(question)

    workflow_score = sum(1 for t in WORKFLOW_TERMS if t in q)
    data_score = sum(1 for t in DATA_TERMS if t in q)
    method_score = sum(1 for t in METHOD_TERMS if t in q)
    result_score = sum(1 for t in RESULT_TERMS if t in q)

    scores = {
        "workflow": workflow_score,
        "data": data_score,
        "method": method_score,
        "result": result_score,
    }

    best_mode = max(scores, key=scores.get)
    if scores[best_mode] == 0:
        return "general"
    return best_mode


def build_query_plan(question: str) -> QueryPlan:
    mode = detect_query_mode(question)
    boost_terms = MODE_BOOSTS.get(mode, [])
    expanded_query = question.strip()

    if boost_terms:
        expanded_query = f"{expanded_query}\nRelated terms: " + ", ".join(boost_terms)

    return QueryPlan(
        mode=mode,
        expanded_query=expanded_query,
        boost_terms=boost_terms,
    )


def infer_evidence_role(ev) -> str:
    text = (getattr(ev, "text", "") or "").lower()
    file_path = (getattr(ev, "file_path", "") or "").lower()
    metadata = getattr(ev, "metadata", {}) or {}
    chunk_type = str(metadata.get("chunk_type") or getattr(ev, "chunk_type", "") or "").lower()
    doc_role = str(metadata.get("doc_role") or "").lower()

    if chunk_type == "workflow_dossier" or doc_role == "workflow":
        return "workflow_dossier"
    if chunk_type == "script_dossier" or doc_role == "script":
        return "script_dossier"
    if chunk_type in {"function_summary", "script_summary"}:
        return "function_summary"
    if chunk_type in {"dataset_summary"}:
        return "dataset_summary"
    if chunk_type in {"project_doc"}:
        return "project_doc"

    if file_path.endswith((".csv", ".tsv", ".xlsx", ".xls")):
        return "dataset_summary"
    if file_path.endswith((".py", ".r", ".nf", ".sh")):
        return "function_summary"
    if file_path.endswith((".md", ".txt")):
        return "project_doc"

    if "train" in text or "test" in text or "split" in text:
        return "metadata"
    return "general"


def select_diverse_evidence(evidence: list, top_k: int, mode: str) -> list:
    if len(evidence) <= top_k:
        return evidence

    grouped: dict[str, list] = defaultdict(list)
    for ev in evidence:
        grouped[infer_evidence_role(ev)].append(ev)

    role_order = {
        "workflow": ["workflow_dossier", "script_dossier", "function_summary", "dataset_summary", "project_doc", "metadata", "general"],
        "data": ["dataset_summary", "project_doc", "script_dossier", "function_summary", "metadata", "general"],
        "method": ["script_dossier", "function_summary", "dataset_summary", "project_doc", "metadata", "general"],
        "result": ["dataset_summary", "script_dossier", "project_doc", "function_summary", "metadata", "general"],
        "general": ["script_dossier", "workflow_dossier", "function_summary", "dataset_summary", "project_doc", "metadata", "general"],
    }.get(mode, ["project_doc", "function_summary", "dataset_summary", "metadata", "general"])

    selected = []
    seen = set()

    for role in role_order:
        for ev in grouped.get(role, []):
            key = (getattr(ev, "file_path", None), getattr(ev, "chunk_id", None))
            if key not in seen:
                selected.append(ev)
                seen.add(key)
                break
        if len(selected) >= top_k:
            return selected[:top_k]

    for ev in evidence:
        key = (getattr(ev, "file_path", None), getattr(ev, "chunk_id", None))
        if key in seen:
            continue
        selected.append(ev)
        seen.add(key)
        if len(selected) >= top_k:
            break

    return selected[:top_k]