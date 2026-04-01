from __future__ import annotations
import argparse
import re
import json
from pathlib import Path

import faiss
import numpy as np

from project_assistant.ai.embedding_service import EmbeddingService
from project_assistant.indexer.db import db_cursor


HELPER_FUNCTION_NAMES = {
    "load_rds", "save_rds", "log_msg", "pad_pos", "exists_cp", "force_step"
}

LOW_VALUE_ROLES = {"io_helper", "utility"}

def _detect_query_mode(query: str) -> str:
    q = re.sub(r"\s+", " ", query.lower()).strip()

    if any(x in q for x in ["workflow", "pipeline", "stage", "orchestration", "flow"]):
        return "workflow"
    if any(x in q for x in ["sample", "samples", "count", "rows", "metadata", "sheet", "table", "dataset"]):
        return "data"
    if any(x in q for x in ["model", "modeling", "train", "test", "split", "validation", "lambda", "alpha", "glmnet", "limma", "regression", "mae", "metrics"]):
        return "method"
    if any(x in q for x in ["result", "results", "output", "outputs", "performance", "hits", "significant"]):
        return "result"

    return "general"


def _chunk_type_boost(chunk_type: str, mode: str) -> float:
    chunk_type = (chunk_type or "").lower()

    boost_map = {
        "workflow": {
            "workflow_dossier": 1.50,
            "script_dossier": 1.15,
            "project_doc": 1.10,
            "function_summary": 1.00,
            "file_summary": 1.00,
            "dataset_summary": 0.95,
        },
        "data": {
            "dataset_summary": 1.40,
            "project_doc": 1.15,
            "script_dossier": 1.10,
            "workflow_dossier": 1.00,
            "function_summary": 0.95,
            "file_summary": 0.95,
        },
        "method": {
            "script_dossier": 1.45,
            "function_summary": 1.20,
            "dataset_summary": 1.15,
            "project_doc": 1.05,
            "workflow_dossier": 1.00,
            "file_summary": 1.00,
        },
        "result": {
            "dataset_summary": 1.35,
            "project_doc": 1.15,
            "script_dossier": 1.10,
            "function_summary": 1.00,
            "workflow_dossier": 0.95,
            "file_summary": 1.00,
        },
        "general": {
            "script_dossier": 1.25,
            "workflow_dossier": 1.20,
            "dataset_summary": 1.10,
            "function_summary": 1.05,
            "project_doc": 1.00,
            "file_summary": 1.00,
        },
    }

    return boost_map.get(mode, boost_map["general"]).get(chunk_type, 1.0)


def _fetch_dossier_chunks_from_meta(meta: list[dict], limit: int = 6) -> list[dict]:
    results = []

    for row in meta:
        chunk_type = (row.get("chunk_type") or "").lower()
        if chunk_type not in {"workflow_dossier", "script_dossier"}:
            continue

        results.append({
            "score": 0.5,
            "chunk": row,
        })

        if len(results) >= limit:
            break

    return results


def _rerank_results(results: list[dict], query: str) -> list[dict]:
    mode = _detect_query_mode(query)

    reranked = []
    for r in results:
        chunk = r.get("chunk") or {}
        meta = chunk.get("metadata") or {}
        chunk_type = chunk.get("chunk_type") or meta.get("chunk_type") or ""
        score = float(r.get("score", 0.0))
        boosted_score = score * _chunk_type_boost(chunk_type, mode)

        r2 = dict(r)
        r2["score"] = boosted_score
        reranked.append(r2)

    reranked.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return reranked


def load_meta(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def re_split_simple(text: str):
    import re
    return [x for x in re.split(r"[^a-zA-Z0-9_.*/-]+", text) if x]


def _normalize(text: str) -> str:
    return (text or "").strip().lower()


def normalize_artifact_name(name: str) -> str:
    import re
    name = _normalize(name)
    if "/" in name:
        name = name.split("/")[-1]
    name = name.replace("%s", "*").replace("{}", "*")
    name = re.sub(r"\s+", " ", name)
    return name


def artifact_tokens(name: str) -> list[str]:
    import re
    base = normalize_artifact_name(name)
    return [t for t in re.split(r"[^a-z0-9]+", base) if t]


def artifact_similarity(a: str, b: str) -> float:
    a_norm = normalize_artifact_name(a)
    b_norm = normalize_artifact_name(b)

    if not a_norm or not b_norm:
        return 0.0
    if a_norm == b_norm:
        return 1.0

    a_tokens = set(artifact_tokens(a_norm))
    b_tokens = set(artifact_tokens(b_norm))
    if not a_tokens or not b_tokens:
        return 0.0

    inter = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    if union == 0:
        return 0.0

    return inter / union


def _extract_artifact_terms(query: str) -> list[str]:
    import re
    terms = re.findall(
        r"[A-Za-z0-9_*./-]+\.(?:csv|tsv|txt|json|yaml|yml|rds|rdata|RData|png|pdf|xlsx|xls|npz|npy|pkl)",
        query
    )
    return list(dict.fromkeys(terms))


def _query_intent_hints(query: str) -> dict:
    q = _normalize(query)
    return {
        "asks_which_script": any(x in q for x in ["which script", "which file", "where is", "what creates", "what generates"]),
        "asks_use": any(x in q for x in ["uses", "use", "read", "input", "depends on"]),
        "asks_output": any(x in q for x in ["creates", "writes", "generates", "output"]),
        "artifact_terms": _extract_artifact_terms(query),
    }


def keyword_candidates(project_name: str, query: str, limit: int = 20):
    q = f"%{query.lower()}%"
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT f.file_id, f.file_name, f.relative_path
            FROM files f
            JOIN projects p ON f.project_id = p.project_id
            WHERE p.project_name = ?
              AND (
                lower(f.file_name) LIKE ?
                OR lower(f.relative_path) LIKE ?
              )
            ORDER BY f.file_id ASC
            LIMIT ?
        """, (project_name, q, q, limit))
        return cur.fetchall()


def metadata_candidates(project_name: str, query: str, limit: int = 20):
    q = _normalize(query)
    artifact_terms = [normalize_artifact_name(x) for x in _extract_artifact_terms(query)]
    results = []

    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT
                f.file_id,
                f.file_name,
                f.relative_path,
                m.metadata_json
            FROM files f
            JOIN projects p ON f.project_id = p.project_id
            JOIN file_metadata_merged m ON f.file_id = m.file_id
            WHERE p.project_name = ?
            ORDER BY m.merged_id DESC
        """, (project_name,))
        rows = cur.fetchall()

    seen = set()
    for row in rows:
        try:
            meta = json.loads(row["metadata_json"])
        except Exception:
            continue

        candidate_artifacts = []
        for key in [
            "final_file_inputs",
            "final_file_outputs",
            "final_checkpoint_inputs",
            "final_checkpoint_outputs",
            "final_plot_outputs",
            "ir_string_path_candidates",
        ]:
            vals = meta.get(key, [])
            if isinstance(vals, list):
                candidate_artifacts.extend([normalize_artifact_name(str(x)) for x in vals if x])

        matched = False
        if artifact_terms:
            best = 0.0
            for q_art in artifact_terms:
                for cand in candidate_artifacts:
                    best = max(best, artifact_similarity(q_art, cand))
            matched = best >= 0.55
        else:
            searchable_lists = []
            for key in [
                "defined_functions",
                "imports_or_libraries",
                "secondary_roles",
                "final_file_inputs",
                "final_file_outputs",
                "final_checkpoint_inputs",
                "final_checkpoint_outputs",
                "final_plot_outputs",
            ]:
                vals = meta.get(key, [])
                if isinstance(vals, list):
                    searchable_lists.extend([str(x) for x in vals if x])

            searchable_text = " | ".join(searchable_lists).lower()
            query_terms = [t for t in re_split_simple(q) if len(t) >= 3]
            overlap = sum(1 for t in query_terms if t in searchable_text)
            matched = overlap >= 2 or q in searchable_text

        if matched:
            fid = row["file_id"]
            if fid not in seen:
                results.append({
                    "file_id": fid,
                    "file_name": row["file_name"],
                    "relative_path": row["relative_path"],
                })
                seen.add(fid)

        if len(results) >= limit:
            break

    return results


def _candidate_artifacts_from_chunk(row: dict) -> list[str]:
    meta = row.get("metadata", {}) or {}
    chunk_type = row.get("chunk_type")

    artifacts = []

    if chunk_type == "file_summary":
        for key in ["file_inputs", "file_outputs", "checkpoint_inputs", "checkpoint_outputs", "plot_outputs"]:
            vals = meta.get(key, [])
            if isinstance(vals, list):
                artifacts.extend([normalize_artifact_name(str(x)) for x in vals if x])

    elif chunk_type == "function_summary":
        for key in ["likely_inputs", "likely_outputs"]:
            vals = meta.get(key, [])
            if isinstance(vals, list):
                for x in vals:
                    if isinstance(x, dict) and x.get("name"):
                        artifacts.append(normalize_artifact_name(x["name"]))

    return list(dict.fromkeys([x for x in artifacts if x]))


def _score_chunk(row: dict, vector_score: float, query: str) -> float:
    score = float(vector_score)

    chunk_type = row.get("chunk_type", "")
    fn_name = _normalize(row.get("function_name") or "")
    meta = row.get("metadata", {}) or {}
    role = _normalize(meta.get("role") or meta.get("dominant_role") or "")
    text = _normalize(row.get("text") or "")
    q = _normalize(query)
    hints = _query_intent_hints(query)

    if hints["asks_which_script"]:
        if chunk_type == "file_summary":
            score += 0.08
        elif chunk_type == "function_summary":
            score -= 0.03

    if chunk_type == "project_doc":
        score -= 0.02

    if chunk_type == "function_summary":
        if role in LOW_VALUE_ROLES:
            score -= 0.05
        if fn_name in HELPER_FUNCTION_NAMES:
            score -= 0.06
        if fn_name.startswith(("load_", "save_", "log_")):
            score -= 0.04

    query_terms = [t for t in re_split_simple(q) if len(t) >= 3]
    overlap = sum(1 for t in query_terms if t in text)
    score += min(0.06, overlap * 0.01)

    artifact_terms = [normalize_artifact_name(t) for t in hints["artifact_terms"]]
    if artifact_terms:
        candidate_artifacts = _candidate_artifacts_from_chunk(row)
        best_artifact_sim = 0.0
        for q_art in artifact_terms:
            for cand in candidate_artifacts:
                best_artifact_sim = max(best_artifact_sim, artifact_similarity(q_art, cand))

        score += best_artifact_sim * 0.16

        if best_artifact_sim >= 0.95 and chunk_type == "file_summary":
            score += 0.05

    return score


def hybrid_search(
    *,
    project_name: str,
    query: str,
    index_path: str,
    meta_path: str,
    embedding_model: str = "BAAI/bge-base-en-v1.5",
    top_k: int = 8,
):
    meta = load_meta(Path(meta_path))
    index = faiss.read_index(index_path)

    emb = EmbeddingService(model_name=embedding_model)
    qvec = emb.embed_texts([query], normalize_embeddings=True, convert_to_numpy=True)

    raw_k = max(top_k * 3, 20)
    scores, idxs = index.search(np.asarray(qvec, dtype=np.float32), raw_k)

    ranked = []
    for score, idx in zip(scores[0], idxs[0]):
        if idx < 0 or idx >= len(meta):
            continue
        row = meta[idx]
        final_score = _score_chunk(row, float(score), query)
        ranked.append((final_score, row))

    ranked.sort(key=lambda x: x[0], reverse=True)

    vector_results = []
    seen = set()
    keep_k = max(top_k * 3, 15)

    for score, row in ranked:
        cid = row["chunk_id"]
        if cid in seen:
            continue
        vector_results.append({
            "score": score,
            "chunk": row,
        })
        seen.add(cid)
        if len(vector_results) >= keep_k:
            break

    sql_file_matches = keyword_candidates(project_name, query, limit=10)
    sql_meta_matches = metadata_candidates(project_name, query, limit=10)
    
    dossier_chunks = _fetch_dossier_chunks_from_meta(meta, limit=6)
    existing_ids = {r["chunk"]["chunk_id"] for r in vector_results}
    for d in dossier_chunks:
        if d["chunk"]["chunk_id"] not in existing_ids:
            vector_results.append(d)

    vector_results = _rerank_results(vector_results, query)
    vector_results = vector_results[:top_k]

    return {
        "vector_results": vector_results,
        "sql_file_matches": sql_file_matches,
        "sql_meta_matches": sql_meta_matches,
    }


def main():
    parser = argparse.ArgumentParser(description="Hybrid query over project chunks.")
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--index-path", required=True)
    parser.add_argument("--meta-path", required=True)
    parser.add_argument("--embedding-model", default="BAAI/bge-base-en-v1.5")
    parser.add_argument("--top-k", type=int, default=8)
    args = parser.parse_args()

    result = hybrid_search(
        project_name=args.project_name,
        query=args.query,
        index_path=args.index_path,
        meta_path=args.meta_path,
        embedding_model=args.embedding_model,
        top_k=args.top_k,
    )

    print("\n=== VECTOR RESULTS ===")
    for item in result["vector_results"]:
        score = item["score"]
        row = item["chunk"]
        print(f"\n[{score:.4f}] {row['chunk_type']} | {row.get('file_name')} | {row.get('function_name')}")
        print(row["text"][:800])

    print("\n=== SQL FILE MATCHES ===")
    for row in result["sql_file_matches"]:
        print(f"{row['file_id']} | {row['file_name']} | {row['relative_path']}")

    print("\n=== SQL METADATA MATCHES ===")
    for row in result["sql_meta_matches"]:
        print(f"{row['file_id']} | {row['file_name']} | {row['relative_path']}")


if __name__ == "__main__":
    main()