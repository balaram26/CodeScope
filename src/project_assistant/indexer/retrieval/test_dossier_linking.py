from __future__ import annotations

import argparse
import json
from pathlib import Path

from project_assistant.indexer.retrieval.query_project_index import hybrid_search


def load_meta(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def find_related_dossiers(meta_rows: list[dict], retrieved_chunks: list[dict], limit: int = 10):
    dossier_rows = [
        r for r in meta_rows
        if (r.get("chunk_type") or "").lower() in {"script_dossier", "workflow_dossier"}
    ]

    related_paths = set()
    for item in retrieved_chunks:
        row = item["chunk"]
        rel_path = row.get("relative_path")
        if rel_path:
            related_paths.add(rel_path)

    matched = []

    for dossier in dossier_rows:
        text = (dossier.get("text") or "").lower()

        score = 0
        matched_paths = []

        for rel_path in related_paths:
            rel_path_l = rel_path.lower()
            file_name_l = Path(rel_path).name.lower()

            if rel_path_l and rel_path_l in text:
                score += 3
                matched_paths.append(rel_path)
            elif file_name_l and file_name_l in text:
                score += 2
                matched_paths.append(rel_path)

        if score > 0:
            matched.append({
                "score": score,
                "chunk_id": dossier.get("chunk_id"),
                "chunk_type": dossier.get("chunk_type"),
                "file_name": dossier.get("file_name"),
                "relative_path": dossier.get("relative_path"),
                "matched_paths": sorted(set(matched_paths)),
                "text_preview": (dossier.get("text") or "")[:500],
            })

    matched.sort(key=lambda x: x["score"], reverse=True)
    return matched[:limit]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-name", required=True)
    ap.add_argument("--query", required=True)
    ap.add_argument("--index-path", required=True)
    ap.add_argument("--meta-path", required=True)
    ap.add_argument("--embedding-model", default="BAAI/bge-base-en-v1.5")
    ap.add_argument("--top-k", type=int, default=8)
    args = ap.parse_args()

    print("\n=== RUN HYBRID SEARCH ===")
    result = hybrid_search(
        project_name=args.project_name,
        query=args.query,
        index_path=args.index_path,
        meta_path=args.meta_path,
        embedding_model=args.embedding_model,
        top_k=args.top_k,
    )

    vector_results = result["vector_results"]
    meta_rows = load_meta(Path(args.meta_path))

    print("\n=== TOP RETRIEVED CHUNKS ===")
    for i, item in enumerate(vector_results, start=1):
        row = item["chunk"]
        print(f"\n[{i}] score={item['score']:.4f}")
        print("chunk_type:", row.get("chunk_type"))
        print("file_name:", row.get("file_name"))
        print("relative_path:", row.get("relative_path"))
        print("chunk_id:", row.get("chunk_id"))
        print("text preview:", (row.get("text") or "")[:400])

    dossier_matches = find_related_dossiers(meta_rows, vector_results, limit=10)

    print("\n=== RELATED DOSSIERS ===")
    if not dossier_matches:
        print("No related dossier chunks found.")
    else:
        for i, d in enumerate(dossier_matches, start=1):
            print(f"\n[{i}] dossier_score={d['score']}")
            print("chunk_type:", d["chunk_type"])
            print("file_name:", d["file_name"])
            print("relative_path:", d["relative_path"])
            print("chunk_id:", d["chunk_id"])
            print("matched_paths:", d["matched_paths"])
            print("text preview:", d["text_preview"])

    print("\n=== SQL FILE MATCHES ===")
    for row in result["sql_file_matches"]:
        print(f"{row['file_id']} | {row['file_name']} | {row['relative_path']}")

    print("\n=== SQL METADATA MATCHES ===")
    for row in result["sql_meta_matches"]:
        print(f"{row['file_id']} | {row['file_name']} | {row['relative_path']}")


if __name__ == "__main__":
    main()
