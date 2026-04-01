import argparse
import json
from pathlib import Path

import faiss
import numpy as np

from project_assistant.ai.embedding_service import EmbeddingService


def load_chunks(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def save_meta(rows, path: Path):
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            meta = {
                "chunk_id": row["chunk_id"],
                "project_name": row["project_name"],
                "chunk_type": row["chunk_type"],
                "file_id": row.get("file_id"),
                "file_name": row.get("file_name"),
                "relative_path": row.get("relative_path"),
                "function_name": row.get("function_name"),
                "metadata": row.get("metadata", {}),
                "text": row["text"],
            }
            f.write(json.dumps(meta, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Embed project chunks and build FAISS index.")
    parser.add_argument("--chunks-jsonl", required=True)
    parser.add_argument("--index-out", required=True)
    parser.add_argument("--embeddings-out", required=True)
    parser.add_argument("--meta-out", required=True)
    parser.add_argument("--embedding-model", default="BAAI/bge-base-en-v1.5")
    args = parser.parse_args()

    chunks_path = Path(args.chunks_jsonl)
    rows = load_chunks(chunks_path)
    if not rows:
        raise SystemExit("No chunks found.")

    texts = [row["text"] for row in rows]

    emb = EmbeddingService(model_name=args.embedding_model)
    vecs = emb.embed_texts(texts, normalize_embeddings=True, convert_to_numpy=True)

    dim = vecs.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(vecs)

    Path(args.index_out).parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, args.index_out)
    np.save(args.embeddings_out, vecs)
    save_meta(rows, Path(args.meta_out))

    print(f"[OK] Indexed {len(rows)} chunks")
    print(f"[OK] Index: {args.index_out}")
    print(f"[OK] Embeddings: {args.embeddings_out}")
    print(f"[OK] Meta: {args.meta_out}")


if __name__ == "__main__":
    main()
