import json
from pathlib import Path

from project_assistant.indexer.retrieval.query_project_index import hybrid_search


def build_project_context(
    *,
    project_name: str,
    query: str,
    index_path: str,
    meta_path: str,
    embedding_model: str = "BAAI/bge-base-en-v1.5",
    top_k: int = 6,
    max_chars_per_chunk: int = 1800,
) -> dict:
    result = hybrid_search(
        project_name=project_name,
        query=query,
        index_path=index_path,
        meta_path=meta_path,
        embedding_model=embedding_model,
        top_k=top_k,
    )

    context_blocks = []
    seen = set()

    for item in result["vector_results"]:
        row = item["chunk"]
        cid = row["chunk_id"]
        if cid in seen:
            continue
        seen.add(cid)

        block = {
            "source": "vector",
            "score": round(item["score"], 4),
            "chunk_id": cid,
            "chunk_type": row.get("chunk_type"),
            "file_name": row.get("file_name"),
            "function_name": row.get("function_name"),
            "relative_path": row.get("relative_path"),
            "text": (row.get("text") or "")[:max_chars_per_chunk],
        }
        context_blocks.append(block)

    return {
        "project_name": project_name,
        "query": query,
        "vector_results": result["vector_results"],
        "sql_file_matches": result["sql_file_matches"],
        "sql_meta_matches": result["sql_meta_matches"],
        "context_blocks": context_blocks,
    }


def render_project_context_text(context_obj: dict) -> str:
    lines = []
    lines.append(f"Project search scope: {context_obj['project_name']}")
    lines.append(f"User query: {context_obj['query']}")
    lines.append("")

    if context_obj.get("sql_file_matches"):
        lines.append("SQL file matches:")
        for row in context_obj["sql_file_matches"][:8]:
            lines.append(f"- {row['file_name']} | {row['relative_path']}")
        lines.append("")

    if context_obj.get("sql_meta_matches"):
        lines.append("SQL metadata matches:")
        for row in context_obj["sql_meta_matches"][:8]:
            lines.append(f"- {row['file_name']} | {row['relative_path']}")
        lines.append("")

    lines.append("Top retrieved project chunks:")
    for i, block in enumerate(context_obj.get("context_blocks", []), start=1):
        lines.append(f"\n[{i}] type={block.get('chunk_type')} file={block.get('file_name')} function={block.get('function_name')} score={block.get('score')}")
        lines.append(block.get("text", ""))

    return "\n".join(lines).strip()
