from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from project_assistant.indexer.config import DB_PATH, GLOBAL_INDEX_DIR
from project_assistant.indexer.db import db_cursor


class ProjectHelper:
    def __init__(self, project_db_path: str | None = None):
        self.project_db_path = Path(project_db_path) if project_db_path else DB_PATH
        self.index_root = GLOBAL_INDEX_DIR
        self.embedding_model = "BAAI/bge-base-en-v1.5"
        self.embedder = None

    def _get_embedder(self):
        if self.embedder is None:
            from project_assistant.ai.embedding_service import EmbeddingService
            self.embedder = EmbeddingService(model_name=self.embedding_model)
        return self.embedder

    def get_status(self, source_system_id: str) -> dict[str, Any]:
        project_name = source_system_id

        stats = {
            "total_files": 0,
            "parsed_files": 0,
            "deduped_files": 0,
            "summarized_files": 0,
            "function_metadata_rows": 0,
            "merged_metadata_rows": 0,
            "has_chunks_jsonl": False,
            "has_faiss_index": False,
            "has_chunks_meta": False,
        }

        with db_cursor() as (_, cur):
            cur.execute("""
                SELECT project_id
                FROM projects
                WHERE project_name = ?
            """, (project_name,))
            prow = cur.fetchone()

            if not prow:
                return {
                    "status": "registered",
                    "current_stage": "ingest",
                    "short_error": None,
                    "stats": stats,
                }

            project_id = prow["project_id"]

            cur.execute("SELECT COUNT(*) AS cnt FROM files WHERE project_id = ?", (project_id,))
            stats["total_files"] = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM files
                WHERE project_id = ?
                  AND status IN ('parsed', 'summarized')
            """, (project_id,))
            stats["parsed_files"] = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM files
                WHERE project_id = ?
                  AND COALESCE(is_duplicate, 0) = 0
            """, (project_id,))
            stats["deduped_files"] = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM file_summaries fs
                JOIN files f ON fs.file_id = f.file_id
                WHERE f.project_id = ?
            """, (project_id,))
            stats["summarized_files"] = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM function_metadata_llm m
                JOIN files f ON m.file_id = f.file_id
                WHERE f.project_id = ?
            """, (project_id,))
            stats["function_metadata_rows"] = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM file_metadata_merged m
                JOIN files f ON m.file_id = f.file_id
                WHERE f.project_id = ?
            """, (project_id,))
            stats["merged_metadata_rows"] = cur.fetchone()["cnt"]

        chunks_jsonl = self.index_root / f"{project_name}_chunks.jsonl"
        faiss_index = self.index_root / f"{project_name}.faiss.index"
        chunks_meta = self.index_root / f"{project_name}_chunks_meta.jsonl"

        stats["has_chunks_jsonl"] = chunks_jsonl.exists()
        stats["has_faiss_index"] = faiss_index.exists()
        stats["has_chunks_meta"] = chunks_meta.exists()

        if stats["has_faiss_index"] and stats["has_chunks_meta"]:
            status = "ready"
            stage = "index"
        elif stats["has_chunks_jsonl"]:
            status = "partial"
            stage = "chunks"
        elif stats["summarized_files"] > 0:
            status = "partial"
            stage = "summaries"
        elif stats["merged_metadata_rows"] > 0:
            status = "partial"
            stage = "merge"
        elif stats["function_metadata_rows"] > 0:
            status = "partial"
            stage = "llm"
        elif stats["parsed_files"] > 0:
            status = "partial"
            stage = "parse"
        elif stats["total_files"] > 0:
            status = "running"
            stage = "ingest"
        else:
            status = "registered"
            stage = "ingest"

        return {
            "status": status,
            "current_stage": stage,
            "stats": stats,
        }

    def search(
        self,
        question: str,
        source_system_ids: list[str],
        top_k: int = 8,
    ) -> list[dict[str, Any]]:
        import faiss

        embedder = self._get_embedder()
        query_vec = embedder.embed_texts(
            [question],
            normalize_embeddings=True,
            convert_to_numpy=True,
        )

        results: list[dict[str, Any]] = []

        for project_name in source_system_ids:
            index_path = self.index_root / f"{project_name}.faiss.index"
            meta_path = self.index_root / f"{project_name}_chunks_meta.jsonl"

            if not index_path.exists() or not meta_path.exists():
                continue

            index = faiss.read_index(str(index_path))

            meta_rows = []
            with meta_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    meta_rows.append(json.loads(line))

            if not meta_rows:
                continue

            D, I = index.search(query_vec, top_k)

            for score, idx in zip(D[0], I[0]):
                if idx < 0 or idx >= len(meta_rows):
                    continue

                row = meta_rows[idx]
                results.append({
                    "source_system_id": project_name,
                    "display_name": project_name,
                    "file_path": row.get("relative_path"),
                    "chunk_id": row.get("chunk_id"),
                    "score": float(score),
                    "text": row.get("text", ""),
                    "metadata": {
                        "chunk_type": row.get("chunk_type"),
                        "file_name": row.get("file_name"),
                        "function_name": row.get("function_name"),
                        **(row.get("metadata") or {}),
                    },
                })

        results.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        return results[:top_k]

    def list_projects_for_ui(self) -> list[dict[str, Any]]:
        with db_cursor() as (_, cur):
            cur.execute("""
                SELECT project_id, project_name
                FROM projects
                ORDER BY lower(project_name)
            """)
            rows = cur.fetchall()

        return [
            {
                "project_id": row["project_id"],
                "source_system_id": row["project_name"],
                "display_name": row["project_name"],
            }
            for row in rows
        ]

    def start_import(
        self,
        project_name: str,
        source_path: str,
        include_patterns=None,
        exclude_patterns=None,
    ) -> dict[str, Any]:
        import sqlite3
        import subprocess
        import sys

        cmd = [
            sys.executable,
            "-m",
            "project_assistant.indexer.run_project_ingest",
            "--input",
            source_path,
            "--project-name",
            project_name,
            "--source-kind",
            "mixed",
            "--bundle-name",
            Path(source_path).stem,
            "--create-if-missing",
        ]

        subprocess.run(cmd, check=True)

        conn = sqlite3.connect(str(self.project_db_path))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT project_name
                FROM projects
                WHERE lower(project_name) = lower(?)
                   OR replace(lower(project_name), '_', ' ') = replace(lower(?), '_', ' ')
                   OR replace(lower(project_name), ' ', '_') = replace(lower(?), ' ', '_')
                ORDER BY project_id DESC
                LIMIT 1
                """,
                (project_name, project_name, project_name),
            ).fetchone()

            if row is None:
                row = conn.execute(
                    """
                    SELECT project_name
                    FROM projects
                    ORDER BY project_id DESC
                    LIMIT 1
                    """
                ).fetchone()

            if row is None:
                raise RuntimeError("Project ingest completed, but no project was found in the backend DB.")

            backend_project_name = row["project_name"]
        finally:
            conn.close()

        return {
            "source_system_id": backend_project_name,
            "display_name": project_name,
            "source_path": source_path,
            "source_db_path": str(self.project_db_path),
            "backend_job_id": f"proj-{backend_project_name}",
        }

    def update_project_source(
        self,
        source_system_id: str,
        source_path: str,
        include_patterns=None,
        exclude_patterns=None,
        merge_mode: str = "overwrite_existing",
    ) -> dict[str, Any]:
        import sqlite3
        import subprocess
        import sys

        if merge_mode not in {"overwrite_existing", "add_new_only"}:
            raise ValueError(f"Unsupported merge_mode: {merge_mode}")

        backend_project_name = source_system_id
        bundle_name = Path(source_path).stem

        cmd = [
            sys.executable,
            "-m",
            "project_assistant.indexer.run_project_ingest",
            "--input",
            source_path,
            "--project-name",
            backend_project_name,
            "--source-kind",
            "mixed",
            "--bundle-name",
            bundle_name,
            "--create-if-missing",
            "--merge-mode",
            merge_mode,
        ]

        subprocess.run(cmd, check=True)

        conn = sqlite3.connect(str(self.project_db_path))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT project_name
                FROM projects
                WHERE lower(project_name) = lower(?)
                LIMIT 1
                """,
                (backend_project_name,),
            ).fetchone()

            if row is None:
                raise RuntimeError(
                    f"Project update completed, but backend project was not found: {backend_project_name}"
                )

            backend_project_name = row["project_name"]

            # Get the newest source created for this project+bundle
            source_row = conn.execute(
                """
                SELECT s.source_id
                FROM sources s
                JOIN projects p ON s.project_id = p.project_id
                WHERE p.project_name = ?
                  AND s.bundle_name = ?
                ORDER BY s.source_id DESC
                LIMIT 1
                """,
                (backend_project_name, bundle_name),
            ).fetchone()

            affected_file_ids: list[int] = []
            if source_row is not None:
                source_id = source_row["source_id"]
                file_rows = conn.execute(
                    """
                    SELECT file_id
                    FROM files
                    WHERE source_id = ?
                    ORDER BY file_id ASC
                    """,
                    (source_id,),
                ).fetchall()
                affected_file_ids = [int(r["file_id"]) for r in file_rows]
        finally:
            conn.close()

        return {
            "source_system_id": backend_project_name,
            "display_name": backend_project_name,
            "source_path": source_path,
            "source_db_path": str(self.project_db_path),
            "backend_job_id": f"proj-{backend_project_name}",
            "merge_mode": merge_mode,
            "affected_file_ids": affected_file_ids,
        }