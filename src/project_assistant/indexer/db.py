import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from project_assistant.indexer.config import DB_PATH, ensure_runtime_dirs

def get_project_by_name_or_raise(project_name: str):
    row = get_project_by_name(project_name)
    if not row:
        raise ValueError(f"Project not found: {project_name}")
    return row


def get_summarized_files_for_project(project_name: str):
    query = """
        SELECT
            f.file_id,
            f.project_id,
            f.relative_path,
            f.absolute_path,
            f.file_name,
            f.file_ext,
            f.file_type,
            f.is_duplicate,
            f.canonical_file_id,
            s.summary_type,
            s.summary_text,
            s.model_name
        FROM files f
        JOIN projects p ON f.project_id = p.project_id
        JOIN file_summaries s ON f.file_id = s.file_id
        WHERE p.project_name = ?
          AND COALESCE(f.is_duplicate, 0) = 0
        ORDER BY f.file_type, f.file_name
    """
    with db_cursor() as (_, cur):
        cur.execute(query, (project_name,))
        return cur.fetchall()

def get_file_metadata_for_file_ids(file_ids: list[int]) -> dict[int, dict]:
    if not file_ids:
        return {}

    placeholders = ",".join(["?"] * len(file_ids))
    out = {}

    with db_cursor() as (_, cur):
        # Prefer merged metadata
        cur.execute(f"""
            SELECT file_id, metadata_json
            FROM file_metadata_merged
            WHERE file_id IN ({placeholders})
            ORDER BY merged_id DESC
        """, tuple(file_ids))
        rows = cur.fetchall()

    for row in rows:
        fid = row["file_id"]
        if fid not in out:
            try:
                out[fid] = json.loads(row["metadata_json"])
            except Exception:
                out[fid] = {}

    # Fill remaining from parser metadata
    missing = [fid for fid in file_ids if fid not in out]
    if missing:
        placeholders2 = ",".join(["?"] * len(missing))
        with db_cursor() as (_, cur):
            cur.execute(f"""
                SELECT file_id, metadata_json
                FROM file_metadata
                WHERE file_id IN ({placeholders2})
                ORDER BY metadata_id DESC
            """, tuple(missing))
            rows2 = cur.fetchall()

        for row in rows2:
            fid = row["file_id"]
            if fid not in out:
                try:
                    out[fid] = json.loads(row["metadata_json"])
                except Exception:
                    out[fid] = {}

    return out


def ensure_column_exists(table_name: str, column_name: str, column_def: str) -> None:
    with db_cursor() as (_, cur):
        cur.execute(f"PRAGMA table_info({table_name})")
        cols = {row["name"] for row in cur.fetchall()}
        if column_name not in cols:
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    ensure_runtime_dirs()
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_cursor(db_path: Optional[Path] = None):
    conn = get_connection(db_path)
    cur = conn.cursor()
    try:
        yield conn, cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Optional[Path] = None) -> None:
    with db_cursor(db_path) as (_, cur):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT NOT NULL UNIQUE,
            description TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            source_origin TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            bundle_name TEXT,
            original_path TEXT NOT NULL,
            managed_path TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            FOREIGN KEY(project_id) REFERENCES projects(project_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            relative_path TEXT NOT NULL,
            absolute_path TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_ext TEXT,
            file_type TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(source_id) REFERENCES sources(source_id),
            FOREIGN KEY(project_id) REFERENCES projects(project_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS ingest_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            notes TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS file_metadata (
            metadata_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            parser_name TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        )
        """)

        ensure_column_exists("files", "is_duplicate", "INTEGER NOT NULL DEFAULT 0")
        ensure_column_exists("files", "canonical_file_id", "INTEGER")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS file_summaries (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            summary_type TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            model_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS file_ir (
            ir_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            ir_type TEXT NOT NULL,
            ir_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS function_metadata_llm (
            llm_function_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            function_name TEXT NOT NULL,
            model_name TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS file_metadata_llm (
            llm_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            model_name TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS function_metadata_llm (
            llm_function_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            function_name TEXT NOT NULL,
            model_name TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS file_metadata_merged (
            merged_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            merge_version TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        )
        """)


def create_ingest_run(notes: str = "") -> int:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO ingest_runs (started_at, status, notes)
            VALUES (?, ?, ?)
        """, (now, "running", notes))
        return cur.lastrowid


def finalize_ingest_run(run_id: int, status: str, notes: str = "") -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    with db_cursor() as (_, cur):
        cur.execute("""
            UPDATE ingest_runs
            SET finished_at = ?, status = ?, notes = ?
            WHERE run_id = ?
        """, (now, status, notes, run_id))


def get_project_by_name(project_name: str):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT * FROM projects WHERE project_name = ?
        """, (project_name,))
        return cur.fetchone()


def create_project(project_name: str, description: str = "") -> int:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO projects (project_name, description, created_at, updated_at)
            VALUES (?, ?, ?, ?)
        """, (project_name, description, now, now))
        return cur.lastrowid


def get_or_create_project(project_name: str, description: str = "") -> int:
    row = get_project_by_name(project_name)
    if row:
        return row["project_id"]
    return create_project(project_name, description)


def create_source(
    project_id: int,
    source_origin: str,
    source_kind: str,
    bundle_name: str,
    original_path: str,
    managed_path: str,
) -> int:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO sources (
                project_id, source_origin, source_kind, bundle_name,
                original_path, managed_path, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            project_id, source_origin, source_kind, bundle_name,
            original_path, managed_path, now
        ))
        return cur.lastrowid


def register_file(
    source_id: int,
    project_id: int,
    relative_path: str,
    absolute_path: str,
    file_name: str,
    file_ext: str,
    file_type: str,
    size_bytes: int,
    sha256: str,
    status: str = "registered",
) -> int:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    with db_cursor() as (_, cur):
        cur.execute("""
            INSERT INTO files (
                source_id, project_id, relative_path, absolute_path,
                file_name, file_ext, file_type, size_bytes, sha256,
                status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_id, project_id, relative_path, absolute_path,
            file_name, file_ext, file_type, size_bytes, sha256,
            status, now, now
        ))
        return cur.lastrowid


def get_files_to_parse(project_name: Optional[str] = None, limit: Optional[int] = None):
    query = """
        SELECT f.*
        FROM files f
        JOIN projects p ON f.project_id = p.project_id
        WHERE f.status = 'registered'
    """
    params = []

    if project_name:
        query += " AND p.project_name = ?"
        params.append(project_name)

    query += " ORDER BY f.file_id ASC"

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with db_cursor() as (_, cur):
        cur.execute(query, tuple(params))
        return cur.fetchall()


def upsert_file_metadata(file_id: int, parser_name: str, metadata: dict) -> None:
    from datetime import datetime

    now = datetime.utcnow().isoformat(timespec="seconds")
    metadata_json = json.dumps(metadata, ensure_ascii=False, indent=2)

    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT metadata_id FROM file_metadata
            WHERE file_id = ? AND parser_name = ?
        """, (file_id, parser_name))
        row = cur.fetchone()

        if row:
            cur.execute("""
                UPDATE file_metadata
                SET metadata_json = ?, updated_at = ?
                WHERE metadata_id = ?
            """, (metadata_json, now, row["metadata_id"]))
        else:
            cur.execute("""
                INSERT INTO file_metadata (
                    file_id, parser_name, metadata_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
            """, (file_id, parser_name, metadata_json, now, now))


def update_file_status(file_id: int, status: str) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    with db_cursor() as (_, cur):
        cur.execute("""
            UPDATE files
            SET status = ?, updated_at = ?
            WHERE file_id = ?
        """, (status, now, file_id))

def get_project_id_by_name(project_name: str):
    row = get_project_by_name(project_name)
    return row["project_id"] if row else None


def get_project_files_for_dedup(project_name: Optional[str] = None):
    query = """
        SELECT f.*
        FROM files f
        JOIN projects p ON f.project_id = p.project_id
        WHERE 1=1
    """
    params = []

    if project_name:
        query += " AND p.project_name = ?"
        params.append(project_name)

    query += " ORDER BY f.project_id, f.sha256, f.file_id ASC"

    with db_cursor() as (_, cur):
        cur.execute(query, tuple(params))
        return cur.fetchall()


def mark_file_as_canonical(file_id: int) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    with db_cursor() as (_, cur):
        cur.execute("""
            UPDATE files
            SET is_duplicate = 0,
                canonical_file_id = ?,
                updated_at = ?
            WHERE file_id = ?
        """, (file_id, now, file_id))


def mark_file_as_duplicate(file_id: int, canonical_file_id: int) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    with db_cursor() as (_, cur):
        cur.execute("""
            UPDATE files
            SET is_duplicate = 1,
                canonical_file_id = ?,
                updated_at = ?
            WHERE file_id = ?
        """, (canonical_file_id, now, file_id))


def get_files_to_summarize(project_name: Optional[str] = None, limit: Optional[int] = None):
    query = """
        SELECT f.*
        FROM files f
        JOIN projects p ON f.project_id = p.project_id
        WHERE f.status = 'parsed'
          AND COALESCE(f.is_duplicate, 0) = 0
    """
    params = []

    if project_name:
        query += " AND p.project_name = ?"
        params.append(project_name)

    query += " ORDER BY f.file_id ASC"

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with db_cursor() as (_, cur):
        cur.execute(query, tuple(params))
        return cur.fetchall()

def upsert_file_summary(
    file_id: int,
    summary_type: str,
    summary_text: str,
    model_name: str = "heuristic_v1",
) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")

    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT summary_id FROM file_summaries
            WHERE file_id = ? AND summary_type = ?
        """, (file_id, summary_type))
        row = cur.fetchone()

        if row:
            cur.execute("""
                UPDATE file_summaries
                SET summary_text = ?, model_name = ?, updated_at = ?
                WHERE summary_id = ?
            """, (summary_text, model_name, now, row["summary_id"]))
        else:
            cur.execute("""
                INSERT INTO file_summaries (
                    file_id, summary_type, summary_text, model_name, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (file_id, summary_type, summary_text, model_name, now, now))


def get_latest_metadata_for_file(file_id: int):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT *
            FROM file_metadata
            WHERE file_id = ?
            ORDER BY metadata_id DESC
            LIMIT 1
        """, (file_id,))
        return cur.fetchone()

def delete_project_derived_data(project_name: str) -> None:
    """
    Remove parsed metadata, summaries, duplicate flags, and reset file status
    for one project, but keep project/source/file registry.
    """
    with db_cursor() as (_, cur):
        cur.execute("""
            DELETE FROM file_metadata
            WHERE file_id IN (
                SELECT f.file_id
                FROM files f
                JOIN projects p ON f.project_id = p.project_id
                WHERE p.project_name = ?
            )
        """, (project_name,))

        cur.execute("""
            DELETE FROM file_summaries
            WHERE file_id IN (
                SELECT f.file_id
                FROM files f
                JOIN projects p ON f.project_id = p.project_id
                WHERE p.project_name = ?
            )
        """, (project_name,))

        cur.execute("""
            UPDATE files
            SET status = 'registered',
                is_duplicate = 0,
                canonical_file_id = NULL
            WHERE project_id = (
                SELECT project_id
                FROM projects
                WHERE project_name = ?
            )
        """, (project_name,))

def delete_project_from_db(project_name: str) -> dict:
    """
    Hard delete project rows from DB.
    Returns managed paths for filesystem cleanup.
    """
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT project_id
            FROM projects
            WHERE project_name = ?
        """, (project_name,))
        row = cur.fetchone()

        if not row:
            raise ValueError(f"Project not found: {project_name}")

        project_id = row["project_id"]

        cur.execute("""
            SELECT managed_path
            FROM sources
            WHERE project_id = ?
        """, (project_id,))
        managed_paths = [r["managed_path"] for r in cur.fetchall()]

        cur.execute("""
            DELETE FROM file_metadata
            WHERE file_id IN (
                SELECT file_id
                FROM files
                WHERE project_id = ?
            )
        """, (project_id,))

        cur.execute("""
            DELETE FROM file_summaries
            WHERE file_id IN (
                SELECT file_id
                FROM files
                WHERE project_id = ?
            )
        """, (project_id,))

        cur.execute("DELETE FROM files WHERE project_id = ?", (project_id,))
        cur.execute("DELETE FROM sources WHERE project_id = ?", (project_id,))
        cur.execute("DELETE FROM projects WHERE project_id = ?", (project_id,))

    return {
        "project_id": project_id,
        "managed_paths": managed_paths,
    }


def get_files_for_ir(
    project_name: Optional[str] = None,
    limit: Optional[int] = None,
    ext_filter: Optional[str] = None,
    only_missing: bool = True,
):
    query = """
        SELECT f.*
        FROM files f
        JOIN projects p ON f.project_id = p.project_id
        LEFT JOIN file_ir ir
          ON f.file_id = ir.file_id
         AND ir.ir_type = 'script_ir_v1'
        WHERE f.file_type = 'code'
          AND f.file_ext IN ('.py', '.r', '.nf', '.cpp', '.cc', '.cxx', '.hpp', '.h')
    """
    params = []

    if only_missing:
        query += " AND ir.file_id IS NULL"

    if project_name:
        query += " AND p.project_name = ?"
        params.append(project_name)

    if ext_filter:
        query += " AND f.file_ext = ?"
        params.append(ext_filter.lower())

    query += " ORDER BY f.file_id ASC"

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with db_cursor() as (_, cur):
        cur.execute(query, tuple(params))
        return cur.fetchall()

def get_files_for_function_llm(
    project_name: Optional[str] = None,
    limit: Optional[int] = None,
    ext_filter: Optional[str] = None,
    only_missing: bool = True,
    model_name: Optional[str] = None,
):
    query = """
        SELECT f.*
        FROM files f
        JOIN projects p ON f.project_id = p.project_id
        JOIN file_ir ir
          ON f.file_id = ir.file_id
         AND ir.ir_type = 'script_ir_v1'
        WHERE f.file_type = 'code'
          AND COALESCE(f.is_duplicate, 0) = 0
          AND f.file_ext IN ('.py', '.r', '.nf', '.cpp', '.cc', '.cxx', '.hpp', '.h')
    """
    params = []

    if project_name:
        query += " AND p.project_name = ?"
        params.append(project_name)

    if ext_filter:
        query += " AND f.file_ext = ?"
        params.append(ext_filter.lower())

    if only_missing:
        if model_name:
            query += """
              AND NOT EXISTS (
                    SELECT 1
                    FROM function_metadata_llm l
                    WHERE l.file_id = f.file_id
                      AND l.model_name = ?
              )
            """
            params.append(model_name)
        else:
            query += """
              AND NOT EXISTS (
                    SELECT 1
                    FROM function_metadata_llm l
                    WHERE l.file_id = f.file_id
              )
            """

    query += " ORDER BY f.file_id ASC"

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with db_cursor() as (_, cur):
        cur.execute(query, tuple(params))
        return cur.fetchall()

def upsert_file_ir(file_id: int, ir_type: str, ir_obj: dict) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    ir_json = json.dumps(ir_obj, ensure_ascii=False, indent=2)

    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT ir_id FROM file_ir
            WHERE file_id = ? AND ir_type = ?
        """, (file_id, ir_type))
        row = cur.fetchone()

        if row:
            cur.execute("""
                UPDATE file_ir
                SET ir_json = ?, updated_at = ?
                WHERE ir_id = ?
            """, (ir_json, now, row["ir_id"]))
        else:
            cur.execute("""
                INSERT INTO file_ir (
                    file_id, ir_type, ir_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
            """, (file_id, ir_type, ir_json, now, now))

def get_file_ir(file_id: int, ir_type: str = "script_ir_v1"):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT * FROM file_ir
            WHERE file_id = ? AND ir_type = ?
            ORDER BY ir_id DESC
            LIMIT 1
        """, (file_id, ir_type))
        return cur.fetchone()

def upsert_function_metadata_llm(
    file_id: int,
    function_name: str,
    model_name: str,
    metadata: dict,
) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    payload = json.dumps(metadata, ensure_ascii=False, indent=2)

    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT llm_function_id
            FROM function_metadata_llm
            WHERE file_id = ? AND function_name = ? AND model_name = ?
        """, (file_id, function_name, model_name))
        row = cur.fetchone()

        if row:
            cur.execute("""
                UPDATE function_metadata_llm
                SET metadata_json = ?, updated_at = ?
                WHERE llm_function_id = ?
            """, (payload, now, row["llm_function_id"]))
        else:
            cur.execute("""
                INSERT INTO function_metadata_llm (
                    file_id, function_name, model_name, metadata_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (file_id, function_name, model_name, payload, now, now))

def get_file_ir(file_id: int, ir_type: str = "script_ir_v1"):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT *
            FROM file_ir
            WHERE file_id = ? AND ir_type = ?
            ORDER BY ir_id DESC
            LIMIT 1
        """, (file_id, ir_type))
        return cur.fetchone()


def upsert_function_metadata_llm(
    file_id: int,
    function_name: str,
    model_name: str,
    metadata: dict,
) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    payload = json.dumps(metadata, ensure_ascii=False, indent=2)

    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT llm_function_id
            FROM function_metadata_llm
            WHERE file_id = ? AND function_name = ? AND model_name = ?
        """, (file_id, function_name, model_name))
        row = cur.fetchone()

        if row:
            cur.execute("""
                UPDATE function_metadata_llm
                SET metadata_json = ?, updated_at = ?
                WHERE llm_function_id = ?
            """, (payload, now, row["llm_function_id"]))
        else:
            cur.execute("""
                INSERT INTO function_metadata_llm (
                    file_id, function_name, model_name, metadata_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (file_id, function_name, model_name, payload, now, now))


def get_function_metadata_llm_for_file(file_id: int, model_name: Optional[str] = None):
    query = """
        SELECT *
        FROM function_metadata_llm
        WHERE file_id = ?
    """
    params = [file_id]

    if model_name:
        query += " AND model_name = ?"
        params.append(model_name)

    query += " ORDER BY function_name ASC, llm_function_id DESC"

    with db_cursor() as (_, cur):
        cur.execute(query, tuple(params))
        return cur.fetchall()

def upsert_file_metadata_merged(
    file_id: int,
    merge_version: str,
    metadata: dict,
) -> None:
    from datetime import datetime
    now = datetime.utcnow().isoformat(timespec="seconds")
    payload = json.dumps(metadata, ensure_ascii=False, indent=2)

    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT merged_id
            FROM file_metadata_merged
            WHERE file_id = ? AND merge_version = ?
        """, (file_id, merge_version))
        row = cur.fetchone()

        if row:
            cur.execute("""
                UPDATE file_metadata_merged
                SET metadata_json = ?, updated_at = ?
                WHERE merged_id = ?
            """, (payload, now, row["merged_id"]))
        else:
            cur.execute("""
                INSERT INTO file_metadata_merged (
                    file_id, merge_version, metadata_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
            """, (file_id, merge_version, payload, now, now))


def get_latest_file_metadata(file_id: int):
    with db_cursor() as (_, cur):
        cur.execute("""
            SELECT *
            FROM file_metadata
            WHERE file_id = ?
            ORDER BY metadata_id DESC
            LIMIT 1
        """, (file_id,))
        return cur.fetchone()

def get_files_for_merge(project_name: Optional[str] = None, limit: Optional[int] = None):
    query = """
        SELECT f.*
        FROM files f
        JOIN projects p ON f.project_id = p.project_id
        WHERE COALESCE(f.is_duplicate, 0) = 0
          AND (
                f.file_ext IN (
                    '.py', '.r', '.nf', '.cpp', '.cc', '.cxx', '.hpp', '.h',
                    '.yaml', '.yml', '.sh', '.bash', '.txt'
                )
                OR f.file_name IN ('Dockerfile', 'nextflow.config')
          )
    """
    params = []

    if project_name:
        query += " AND p.project_name = ?"
        params.append(project_name)

    query += " ORDER BY f.file_id ASC"

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with db_cursor() as (_, cur):
        cur.execute(query, tuple(params))
        return cur.fetchall()

def get_latest_file_metadata_merged(file_id: int, merge_version: str | None = None):
    query = """
        SELECT *
        FROM file_metadata_merged
        WHERE file_id = ?
    """
    params = [file_id]

    if merge_version:
        query += " AND merge_version = ?"
        params.append(merge_version)

    query += " ORDER BY merged_id DESC LIMIT 1"

    with db_cursor() as (_, cur):
        cur.execute(query, tuple(params))
        return cur.fetchone()