from __future__ import annotations

import shutil
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path

from project_assistant.indexer.config import TMP_EXTRACT_DIR


def ensure_tmp_extract_dir() -> None:
    TMP_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)


def is_archive(path: Path) -> bool:
    name = path.name.lower()
    return (
        name.endswith(".zip")
        or name.endswith(".tar")
        or name.endswith(".tar.gz")
        or name.endswith(".tgz")
    )


def build_extract_dir(input_path: Path) -> Path:
    ensure_tmp_extract_dir()
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    stem = input_path.name
    for suffix in [".tar.gz", ".tgz", ".zip", ".tar"]:
        if stem.lower().endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return TMP_EXTRACT_DIR / f"{stem}_{ts}"


def extract_archive(archive_path: Path) -> Path:
    extract_dir = build_extract_dir(archive_path)
    extract_dir.mkdir(parents=True, exist_ok=True)

    name = archive_path.name.lower()

    if name.endswith(".zip"):
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)
    elif name.endswith(".tar") or name.endswith(".tar.gz") or name.endswith(".tgz"):
        with tarfile.open(archive_path, "r:*") as tf:
            tf.extractall(extract_dir)
    else:
        raise ValueError(f"Unsupported archive format: {archive_path}")

    return extract_dir


def resolve_extracted_bundle_root(extract_dir: Path) -> Path:
    children = [p for p in extract_dir.iterdir() if p.name != "__MACOSX"]

    if len(children) == 1 and children[0].is_dir():
        return children[0]

    return extract_dir


def cleanup_extracted_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
