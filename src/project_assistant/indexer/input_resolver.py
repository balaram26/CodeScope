from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from project_assistant.indexer.archive_utils import (
    cleanup_extracted_dir,
    extract_archive,
    is_archive,
    resolve_extracted_bundle_root,
)


@dataclass
class ResolvedInput:
    bundle_dir: Path
    cleanup_dir: Path | None = None


def resolve_input_path(input_path: Path) -> ResolvedInput:
    if input_path.is_dir():
        return ResolvedInput(bundle_dir=input_path, cleanup_dir=None)

    if input_path.is_file() and is_archive(input_path):
        extract_dir = extract_archive(input_path)
        bundle_root = resolve_extracted_bundle_root(extract_dir)
        return ResolvedInput(bundle_dir=bundle_root, cleanup_dir=extract_dir)

    raise ValueError(f"Input must be a directory or supported archive: {input_path}")


def cleanup_resolved_input(resolved: ResolvedInput) -> None:
    if resolved.cleanup_dir is not None:
        cleanup_extracted_dir(resolved.cleanup_dir)
