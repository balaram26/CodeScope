import json
import re
from pathlib import Path
from typing import Optional

from project_assistant.indexer.config import BUNDLE_MANIFEST_NAME, VALID_SOURCE_KINDS


def slugify_project_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s-]+", "_", name)
    return name.strip("_")


def load_bundle_manifest(bundle_dir: Path) -> dict:
    manifest_path = bundle_dir / BUNDLE_MANIFEST_NAME
    if not manifest_path.exists():
        return {}

    with open(manifest_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Manifest must be a JSON object: {manifest_path}")

    return data


def resolve_project_name(
    bundle_dir: Path,
    explicit_project_name: Optional[str] = None,
) -> str:
    if explicit_project_name:
        return slugify_project_name(explicit_project_name)

    manifest = load_bundle_manifest(bundle_dir)
    manifest_project_name = manifest.get("project_name")
    if manifest_project_name:
        return slugify_project_name(manifest_project_name)

    return slugify_project_name(bundle_dir.name)


def resolve_source_kind(
    bundle_dir: Path,
    explicit_source_kind: Optional[str] = None,
) -> str:
    if explicit_source_kind:
        kind = explicit_source_kind.strip().lower()
        if kind not in VALID_SOURCE_KINDS:
            raise ValueError(f"Invalid source kind: {kind}")
        return kind

    manifest = load_bundle_manifest(bundle_dir)
    manifest_kind = str(manifest.get("source_kind", "mixed")).strip().lower()
    if manifest_kind not in VALID_SOURCE_KINDS:
        return "mixed"

    return manifest_kind


def resolve_bundle_name(
    bundle_dir: Path,
    explicit_bundle_name: Optional[str] = None,
) -> str:
    if explicit_bundle_name:
        return explicit_bundle_name.strip()
    manifest = load_bundle_manifest(bundle_dir)
    return str(manifest.get("bundle_name", bundle_dir.name)).strip()