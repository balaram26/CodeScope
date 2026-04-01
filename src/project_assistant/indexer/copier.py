import shutil
from datetime import datetime
from pathlib import Path

from project_assistant.indexer.config import MANAGED_DIR


def ensure_managed_root() -> None:
    MANAGED_DIR.mkdir(parents=True, exist_ok=True)


def build_managed_bundle_dir(project_name: str, bundle_name: str) -> Path:
    ensure_managed_root()

    project_root = MANAGED_DIR / project_name
    sources_root = project_root / "sources"
    sources_root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    safe_bundle_name = bundle_name.replace(" ", "_")
    target_dir = sources_root / f"upload_{timestamp}_{safe_bundle_name}"
    return target_dir


def copy_bundle_to_managed(bundle_dir: Path, project_name: str, bundle_name: str) -> Path:
    target_dir = build_managed_bundle_dir(project_name, bundle_name)
    shutil.copytree(bundle_dir, target_dir)
    return target_dir