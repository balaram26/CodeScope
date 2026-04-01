from __future__ import annotations

import yaml
from pathlib import Path


def load_yaml_config(path: str | Path) -> dict:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"YAML config not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError("Invalid YAML config: expected dictionary at root")

    return data