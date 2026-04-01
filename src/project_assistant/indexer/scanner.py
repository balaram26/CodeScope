from pathlib import Path
from typing import List

from project_assistant.indexer.config import INBOX_DIR


def ensure_inbox() -> None:
    INBOX_DIR.mkdir(parents=True, exist_ok=True)


def find_bundle_dirs(inbox_dir: Path | None = None) -> List[Path]:
    inbox = inbox_dir or INBOX_DIR
    ensure_inbox()

    bundles = [p for p in inbox.iterdir() if p.is_dir()]
    bundles.sort(key=lambda p: p.name.lower())
    return bundles

from project_assistant.indexer.config import INBOX_DIR
from project_assistant.indexer.archive_utils import is_archive


def ensure_inbox() -> None:
    INBOX_DIR.mkdir(parents=True, exist_ok=True)


def find_bundle_inputs(inbox_dir: Path | None = None) -> List[Path]:
    inbox = inbox_dir or INBOX_DIR
    ensure_inbox()

    items = []
    for p in inbox.iterdir():
        if p.is_dir():
            items.append(p)
        elif p.is_file() and is_archive(p):
            items.append(p)

    items.sort(key=lambda p: p.name.lower())
    return items