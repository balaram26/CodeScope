from pathlib import Path

from project_assistant.indexer.config import (
    CODE_EXTENSIONS,
    DOC_EXTENSIONS,
    REPORT_EXTENSIONS,
    RESULT_EXTENSIONS,
)


def classify_file(file_path: Path) -> str:
    ext = file_path.suffix.lower()

    if ext in CODE_EXTENSIONS:
        return "code"
    if ext in DOC_EXTENSIONS:
        return "doc"
    if ext in RESULT_EXTENSIONS:
        return "result"
    if ext in REPORT_EXTENSIONS:
        return "report"

    return "other"