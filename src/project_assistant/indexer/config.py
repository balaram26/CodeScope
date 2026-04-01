from pathlib import Path
import os

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[3]

APP_HOME = Path(
    os.environ.get("PROJECT_ASSISTANT_HOME", Path.home() / ".project_assistant")
).expanduser().resolve()

DATA_ROOT = APP_HOME / "data"
PROJECTS_ROOT = DATA_ROOT / "projects"

INBOX_DIR = PROJECTS_ROOT / "inbox"
MANAGED_DIR = PROJECTS_ROOT / "managed"
DB_DIR = PROJECTS_ROOT / "db"
GENERATED_DIR = PROJECTS_ROOT / "generated"
LOG_DIR = PROJECTS_ROOT / "logs"
GLOBAL_INDEX_DIR = PROJECTS_ROOT / "global_index"
TMP_EXTRACT_DIR = PROJECTS_ROOT / "tmp_extract"

DB_PATH = DB_DIR / "projects.sqlite"

BUNDLE_MANIFEST_NAME = "bundle_manifest.json"

ARCHIVE_EXTENSIONS = {
    ".zip",
    ".tar",
    ".gz",
    ".tgz",
    ".tar.gz",
}

IGNORE_DIR_NAMES = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".nextflow",
    "work",
    "tmp",
    "temp",
    "node_modules",
    "venv",
    ".venv",
    "env",
}

IGNORE_FILE_NAMES = {
    ".DS_Store",
}

CODE_EXTENSIONS = {
    ".py", ".r", ".nf", ".sh", ".bash", ".zsh",
    ".cpp", ".c", ".h", ".hpp",
    ".java", ".kt", ".scala",
    ".js", ".ts",
    ".yaml", ".yml", ".json", ".toml", ".ini", ".cfg",
}

DOC_EXTENSIONS = {
    ".md", ".txt", ".rst"
}

RESULT_EXTENSIONS = {
    ".csv", ".tsv", ".xlsx", ".xls"
}

REPORT_EXTENSIONS = {
    ".html", ".htm", ".pdf"
}

MAX_HASH_FILE_SIZE_MB = 1024

SOURCE_ORIGIN_UPLOAD = "upload"

VALID_SOURCE_KINDS = {"code", "results", "docs", "notes", "mixed"}


def ensure_runtime_dirs() -> None:
    for path in [
        APP_HOME,
        DATA_ROOT,
        PROJECTS_ROOT,
        INBOX_DIR,
        MANAGED_DIR,
        DB_DIR,
        GENERATED_DIR,
        LOG_DIR,
        GLOBAL_INDEX_DIR,
        TMP_EXTRACT_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)