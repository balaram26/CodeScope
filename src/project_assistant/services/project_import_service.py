from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from project_assistant.services.project_helper import ProjectHelper


class ProjectImportService:
    def __init__(
        self,
        project_helper: ProjectHelper,
        models_yaml_path: str | Path | None = None,
    ):
        self.project_helper = project_helper
        self.models_yaml_path = (
            str(models_yaml_path)
            if models_yaml_path is not None
            else os.environ.get("PROJECT_ASSISTANT_MODELS_YAML")
        )

    def _run_digest(
        self,
        backend_project_name: str,
        file_ids: list[int] | None = None,
        rebuild_docs: bool = False,
    ) -> None:
        cmd = [
            sys.executable,
            "-m",
            "project_assistant.indexer.pipelines.run_project_digest",
            "--project-name",
            backend_project_name,
            "--task-name",
            "function_metadata_extract",
            "--model-label",
            "local_llm_v1",
            "--max-tokens",
            "1200",
            "--embedding-model",
            "BAAI/bge-base-en-v1.5",
            "--from-stage",
            "parse",
            "--to-stage",
            "index",
        ]

        if self.models_yaml_path:
            cmd.extend(["--config-path", self.models_yaml_path])

        if file_ids:
            cmd.extend(["--file-ids", ",".join(str(x) for x in file_ids)])

        if rebuild_docs:
            cmd.append("--rebuild-docs")

        subprocess.run(cmd, check=True)

    def import_project(
        self,
        project_name: str,
        source_path: str,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> dict[str, Any]:
        backend_payload = self.project_helper.start_import(
            project_name=project_name,
            source_path=source_path,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )

        backend_project_name = backend_payload["source_system_id"]

        self._run_digest(backend_project_name)

        return {
            "project_name": project_name.strip(),
            "backend_project_name": backend_project_name,
            "backend_payload": backend_payload,
            "status": "ready",
        }

    def update_project(
        self,
        source_system_id: str,
        source_path: str,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        merge_mode: str = "overwrite_existing",
    ) -> dict[str, Any]:
        backend_payload = self.project_helper.update_project_source(
            source_system_id=source_system_id,
            source_path=source_path,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            merge_mode=merge_mode,
        )

        affected_file_ids = backend_payload.get("affected_file_ids", [])

        self._run_digest(
            source_system_id,
            file_ids=affected_file_ids,
            rebuild_docs=False,
        )

        return {
            "backend_project_name": source_system_id,
            "backend_payload": backend_payload,
            "status": "ready",
        }