from __future__ import annotations

import subprocess
import sys


class ProjectDeleteService:
    def delete_project(self, project_name: str, mode: str = "hard") -> None:
        cmd = [
            sys.executable,
            "-m",
            "project_assistant.indexer.processors.delete_project",
            "--project-name",
            project_name,
            "--mode",
            mode,
        ]
        subprocess.run(cmd, check=True)
