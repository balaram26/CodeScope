from __future__ import annotations

from pathlib import Path

from pathlib import Path

from project_assistant.services.llm_adapter import AssistantLLMAdapter
from project_assistant.services.project_delete_service import ProjectDeleteService
from project_assistant.services.project_helper import ProjectHelper
from project_assistant.services.project_import_service import ProjectImportService


def build_services():
    llm_adapter = AssistantLLMAdapter()
    project_helper = ProjectHelper()

    models_yaml_path = Path(__file__).resolve().parents[3] / "models" / "models.yaml"

    project_import_service = ProjectImportService(
        project_helper=project_helper,
        models_yaml_path=models_yaml_path,
    )
    project_delete_service = ProjectDeleteService()

    return {
        "llm_adapter": llm_adapter,
        "project_helper": project_helper,
        "project_import_service": project_import_service,
        "project_delete_service": project_delete_service,
    }

