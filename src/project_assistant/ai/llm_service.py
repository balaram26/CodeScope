from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from .model_manager import ModelManager

from .config import load_yaml_config
from .json_utils import safe_parse_json


@dataclass(frozen=True)
class TaskSpec:
    name: str
    model: str
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    repeat_penalty: Optional[float] = None


class LLMService:
    """Shared wrapper around ModelManager for all local GGUF generation tasks."""

    def __init__(self, model_manager: ModelManager, task_specs: Dict[str, TaskSpec] | None = None):
        self.model_manager = model_manager
        self.task_specs = task_specs or {}
        self.tasks = self.task_specs

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> "LLMService":
        cfg = load_yaml_config(config_path)
        task_specs: Dict[str, TaskSpec] = {}
        for task_name, spec in (cfg.get("tasks") or {}).items():
            if not isinstance(spec, dict):
                continue
            model_name = spec.get("model")
            if not model_name:
                continue
            task_specs[task_name] = TaskSpec(
                name=task_name,
                model=model_name,
                max_tokens=spec.get("max_tokens"),
                temperature=spec.get("temperature"),
                top_p=spec.get("top_p"),
                repeat_penalty=spec.get("repeat_penalty"),
            )
        return cls(model_manager=ModelManager(str(config_path)), task_specs=task_specs)

    def list_tasks(self) -> list[str]:
        return sorted(self.task_specs.keys())

    def resolve_model_for_task(self, task: str) -> str:
        if task not in self.task_specs:
            raise KeyError(f"Unknown task '{task}'. Available tasks: {self.list_tasks()}")
        return self.task_specs[task].model

    def complete(
        self,
        task: str,
        prompt: str,
        *,
        max_tokens: Optional[int] = None,
    ) -> str:
        spec = self.task_specs.get(task)
        if spec is None:
            raise KeyError(f"Unknown task '{task}'. Available tasks: {self.list_tasks()}")
        return self.complete_with_model(
            model_name=spec.model,
            prompt=prompt,
            max_tokens=(max_tokens if max_tokens is not None else spec.max_tokens or 600),
        )

    def complete_with_model(
        self,
        model_name: str,
        prompt: str,
        *,
        max_tokens: int = 600,
    ) -> str:
        return self.model_manager.generate(model_name, prompt, max_tokens=max_tokens)

    def extract_json(
        self,
        task: str,
        prompt: str,
        *,
        max_tokens: Optional[int] = None,
    ) -> Dict[str, Any]:
        text = self.complete(task=task, prompt=prompt, max_tokens=max_tokens)
        ok, parsed, error = safe_parse_json(text)
        return {
            "ok_json": ok,
            "parsed": parsed,
            "error": error,
            "output_text": text,
        }

    def extract_json_with_model(
        self,
        model_name: str,
        prompt: str,
        *,
        max_tokens: int = 600,
    ) -> Dict[str, Any]:
        text = self.complete_with_model(model_name=model_name, prompt=prompt, max_tokens=max_tokens)
        ok, parsed, error = safe_parse_json(text)
        return {
            "ok_json": ok,
            "parsed": parsed,
            "error": error,
            "output_text": text,
        }
