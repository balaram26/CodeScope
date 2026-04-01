from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from project_assistant.ai.llm_service import LLMService


class AssistantLLMAdapter:
    """
    Final-answer adapter for local or OpenAI-backed generation.
    """

    def __init__(
        self,
        *,
        provider: str | None = None,
        local_config_path: str | Path | None = None,
        local_task_name: str = "assistant_answer",
        local_fallback_tasks: list[str] | None = None,
        openai_model: str = "gpt-5.4-mini",
        openai_api_key_env: str = "OPENAI_API_KEY",
        default_max_tokens: int = 500,
    ):
        self.provider = provider or os.environ.get("PROJECT_ASSISTANT_LLM_PROVIDER", "local")

        env_cfg = os.environ.get("PROJECT_ASSISTANT_MODELS_YAML")
        self.local_config_path = (
            Path(local_config_path) if local_config_path is not None
            else Path(env_cfg) if env_cfg
            else None
        )

        self.local_task_name = local_task_name
        self.local_fallback_tasks = local_fallback_tasks or [
            "research_assistant",
            "project_readme_synthesis",
            "function_metadata_extract",
        ]
        self.openai_model = openai_model
        self.openai_api_key_env = openai_api_key_env
        self.default_max_tokens = default_max_tokens

        self._local_service: LLMService | None = None
        self._openai_client: Any | None = None

    def _get_local_service(self) -> LLMService:
        if self.local_config_path is None:
            raise RuntimeError(
                "Local LLM provider selected, but no model config was provided. "
                "Set PROJECT_ASSISTANT_MODELS_YAML or pass local_config_path explicitly."
            )
        if self._local_service is None:
            self._local_service = LLMService.from_yaml(self.local_config_path)
        return self._local_service

    def _resolve_local_task(self) -> str:
        service = self._get_local_service()
        tasks = set(service.list_tasks())

        if self.local_task_name in tasks:
            return self.local_task_name

        for task in self.local_fallback_tasks:
            if task in tasks:
                return task

        raise KeyError(
            f"No usable local LLM task found. Requested '{self.local_task_name}', "
            f"fallbacks {self.local_fallback_tasks}, available: {sorted(tasks)}"
        )

    def generate_local(self, prompt: str, max_tokens: int | None = None) -> str:
        service = self._get_local_service()
        task = self._resolve_local_task()
        return service.complete(
            task=task,
            prompt=prompt,
            max_tokens=max_tokens or self.default_max_tokens,
        )

    def _get_openai_client(self):
        if self._openai_client is None:
            api_key = os.environ.get(self.openai_api_key_env)
            if not api_key:
                raise RuntimeError(
                    f"OpenAI provider selected, but env var '{self.openai_api_key_env}' is not set."
                )
            try:
                from openai import OpenAI
            except Exception as exc:
                raise RuntimeError(
                    "OpenAI package is not installed in the active environment."
                ) from exc

            self._openai_client = OpenAI(api_key=api_key)

        return self._openai_client

    def _generate_openai(self, prompt: str, max_tokens: int | None = None) -> str:
        client = self._get_openai_client()

        response = client.responses.create(
            model=self.openai_model,
            input=prompt,
            max_output_tokens=max_tokens or self.default_max_tokens,
        )

        text = getattr(response, "output_text", None)
        if text:
            return text

        parts: list[str] = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                txt = getattr(content, "text", None)
                if txt:
                    parts.append(txt)
        return "\n".join(parts).strip()

    def generate(self, prompt: str, max_tokens: int | None = None) -> str:
        if self.provider == "local":
            return self.generate_local(prompt, max_tokens=max_tokens)
        if self.provider == "openai":
            return self._generate_openai(prompt, max_tokens=max_tokens)
        raise ValueError(f"Unsupported LLM provider: {self.provider}")

    def warmup(self) -> None:
        if self.provider == "local":
            _ = self.generate_local("Reply with exactly: OK", max_tokens=16)

    def describe(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "local_config_path": str(self.local_config_path) if self.local_config_path else None,
            "local_task_name": self.local_task_name,
            "openai_model": self.openai_model,
            "default_max_tokens": self.default_max_tokens,
        }
