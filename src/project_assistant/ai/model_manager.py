from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional, Any
import yaml
from pathlib import Path

from llama_cpp import Llama


@dataclass
class ModelSpec:
    name: str
    path: str
    n_ctx: int = 8192
    n_threads: int = 24
    n_gpu_layers: int = 0
    temperature: float = 0.2
    top_p: float = 0.9
    repeat_penalty: float = 1.1
    seed: int = 42


class ModelManager:
    """
    Loads GGUF models lazily and caches them in memory.
    """
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        cfg = yaml.safe_load(self.config_path.read_text(encoding="utf-8"))
        self.specs: Dict[str, ModelSpec] = {}
        for name, s in cfg.get("models", {}).items():
            self.specs[name] = ModelSpec(
                name=name,
                path=s["path"],
                n_ctx=int(s.get("n_ctx", 8192)),
                n_threads=int(s.get("n_threads", 24)),
                n_gpu_layers=int(s.get("n_gpu_layers", 0)),
                temperature=float(s.get("temperature", 0.2)),
                top_p=float(s.get("top_p", 0.9)),
                repeat_penalty=float(s.get("repeat_penalty", 1.1)),
                seed=int(s.get("seed", 42)),
            )
        self._loaded: Dict[str, Llama] = {}

    def list_models(self):
        return list(self.specs.keys())

    def get(self, model_name: str) -> Llama:
        if model_name in self._loaded:
            return self._loaded[model_name]
        if model_name not in self.specs:
            raise KeyError(f"Unknown model '{model_name}'. Available: {self.list_models()}")
        spec = self.specs[model_name]
        if not Path(spec.path).exists():
            raise FileNotFoundError(f"GGUF not found for model '{model_name}': {spec.path}")

        llm = Llama(
            model_path=spec.path,
            n_ctx=spec.n_ctx,
            n_threads=spec.n_threads,
            n_gpu_layers=spec.n_gpu_layers,
            seed=spec.seed,
            verbose=False,
        )
        self._loaded[model_name] = llm
        return llm

    def generate(self, model_name: str, prompt: str, max_tokens: int = 600) -> str:
        spec = self.specs[model_name]
        llm = self.get(model_name)

        out = llm(
            prompt,
            max_tokens=max_tokens,
            temperature=spec.temperature,
            top_p=spec.top_p,
            repeat_penalty=spec.repeat_penalty,
            # stop=["</json>", "```"],  # help keep outputs bounded
            stop=["```", "<|endoftext|>"]
        )
        txt = out["choices"][0]["text"]
        if not txt.strip():
            out = llm(
                prompt,
                max_tokens=max_tokens,
                temperature=spec.temperature,
                top_p=spec.top_p,
                repeat_penalty=spec.repeat_penalty,
                stop=None,
            )
            txt = out["choices"][0]["text"]
        return txt
        # return out["choices"][0]["text"]