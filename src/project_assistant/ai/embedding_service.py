from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np

try:
    from sentence_transformers import SentenceTransformer
except ImportError as e:
    raise SystemExit("Please pip install sentence-transformers") from e


class EmbeddingService:
    """Simple cached sentence-transformers wrapper."""

    _cache: Dict[str, SentenceTransformer] = {}

    def __init__(self, model_name: str = "BAAI/bge-base-en-v1.5"):
        self.model_name = model_name

    def get_model(self) -> SentenceTransformer:
        if self.model_name not in self._cache:
            self._cache[self.model_name] = SentenceTransformer(self.model_name)
        return self._cache[self.model_name]

    def embed_texts(
        self,
        texts: List[str],
        *,
        normalize_embeddings: bool = True,
        convert_to_numpy: bool = True,
    ) -> np.ndarray:
        model = self.get_model()
        vecs = model.encode(
            texts,
            normalize_embeddings=normalize_embeddings,
            convert_to_numpy=convert_to_numpy,
            show_progress_bar=False,
        )
        return np.asarray(vecs, dtype=np.float32)
