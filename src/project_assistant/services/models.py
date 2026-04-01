from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ProjectRef:
    project_name: str
    source_path: Optional[str] = None
    status: str = "ready"
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryRequest:
    question: str
    project_names: list[str]
    mode: str = "ask"
    top_k: int = 8
    include_evidence: bool = True


@dataclass
class EvidenceChunk:
    project_name: str
    file_path: Optional[str]
    chunk_id: Optional[str]
    score: Optional[float]
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResponse:
    answer: str
    evidence: list[EvidenceChunk] = field(default_factory=list)
    used_projects: list[str] = field(default_factory=list)