from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any


def _lower(x: str | None) -> str:
    return (x or "").strip().lower()


def _basename(path: str | None) -> str:
    return Path(path or "").name.lower()


def _stem(path: str | None) -> str:
    return Path(path or "").stem.lower()


def is_workflow_file(path: str | None) -> bool:
    return _lower(path).endswith(".nf")


def is_script_file(path: str | None) -> bool:
    p = _lower(path)
    return p.endswith(".r") or p.endswith(".py") or p.endswith(".sh")


def is_artifact_file(path: str | None) -> bool:
    p = _lower(path)
    return p.endswith((".csv", ".tsv", ".xlsx", ".xls", ".pdf", ".png", ".jpg", ".jpeg", ".txt"))


def group_evidence_by_file(evidence) -> dict[str, list]:
    grouped: dict[str, list] = defaultdict(list)
    for ev in evidence:
        if ev.file_path:
            grouped[ev.file_path].append(ev)
    return grouped


def build_script_dossier(file_path: str, file_chunks: list) -> str:
    texts = [c.text for c in file_chunks if c.text]
    merged = "\n\n".join(texts[:8])

    return (
        f"SCRIPT DOSSIER\n"
        f"File: {file_path}\n"
        f"Type: script\n"
        f"Evidence from this script:\n{merged[:5000]}"
    )


def build_workflow_dossier(file_path: str, file_chunks: list) -> str:
    texts = [c.text for c in file_chunks if c.text]
    merged = "\n\n".join(texts[:8])

    return (
        f"WORKFLOW DOSSIER\n"
        f"File: {file_path}\n"
        f"Type: workflow\n"
        f"Evidence from this workflow:\n{merged[:5000]}"
    )


def score_artifact_link(script_path: str, artifact_path: str, script_text: str) -> int:
    score = 0
    a_base = _basename(artifact_path)
    a_stem = _stem(artifact_path)

    s = script_text.lower()

    if a_base and a_base in s:
        score += 5
    if a_stem and a_stem in s:
        score += 3

    # heuristic: output-ish words
    for token in ["output", "outfile", "out_path", "write", "save", "export", "plot"]:
        if token in s:
            score += 1

    return score


def link_artifacts_to_scripts(evidence) -> dict[str, list[str]]:
    grouped = group_evidence_by_file(evidence)

    script_files = [fp for fp in grouped if is_script_file(fp) or is_workflow_file(fp)]
    artifact_files = [fp for fp in grouped if is_artifact_file(fp)]

    script_texts = {
        fp: "\n\n".join(c.text for c in grouped[fp] if c.text)[:10000]
        for fp in script_files
    }

    links: dict[str, list[str]] = defaultdict(list)

    for artifact_fp in artifact_files:
        candidates = []
        for script_fp in script_files:
            score = score_artifact_link(script_fp, artifact_fp, script_texts[script_fp])
            if score > 0:
                candidates.append((score, script_fp))

        candidates.sort(reverse=True)
        best = [fp for _, fp in candidates[:3]]
        if best:
            links[artifact_fp] = best

    return links


def build_artifact_link_blocks(evidence) -> list[str]:
    grouped = group_evidence_by_file(evidence)
    links = link_artifacts_to_scripts(evidence)

    blocks: list[str] = []
    for artifact_fp, producers in links.items():
        artifact_chunks = grouped.get(artifact_fp, [])
        artifact_text = "\n\n".join(c.text for c in artifact_chunks if c.text)[:2500]
        block = (
            f"ARTIFACT LINK SUMMARY\n"
            f"Artifact: {artifact_fp}\n"
            f"Likely generating scripts/workflows:\n"
            + "\n".join(f"- {p}" for p in producers)
            + "\n"
        )
        if artifact_text:
            block += f"Artifact evidence:\n{artifact_text}"
        blocks.append(block)

    return blocks


def expand_context(evidence) -> dict[str, list[str]]:
    grouped = group_evidence_by_file(evidence)

    workflow_blocks: list[str] = []
    script_blocks: list[str] = []

    for file_path, chunks in grouped.items():
        if is_workflow_file(file_path):
            workflow_blocks.append(build_workflow_dossier(file_path, chunks))
        elif is_script_file(file_path):
            script_blocks.append(build_script_dossier(file_path, chunks))

    artifact_blocks = build_artifact_link_blocks(evidence)

    return {
        "workflow_dossiers": workflow_blocks[:3],
        "script_dossiers": script_blocks[:4],
        "artifact_links": artifact_blocks[:4],
    }