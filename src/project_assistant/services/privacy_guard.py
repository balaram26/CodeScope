from __future__ import annotations

import re


SENSITIVE_TERMS = [
    "sample_id",
    "sample name",
    "sample_name",
    "subject_id",
    "participant_id",
    "sentrix",
    "sentrix_id",
    "sentrix_position",
    "sample_plate",
    "sample_well",
    "sample_group",
    "plate",
    "well",
    "barcode",
]


TABULAR_EXTENSIONS = (".csv", ".tsv", ".txt")


def chunk_is_sensitive(ev) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    text = (getattr(ev, "text", "") or "").lower()
    metadata = getattr(ev, "metadata", {}) or {}
    file_path = (getattr(ev, "file_path", "") or "").lower()
    item_type = str(getattr(ev, "item_type", "") or "").lower()
    chunk_type = str(metadata.get("chunk_type") or "").lower()

    if item_type == "project":
        if file_path.endswith(TABULAR_EXTENSIONS):
            reasons.append("project_tabular_file")

        if chunk_type == "dataset_summary":
            reasons.append("dataset_summary_chunk")

        for term in SENSITIVE_TERMS:
            if term in text:
                reasons.append(f"term:{term}")

        if re.search(r"(sample[_ ]?name|sample[_ ]?id|sentrix[_ ]?id)", text):
            reasons.append("sample_identifier_pattern")

    return (len(reasons) > 0, reasons)