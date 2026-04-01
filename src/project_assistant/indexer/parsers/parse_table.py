import csv
from pathlib import Path


def _safe_read_lines(file_path: Path, max_lines: int = 20) -> list[str]:
    lines = []
    with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        for i, line in enumerate(f):
            lines.append(line)
            if i + 1 >= max_lines:
                break
    return lines


def _detect_delimiter(sample_lines: list[str]) -> str:
    sample_text = "".join(sample_lines)

    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=[",", ";", "\t"])
        return dialect.delimiter
    except Exception:
        pass

    counts = {
        ",": sum(line.count(",") for line in sample_lines),
        ";": sum(line.count(";") for line in sample_lines),
        "\t": sum(line.count("\t") for line in sample_lines),
    }

    best = max(counts, key=counts.get)
    if counts[best] == 0:
        return ","
    return best


def parse_delimited_table(file_path: Path, delimiter: str) -> dict:
    row_count = 0
    columns = []
    preview_rows = []

    with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for i, row in enumerate(reader):
            if i == 0:
                columns = row
            else:
                row_count += 1
                if len(preview_rows) < 5:
                    preview_rows.append(row[:20])

    return {
        "table_format": "csv" if delimiter == "," else "tsv" if delimiter == "\t" else "delimited",
        "delimiter": delimiter,
        "column_count": len(columns),
        "row_count": row_count,
        "columns": columns[:500],
        "preview_rows": preview_rows,
    }


def parse_table_file(file_path: Path) -> dict:
    suffix = file_path.suffix.lower()

    if suffix in {".csv", ".tsv"}:
        sample_lines = _safe_read_lines(file_path, max_lines=20)
        delimiter = "\t" if suffix == ".tsv" else _detect_delimiter(sample_lines)
        return parse_delimited_table(file_path, delimiter)

    return {
        "table_format": suffix.lstrip("."),
        "note": "Parsing for this table type not yet implemented"
    }