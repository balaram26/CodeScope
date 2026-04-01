import re


def unique_keep_order(items):
    seen = set()
    out = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def clean_path_candidate(s: str) -> str | None:
    s = s.strip()
    if not s:
        return None
    if len(s) > 220:
        return None
    if "\n" in s or "\r" in s:
        return None
    if s.count(" ") > 6 and "/" not in s and "\\" not in s:
        return None
    return s


def extract_string_path_candidates(text: str) -> list[str]:
    pattern = r"""["']([^"']+\.(?:csv|tsv|txt|json|yaml|yml|rds|rdata|RData|pdf|png|jpg|jpeg|svg|tiff|xlsx|xls|npz|npy|pkl|pickle|pt|pth|joblib|vcf|gz))["']"""
    vals = [clean_path_candidate(m.group(1)) for m in re.finditer(pattern, text, flags=re.IGNORECASE)]
    return unique_keep_order([v for v in vals if v])
