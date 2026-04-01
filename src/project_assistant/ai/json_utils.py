from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple


def extract_first_json_object(text: str) -> Optional[str]:
    """Extract the first balanced JSON object from text."""
    if not text:
        return None

    s = text.strip().replace("```json", "").replace("```", "").strip()
    start = s.find("{")
    if start == -1:
        return None

    depth = 0
    in_str = False
    esc = False

    for i in range(start, len(s)):
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return s[start : i + 1]

    return None


def safe_parse_json(text: str) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    try:
        js = extract_first_json_object(text)
        if js is None:
            return False, None, "No JSON object found"
        return True, json.loads(js), ""
    except Exception as e:
        return False, None, str(e)
