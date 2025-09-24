from __future__ import annotations

from typing import Optional


def parse_start_payload(text: Optional[str], default: str = "default") -> str:
    if not text:
        return default
    parts = text.strip().split(maxsplit=1)
    if len(parts) == 1:
        return default
    payload = parts[1].strip()
    return payload or default
