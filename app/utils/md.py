from __future__ import annotations

from html import escape
from typing import Any, Iterable


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def bold(text: Any) -> str:
    value = safe_text(text)
    return f"<b>{escape(value)}</b>"


def italic(text: Any) -> str:
    value = safe_text(text)
    return f"<i>{escape(value)}</i>"


def format_list(items: Iterable[Any]) -> str:
    return "\n".join(f"• {escape(safe_text(item))}" for item in items)
