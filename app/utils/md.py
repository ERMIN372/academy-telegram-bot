from __future__ import annotations

from html import escape
from typing import Iterable


def bold(text: str) -> str:
    return f"<b>{escape(text)}</b>"


def italic(text: str) -> str:
    return f"<i>{escape(text)}</i>"


def format_list(items: Iterable[str]) -> str:
    return "\n".join(f"• {escape(item)}" for item in items)
