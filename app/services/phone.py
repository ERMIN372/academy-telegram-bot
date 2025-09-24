from __future__ import annotations

import re

PHONE_RE = re.compile(r"7\d{10}")


def normalize(phone: str) -> str | None:
    digits = re.sub(r"\D", "", phone)
    if digits.startswith("8"):
        digits = "7" + digits[1:]
    if digits.startswith("7") and len(digits) == 11:
        return "+7" + digits[1:]
    if digits.startswith("9") and len(digits) == 10:
        return "+7" + digits
    return None
