from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict

from app.services import sheets


async def log_event(user_id: int, campaign: str, step: str, meta: Dict[str, Any] | None = None) -> None:
    if meta is None:
        meta = {}
    data = {
        "ts": dt.datetime.utcnow().isoformat(),
        "user_id": user_id,
        "campaign": campaign,
        "step": step,
        "meta": json.dumps(meta, ensure_ascii=False),
    }
    await sheets.append("events", data)
