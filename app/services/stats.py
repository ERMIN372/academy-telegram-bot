from __future__ import annotations

import json
from typing import Any, Dict

from app.services import sheets


async def log_event(
    user_id: int,
    campaign: str,
    step: str,
    meta: Dict[str, Any] | None = None,
    *,
    username: str | None = None,
) -> None:
    meta_payload: Dict[str, Any] = {}
    if meta:
        meta_payload.update(meta)
    meta_payload.setdefault("user_id", user_id)
    meta_payload.setdefault("campaign", campaign or "default")
    if username:
        meta_payload.setdefault("username", username)
    timestamp = sheets.current_timestamp()
    data = {
        "ts": timestamp.utc_text,
        "ts_msk": timestamp.local_text,
        "user_id": user_id,
        "campaign": campaign or "default",
        "step": step,
        "meta_json": json.dumps(meta_payload, ensure_ascii=False),
    }
    await sheets.append(
        "events", data, optional_headers=["ts_msk"], meta=timestamp.meta
    )
