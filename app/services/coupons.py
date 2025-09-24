from __future__ import annotations

import datetime as dt
from typing import Dict, Optional

from app.services import sheets

COUPONS_SHEET = "coupons"


async def find_first_free_coupon(campaign: str | None) -> Optional[Dict[str, str]]:
    records = await sheets.read(COUPONS_SHEET)
    for record in records:
        record_campaign = (record.get("campaign") or "").strip()
        if campaign and record_campaign and record_campaign != campaign:
            continue
        status = (record.get("status") or "").strip().lower()
        if status not in {"", "free"}:
            continue
        code = (record.get("code") or "").strip()
        if not code:
            continue
        return {"row": record["row"], "code": code, "campaign": record_campaign}
    return None


async def reserve_coupon(row: int, user_id: int, code: str) -> Dict[str, str]:
    data = {
        "code": code,
        "status": "reserved",
        "reserved_by": str(user_id),
        "reserved_at": dt.datetime.utcnow().isoformat(),
    }
    await sheets.update_row(COUPONS_SHEET, row, data)
    return {"code": code, "row": row}


async def get_user_coupon(user_id: int, campaign: str | None = None) -> Optional[Dict[str, str]]:
    records = await sheets.read(COUPONS_SHEET)
    for record in records:
        reserved_by = str(record.get("reserved_by") or "").strip()
        if reserved_by != str(user_id):
            continue
        record_campaign = (record.get("campaign") or "").strip()
        if campaign and record_campaign and record_campaign != campaign:
            continue
        code = (record.get("code") or "").strip()
        if not code:
            continue
        return {"row": record["row"], "code": code, "campaign": record_campaign}
    return None
