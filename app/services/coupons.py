from __future__ import annotations

import datetime as dt
from typing import Dict, Optional

from app.services import sheets

COUPONS_SHEET = "coupons"


async def find_first_free_coupon(campaign: str | None) -> Optional[Dict[str, str]]:
    records = await sheets.read(COUPONS_SHEET)
    for record in records:
        if campaign and record.get("campaign") and record.get("campaign") != campaign:
            continue
        status = (record.get("status") or "").lower()
        if status not in {"reserved", "issued"}:
            return {"row": record["row"], "code": record.get("code"), "campaign": record.get("campaign")}
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
        if str(record.get("reserved_by")) != str(user_id):
            continue
        record_campaign = record.get("campaign")
        if campaign and record_campaign and record_campaign != campaign:
            continue
        return {"row": record["row"], "code": record.get("code"), "campaign": record_campaign}
    return None
