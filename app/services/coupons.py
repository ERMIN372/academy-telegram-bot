from __future__ import annotations

import datetime as dt
from typing import Dict, Optional

from app.services import sheets
from app.utils import safe_text

COUPONS_SHEET = "coupons"


async def find_first_free_coupon(campaign: str | None) -> Optional[Dict[str, str]]:
    campaign_filter = safe_text(campaign)
    records = await sheets.read(COUPONS_SHEET)
    for record in records:
        record_campaign = safe_text(record.get("campaign"))
        if campaign_filter and record_campaign and record_campaign != campaign_filter:
            continue
        status = safe_text(record.get("status")).lower()
        if status not in {"", "free"}:
            continue
        code = safe_text(record.get("code"))
        if not code:
            continue
        return {"row": record["row"], "code": code, "campaign": record_campaign}
    return None


async def reserve_coupon(row: int, user_id: int, code: str) -> Dict[str, str]:
    sanitized_code = safe_text(code)
    data = {
        "code": sanitized_code,
        "status": "reserved",
        "reserved_by": safe_text(user_id),
        "reserved_at": dt.datetime.utcnow().isoformat(),
    }
    await sheets.update_row(COUPONS_SHEET, row, data)
    return {"code": sanitized_code, "row": row}


async def get_user_coupon(user_id: int, campaign: str | None = None) -> Optional[Dict[str, str]]:
    campaign_filter = safe_text(campaign)
    records = await sheets.read(COUPONS_SHEET)
    for record in records:
        reserved_by = safe_text(record.get("reserved_by"))
        if reserved_by != safe_text(user_id):
            continue
        record_campaign = safe_text(record.get("campaign"))
        if campaign_filter and record_campaign and record_campaign != campaign_filter:
            continue
        code = safe_text(record.get("code"))
        if not code:
            continue
        return {
            "row": record["row"],
            "code": code,
            "campaign": record_campaign,
            "used_at": safe_text(record.get("used_at")),
            "status": safe_text(record.get("status")),
        }
    return None
