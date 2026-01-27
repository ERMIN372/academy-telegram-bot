from __future__ import annotations

import datetime as dt
import random
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from app.config import get_settings
from app.storage import db

SESSION_TTL_MINUTES = 10


@dataclass
class LotteryConfig:
    enabled: bool
    variants: int
    weights: List[float]
    results: List[str]
    coupon_map: Dict[str, str]
    cooldown_days: int
    title: str
    button_emoji: str
    button_label: str
    draw_prefix: str
    ab_test_enabled: bool


def _normalized_weights(results: List[str], weights: List[float]) -> List[float]:
    if not results:
        return []
    if not weights:
        return [1.0] * len(results)
    normalized: List[float] = []
    for index in range(len(results)):
        if index < len(weights):
            normalized.append(max(float(weights[index]), 0.0))
        else:
            normalized.append(max(float(weights[-1]), 0.0))
    if sum(normalized) <= 0:
        return [1.0] * len(results)
    return normalized


def get_config() -> LotteryConfig:
    settings = get_settings()
    return LotteryConfig(
        enabled=settings.lottery_enabled,
        variants=settings.lottery_variants or len(settings.lottery_results),
        weights=_normalized_weights(settings.lottery_results, settings.lottery_weights),
        results=settings.lottery_results,
        coupon_map=settings.lottery_coupon_campaign_map,
        cooldown_days=settings.lottery_cooldown_days,
        title=settings.lottery_title,
        button_emoji=settings.lottery_button_emoji or "🎯",
        button_label=settings.lottery_button_label or "🎲 Лотерея",
        draw_prefix=settings.draw_prefix or "draw_",
        ab_test_enabled=settings.lottery_ab_test,
    )


def get_user_bucket(user_id: int) -> str:
    return "A" if user_id % 2 == 0 else "B"


def should_show_button(user_id: int) -> Tuple[bool, str | None]:
    config = get_config()
    if not config.ab_test_enabled:
        return True, None
    bucket = get_user_bucket(user_id)
    return bucket == "A", bucket


def choose_result(config: LotteryConfig) -> Tuple[str, int, float]:
    if not config.results:
        raise RuntimeError("LOTTERY_RESULTS is not configured")
    weights = _normalized_weights(config.results, config.weights)
    total = sum(weights)
    if total <= 0:
        index = random.randrange(len(config.results))
        return config.results[index], index, 0.0
    rnd = random.uniform(0, total)
    upto = 0.0
    for index, (result, weight) in enumerate(zip(config.results, weights)):
        upto += weight
        if rnd <= upto:
            return result, index, weight
    last_index = len(config.results) - 1
    return config.results[last_index], last_index, weights[last_index] if weights else 0.0


def weight_share(config: LotteryConfig, weight: float) -> float:
    total = sum(config.weights)
    if total <= 0:
        return 0.0
    return weight / total


@dataclass
class LotterySession:
    session_id: str
    user_id: int
    campaign: str
    created_at: dt.datetime
    expires_at: dt.datetime
    status: str
    variant_index: Optional[int]
    result: Optional[str]
    coupon_campaign: Optional[str]

    @property
    def is_active(self) -> bool:
        return self.status == "active" and dt.datetime.utcnow() <= self.expires_at


@dataclass
class LotteryDraw:
    user_id: int
    campaign: str
    result: str
    coupon_campaign: Optional[str]
    variant_index: int
    drawn_at: dt.datetime
    session_id: Optional[str]
    claimed_at: Optional[dt.datetime]

    @property
    def is_claimed(self) -> bool:
        return self.claimed_at is not None


def _parse_datetime(value: str | None) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


async def create_session(user_id: int, campaign: str) -> LotterySession:
    now = dt.datetime.utcnow()
    expires_at = now + dt.timedelta(minutes=SESSION_TTL_MINUTES)
    session_id = uuid.uuid4().hex
    await db.create_lottery_session(session_id, user_id, campaign, now.isoformat(), expires_at.isoformat())
    return LotterySession(
        session_id=session_id,
        user_id=user_id,
        campaign=campaign,
        created_at=now,
        expires_at=expires_at,
        status="active",
        variant_index=None,
        result=None,
        coupon_campaign=None,
    )


async def get_session(session_id: str) -> LotterySession | None:
    record = await db.get_lottery_session(session_id)
    if not record:
        return None
    session = LotterySession(
        session_id=record["session_id"],
        user_id=int(record["user_id"]),
        campaign=str(record["campaign"]),
        created_at=_parse_datetime(record["created_at"]) or dt.datetime.utcnow(),
        expires_at=_parse_datetime(record["expires_at"]) or dt.datetime.utcnow(),
        status=str(record.get("status") or ""),
        variant_index=(
            int(record["variant_index"])
            if record.get("variant_index") is not None
            else None
        ),
        result=str(record.get("result") or "") or None,
        coupon_campaign=str(record.get("coupon_campaign") or "") or None,
    )
    if session.status == "active" and dt.datetime.utcnow() > session.expires_at:
        await db.update_lottery_session(session.session_id, status="expired")
        session.status = "expired"
    return session


async def store_result(
    session: LotterySession,
    variant_index: int,
    result: str,
    coupon_campaign: Optional[str],
) -> LotteryDraw:
    coupon_campaign_value = coupon_campaign or ""
    await db.update_lottery_session(
        session.session_id,
        status="completed",
        variant_index=variant_index,
        result=result,
        coupon_campaign=coupon_campaign_value,
    )
    now = dt.datetime.utcnow()
    await db.upsert_lottery_draw(
        user_id=session.user_id,
        campaign=session.campaign,
        result=result,
        coupon_campaign=coupon_campaign_value or None,
        variant_index=variant_index,
        session_id=session.session_id,
        drawn_at=now.isoformat(),
    )
    return LotteryDraw(
        user_id=session.user_id,
        campaign=session.campaign,
        result=result,
        coupon_campaign=coupon_campaign or None,
        variant_index=variant_index,
        drawn_at=now,
        session_id=session.session_id,
        claimed_at=None,
    )


async def get_draw(user_id: int, campaign: str) -> LotteryDraw | None:
    record = await db.get_lottery_draw(user_id, campaign)
    if not record:
        return None
    drawn_at = _parse_datetime(record.get("drawn_at"))
    claimed_at = _parse_datetime(record.get("claimed_at"))
    return LotteryDraw(
        user_id=user_id,
        campaign=campaign,
        result=str(record.get("result") or ""),
        coupon_campaign=str(record.get("coupon_campaign") or "") or None,
        variant_index=int(record.get("variant_index") or 0),
        drawn_at=drawn_at or dt.datetime.utcnow(),
        session_id=str(record.get("session_id") or "") or None,
        claimed_at=claimed_at,
    )


async def has_any_draw(user_id: int) -> bool:
    return await db.has_any_lottery_draw(user_id)


async def mark_claimed(user_id: int, campaign: str) -> None:
    await db.mark_lottery_claimed(user_id, campaign, dt.datetime.utcnow().isoformat())


def is_cooldown_active(draw: LotteryDraw, cooldown_days: int) -> bool:
    if cooldown_days <= 0:
        return False
    return (dt.datetime.utcnow() - draw.drawn_at) < dt.timedelta(days=cooldown_days)
