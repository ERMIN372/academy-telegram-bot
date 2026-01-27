from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass
from html import escape
from typing import Dict, Optional

from aiogram import Bot
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter, TelegramAPIError
from zoneinfo import ZoneInfo

from app.config import get_settings, is_admin_user
from app.keyboards.common import kb_after_coupon
from app.services import coupons, stats
from app.utils import safe_text
from app.storage import db

logger = logging.getLogger(__name__)


def _ensure_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _parse_datetime(value: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        parsed = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    else:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        else:
            parsed = parsed.astimezone(dt.timezone.utc)
    return parsed


@dataclass(frozen=True)
class ReminderKey:
    user_id: int
    campaign: str


class ReminderScheduler:
    def __init__(self) -> None:
        self._bot: Bot | None = None
        self._tasks: Dict[ReminderKey, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()
        self._started = False

    async def start(self, bot: Bot) -> None:
        if self._started:
            return
        self._bot = bot
        self._started = True
        pending = await db.fetch_pending_reminders()
        for item in pending:
            campaign_text = safe_text(item.get("campaign")) or "default"
            await self._ensure_task(ReminderKey(item["user_id"], campaign_text))
        if pending:
            logger.info("Reminder scheduler restored %d pending reminders", len(pending))
        else:
            logger.info("Reminder scheduler started with no pending reminders")

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        for task in list(self._tasks.values()):
            task.cancel()
        self._tasks.clear()
        self._bot = None
        logger.info("Reminder scheduler stopped")

    async def schedule(self, user_id: int, campaign: str, code: Optional[str]) -> None:
        settings = get_settings()
        campaign_text = safe_text(campaign) or "default"
        code_text = safe_text(code)

        if settings.reminder_max_per_user == 0:
            await stats.log_event(
                user_id,
                campaign_text,
                "reminder_skipped",
                {"reason": "limit_disabled"},
            )
            return

        if not settings.reminder_enabled:
            await stats.log_event(
                user_id,
                campaign_text,
                "reminder_skipped",
                {"reason": "disabled"},
            )
            return

        if settings.reminder_only_if_no_lead and await db.has_lead(user_id, campaign_text):
            await stats.log_event(
                user_id,
                campaign_text,
                "reminder_skipped",
                {"reason": "lead_exists"},
            )
            return

        coupon_info = None
        if settings.reminder_only_if_not_used or not code_text:
            coupon_info = await coupons.get_user_coupon(user_id, campaign_text)
        if coupon_info:
            code_text = code_text or safe_text(coupon_info.get("code"))
        if not code_text:
            await stats.log_event(
                user_id,
                campaign_text,
                "reminder_skipped",
                {"reason": "no_code"},
            )
            return

        if settings.reminder_only_if_not_used:
            used_at = ""
            if coupon_info is None:
                coupon_info = await coupons.get_user_coupon(user_id, campaign_text)
            if coupon_info:
                used_at = safe_text(coupon_info.get("used_at"))
            if used_at:
                await stats.log_event(
                    user_id,
                    campaign_text,
                    "reminder_skipped",
                    {"reason": "coupon_used", "used_at": used_at},
                )
                return

        existing = await db.get_reminder(user_id, campaign_text)
        if existing:
            attempts = int(existing.get("attempts") or 0)
            if attempts >= settings.reminder_max_per_user:
                await stats.log_event(
                    user_id,
                    campaign_text,
                    "reminder_skipped",
                    {"reason": "attempt_limit", "attempts": attempts},
                )
                return
            status = safe_text(existing.get("status")).lower()
            if status == "scheduled":
                await stats.log_event(
                    user_id,
                    campaign_text,
                    "reminder_skipped",
                    {"reason": "already_scheduled", "attempts": attempts},
                )
                return
            if status in {"sent", "cancelled"}:
                await stats.log_event(
                    user_id,
                    campaign_text,
                    "reminder_skipped",
                    {"reason": f"already_{status}", "attempts": attempts},
                )
                return
            attempts = attempts + 1
        else:
            attempts = 1

        base_time = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc) + dt.timedelta(
            hours=settings.reminder_delay_hours
        )
        scheduled_at = self._adjust_to_work_hours(base_time)

        await db.upsert_reminder(
            user_id,
            campaign_text,
            code_text,
            scheduled_at.isoformat(),
            attempts,
            status="scheduled",
            reason="",
        )
        await stats.log_event(
            user_id,
            campaign_text,
            "reminder_scheduled",
            {
                "scheduled_at": scheduled_at.isoformat(),
                "code": code_text,
                "attempt": attempts,
            },
        )

        if self._started and self._bot:
            await self._ensure_task(ReminderKey(user_id, campaign_text))

    async def cancel(self, user_id: int, campaign: str, reason: str) -> bool:
        campaign_text = safe_text(campaign) or "default"
        reason_text = safe_text(reason)
        reminder = await db.get_reminder(user_id, campaign_text)
        if not reminder or safe_text(reminder.get("status")).lower() != "scheduled":
            return False
        cancelled_at = dt.datetime.utcnow().isoformat()
        await db.update_reminder(
            user_id,
            campaign_text,
            status="cancelled",
            reason=reason_text,
            cancelled_at=cancelled_at,
        )
        await stats.log_event(
            user_id,
            campaign_text,
            "reminder_cancelled",
            {"reason": reason_text, "cancelled_at": cancelled_at},
        )
        key = ReminderKey(user_id, campaign_text)
        async with self._lock:
            task = self._tasks.pop(key, None)
            if task:
                task.cancel()
        return True

    async def _ensure_task(self, key: ReminderKey) -> None:
        async with self._lock:
            task = self._tasks.get(key)
            if task and not task.done():
                return
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._run_reminder(key))
            self._tasks[key] = task
            task.add_done_callback(lambda t, *, key=key: self._tasks.pop(key, None))

    def _adjust_to_work_hours(self, target: dt.datetime) -> dt.datetime:
        settings = get_settings()
        try:
            tz = ZoneInfo(settings.reminder_timezone)
        except Exception:
            logger.warning("Unknown timezone %s, falling back to UTC", settings.reminder_timezone)
            tz = ZoneInfo("UTC")
        target_utc = _ensure_utc(target)
        local_time = target_utc.astimezone(tz)
        start_hour, end_hour = settings.reminder_work_hours
        start_local = local_time.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        end_local = local_time.replace(hour=end_hour, minute=0, second=0, microsecond=0)

        if local_time < start_local:
            return start_local.astimezone(dt.timezone.utc)
        if local_time >= end_local:
            next_day = start_local + dt.timedelta(days=1)
            return next_day.astimezone(dt.timezone.utc)
        return target_utc

    async def _run_reminder(self, key: ReminderKey) -> None:
        try:
            while self._started:
                reminder = await db.get_reminder(key.user_id, key.campaign)
                if not reminder or safe_text(reminder.get("status")).lower() != "scheduled":
                    return

                scheduled_at = _parse_datetime(safe_text(reminder.get("scheduled_at")))
                now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
                if scheduled_at > now:
                    await asyncio.sleep((scheduled_at - now).total_seconds())
                    continue

                ready_at = self._adjust_to_work_hours(now)
                if ready_at > now:
                    await db.update_reminder(
                        key.user_id,
                        key.campaign,
                        scheduled_at=ready_at.isoformat(),
                    )
                    await asyncio.sleep((ready_at - now).total_seconds())
                    continue

                settings = get_settings()
                if settings.reminder_only_if_no_lead and await db.has_lead(key.user_id, key.campaign):
                    await self._mark_cancelled(key, "lead")
                    return

                coupon_info = await coupons.get_user_coupon(key.user_id, key.campaign)
                if not coupon_info:
                    await self._mark_cancelled(key, "no_coupon")
                    return

                used_at = safe_text(coupon_info.get("used_at"))
                if settings.reminder_only_if_not_used and used_at:
                    await self._mark_cancelled(key, "coupon_used", {"used_at": used_at})
                    return

                code = safe_text(reminder.get("code") or coupon_info.get("code"))
                if not code:
                    await self._mark_cancelled(key, "no_code")
                    return

                bot = self._bot
                if bot is None:
                    logger.warning("Bot instance missing, postponing reminder for %s/%s", key.user_id, key.campaign)
                    await asyncio.sleep(5)
                    continue

                await self._send_reminder(bot, key, code)
                return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Reminder task crashed for %s/%s", key.user_id, key.campaign)
            await self._mark_cancelled(key, "error")

    async def _send_reminder(self, bot: Bot, key: ReminderKey, code: str) -> None:
        code_text = safe_text(code)
        payload = {
            "code": escape(code_text),
            "code_plain": code_text,
        }
        settings = get_settings()
        text_template = settings.reminder_text or "Не забудь воспользоваться подарком!"
        try:
            text = text_template.format(**payload)
        except Exception:
            text = text_template

        try:
            await bot.send_message(
                key.user_id,
                text,
                reply_markup=kb_after_coupon(key.campaign, key.user_id),
            )
        except (BotBlocked, ChatNotFound):
            await self._mark_cancelled(key, "unreachable")
            return
        except RetryAfter as exc:
            delay = int(getattr(exc, "timeout", 5))
            logger.warning(
                "RetryAfter while sending reminder to %s/%s, sleeping %s s",
                key.user_id,
                key.campaign,
                delay,
            )
            await asyncio.sleep(delay)
            await self._send_reminder(bot, key, code)
            return
        except TelegramAPIError:
            logger.exception("Telegram API error when sending reminder to %s/%s", key.user_id, key.campaign)
            await self._mark_cancelled(key, "send_failed")
            return

        sent_at = dt.datetime.utcnow().isoformat()
        await db.update_reminder(
            key.user_id,
            key.campaign,
            status="sent",
            sent_at=sent_at,
            reason="",
        )
        await stats.log_event(
            key.user_id,
            key.campaign,
            "reminder_sent",
            {"code": code_text, "sent_at": sent_at},
        )

    async def _mark_cancelled(
        self,
        key: ReminderKey,
        reason: str,
        extra_meta: Optional[Dict[str, str]] = None,
    ) -> None:
        reminder = await db.get_reminder(key.user_id, key.campaign)
        if not reminder or safe_text(reminder.get("status")).lower() != "scheduled":
            return
        cancelled_at = dt.datetime.utcnow().isoformat()
        reason_text = safe_text(reason)
        await db.update_reminder(
            key.user_id,
            key.campaign,
            status="cancelled",
            reason=reason_text,
            cancelled_at=cancelled_at,
        )
        meta: Dict[str, str] = {"reason": reason_text, "cancelled_at": cancelled_at}
        if extra_meta:
            meta.update({k: safe_text(v) for k, v in extra_meta.items()})
        await stats.log_event(key.user_id, key.campaign, "reminder_cancelled", meta)


_scheduler = ReminderScheduler()


async def on_startup(bot: Bot) -> None:
    await _scheduler.start(bot)


async def on_shutdown() -> None:
    await _scheduler.stop()


async def schedule_reminder(user_id: int, campaign: str, code: Optional[str]) -> None:
    await _scheduler.schedule(user_id, campaign, code)


async def cancel_due_to_lead(user_id: int, campaign: str) -> bool:
    return await _scheduler.cancel(user_id, campaign, "lead")
