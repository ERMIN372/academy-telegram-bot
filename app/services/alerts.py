from __future__ import annotations

import asyncio
import datetime as dt
import html
import logging
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.dispatcher.handler import current_handler
from aiogram.utils.exceptions import TelegramAPIError

from app.config import get_settings
from app.services import stats

logger = logging.getLogger(__name__)

DEFAULT_ALERT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass
class ErrorAlert:
    step: str
    description: str
    trace_id: str
    when: dt.datetime
    last_action: str
    user_id: Optional[int]
    username: Optional[str]


class AlertManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._last_sent: Dict[str, dt.datetime] = {}
        self._error_bundles: Dict[str, List[ErrorAlert]] = {}
        self._error_tasks: Dict[str, asyncio.Task[None]] = {}
        self._error_bots: Dict[str, Bot] = {}
        self._no_coupon_attempts: Dict[str, int] = {}
        self._no_coupon_reported: Dict[str, bool] = {}

    async def notify_new_lead(
        self,
        bot: Bot,
        *,
        user_id: int,
        username: str | None,
        phone: str,
        campaign: str,
        created_at: dt.datetime,
    ) -> None:
        settings = get_settings()
        if not self._alerts_enabled(settings):
            await self._log_skip(
                user_id=user_id,
                campaign=campaign,
                reason="disabled",
                alert_type="new_lead",
            )
            return

        key = f"lead:{campaign}:{user_id}"
        if self._is_rate_limited(key, settings):
            await self._log_skip(
                user_id=user_id,
                campaign=campaign,
                reason="rate_limited",
                alert_type="new_lead",
            )
            return

        link, label = self._build_user_link(user_id, username)
        display_phone = phone if not settings.alerts_mask_phone else mask_phone(phone)
        created_at_display = _format_lead_timestamp(created_at, settings)
        mention = self._mention_line(settings)
        body_lines = [
            "🆕 Новый лид",
            f"ID: <code>{user_id}</code>",
            f"Профиль: <a href=\"{html.escape(link)}\">{html.escape(label)}</a>",
            f"Телефон: <code>{html.escape(display_phone)}</code>",
            f"Кампания: <code>{html.escape(campaign or 'default')}</code>",
            f"Создано: <code>{html.escape(created_at_display)}</code>",
            "Ответить в ЛС/созвонить в рабочее время",
        ]
        message = self._compose_message(mention, body_lines)
        meta = {
            "type": "new_lead",
            "campaign": campaign,
        }

        delivered = await self._deliver(bot, message, key, user_id, campaign, meta)
        if delivered:
            self._last_sent[key] = dt.datetime.utcnow()

    async def notify_no_coupons(
        self,
        bot: Bot,
        *,
        campaign: str,
    ) -> None:
        settings = get_settings()
        if not self._alerts_enabled(settings):
            await self._log_skip(
                user_id=0,
                campaign=campaign,
                reason="disabled",
                alert_type="no_coupons",
            )
            return

        async with self._lock:
            attempts = self._no_coupon_attempts.get(campaign, 0) + 1
            self._no_coupon_attempts[campaign] = attempts
            already_reported = self._no_coupon_reported.get(campaign, False)

        if already_reported:
            await self._log_skip(
                user_id=0,
                campaign=campaign,
                reason="already_reported",
                alert_type="no_coupons",
                extra={"attempts": attempts},
            )
            return

        key = f"no_coupons:{campaign}"
        if self._is_rate_limited(key, settings):
            await self._log_skip(
                user_id=0,
                campaign=campaign,
                reason="rate_limited",
                alert_type="no_coupons",
                extra={"attempts": attempts},
            )
            return

        mention = self._mention_line(settings)
        now = dt.datetime.utcnow()
        body_lines = [
            "⚠️ Коды закончились",
            f"Кампания: <code>{html.escape(campaign or 'default')}</code>",
            f"Попыток: <b>{attempts}</b>",
            f"Зафиксировано: <code>{now.strftime('%Y-%m-%d %H:%M:%S')} UTC</code>",
            "Пожалуйста, пополните лист coupons",
        ]
        message = self._compose_message(mention, body_lines)
        meta = {
            "type": "no_coupons",
            "campaign": campaign,
            "attempts": attempts,
        }

        delivered = await self._deliver(bot, message, key, 0, campaign, meta)
        if delivered:
            async with self._lock:
                self._no_coupon_reported[campaign] = True
            self._last_sent[key] = dt.datetime.utcnow()

    async def reset_no_coupons(self, campaign: str) -> None:
        async with self._lock:
            self._no_coupon_reported.pop(campaign, None)
            self._no_coupon_attempts.pop(campaign, None)

    async def notify_error(self, bot: Bot, payload: ErrorAlert) -> None:
        settings = get_settings()
        if not self._alerts_enabled(settings):
            await self._log_skip(
                user_id=payload.user_id or 0,
                campaign="system",
                reason="disabled",
                alert_type="error",
                extra={"step": payload.step, "trace_id": payload.trace_id},
            )
            return

        key = self._error_key(payload)
        async with self._lock:
            bundle = self._error_bundles.setdefault(key, [])
            bundle.append(payload)
            self._error_bots[key] = bot
            if key not in self._error_tasks:
                delay = max(0, settings.alerts_bundle_window)
                self._error_tasks[key] = asyncio.create_task(self._flush_error_bundle(key, delay))

    async def _flush_error_bundle(self, key: str, delay: int) -> None:
        try:
            if delay:
                await asyncio.sleep(delay)
            settings = get_settings()
            while True:
                async with self._lock:
                    entries = list(self._error_bundles.get(key, []))
                    bot = self._error_bots.get(key)
                    if not entries or bot is None:
                        self._error_tasks.pop(key, None)
                        self._error_bundles.pop(key, None)
                        self._error_bots.pop(key, None)
                        return
                now = dt.datetime.utcnow()
                if self._is_rate_limited(key, settings, now):
                    wait_for = self._rate_limit_remaining(key, settings, now)
                    if wait_for <= 0:
                        continue
                    await asyncio.sleep(wait_for)
                    continue

                async with self._lock:
                    entries = self._error_bundles.pop(key, [])
                    bot = self._error_bots.pop(key, None)
                    self._error_tasks.pop(key, None)
                if not entries or bot is None:
                    return
                message = self._format_error_alert(entries)
                meta = {
                    "type": "error",
                    "step": entries[0].step,
                    "trace_ids": [entry.trace_id for entry in entries],
                    "count": len(entries),
                }
                delivered = await self._deliver(
                    bot,
                    message,
                    key,
                    entries[-1].user_id or 0,
                    "system",
                    meta,
                )
                if delivered:
                    self._last_sent[key] = dt.datetime.utcnow()
                return
        except asyncio.CancelledError:
            return

    async def _deliver(
        self,
        bot: Bot,
        message: str,
        key: str,
        user_id: int,
        campaign: str,
        meta: Dict[str, Any],
    ) -> bool:
        settings = get_settings()
        targets = settings.admin_chat_ids
        if not targets:
            await self._log_skip(
                user_id=user_id,
                campaign=campaign,
                reason="no_target",
                alert_type=str(meta.get("type", "unknown")),
                extra=meta,
            )
            return False

        errors: List[str] = []
        for index, chat_id in enumerate(targets):
            try:
                await bot.send_message(chat_id, message, disable_web_page_preview=True)
                await stats.log_event(
                    user_id or 0,
                    campaign or "system",
                    "alert_sent",
                    {**meta, "target": chat_id, "fallback": index > 0},
                )
                return True
            except TelegramAPIError as exc:
                error_text = str(exc)
                errors.append(error_text)
                logger.warning("Failed to deliver alert to %s: %s", chat_id, error_text)
                await stats.log_event(
                    user_id or 0,
                    campaign or "system",
                    "alert_skipped",
                    {
                        **meta,
                        "reason": "send_failed",
                        "target": chat_id,
                        "error": error_text,
                        "fallback": index < len(targets) - 1,
                    },
                )
                continue
            except Exception as exc:  # pragma: no cover - defensive
                error_text = repr(exc)
                errors.append(error_text)
                logger.exception("Unexpected error during alert delivery")
                await stats.log_event(
                    user_id or 0,
                    campaign or "system",
                    "alert_skipped",
                    {
                        **meta,
                        "reason": "unexpected_error",
                        "target": chat_id,
                        "error": error_text,
                        "fallback": index < len(targets) - 1,
                    },
                )
                continue

        await self._log_skip(
            user_id=user_id,
            campaign=campaign,
            reason="delivery_failed",
            alert_type=str(meta.get("type", "unknown")),
            extra={**meta, "errors": errors},
        )
        return False

    async def _log_skip(
        self,
        *,
        user_id: int,
        campaign: str,
        reason: str,
        alert_type: str,
        extra: Dict[str, Any] | None = None,
    ) -> None:
        meta = {"type": alert_type, "reason": reason}
        if extra:
            meta.update(extra)
        await stats.log_event(user_id or 0, campaign or "system", "alert_skipped", meta)

    def _mention_line(self, settings) -> str | None:
        mention = settings.alerts_mention
        if mention:
            mention = mention.strip()
            if mention:
                return mention
        return None

    def _compose_message(self, mention: str | None, lines: Iterable[str]) -> str:
        body = "\n".join(lines)
        if mention:
            return f"{mention}\n{body}"
        return body

    def _build_user_link(self, user_id: int, username: str | None) -> tuple[str, str]:
        if username:
            normalized = username.lstrip("@").strip()
            return (f"https://t.me/{normalized}", f"@{normalized}")
        return (f"tg://user?id={user_id}", str(user_id))

    def _alerts_enabled(self, settings) -> bool:
        return bool(settings.alerts_enabled)

    def _is_rate_limited(
        self,
        key: str,
        settings,
        now: dt.datetime | None = None,
    ) -> bool:
        if settings.alerts_rate_limit <= 0:
            return False
        timestamp = self._last_sent.get(key)
        if not timestamp:
            return False
        now = now or dt.datetime.utcnow()
        delta = (now - timestamp).total_seconds()
        return delta < settings.alerts_rate_limit

    def _rate_limit_remaining(self, key: str, settings, now: dt.datetime) -> float:
        timestamp = self._last_sent.get(key)
        if not timestamp:
            return 0.0
        remaining = settings.alerts_rate_limit - (now - timestamp).total_seconds()
        return max(0.0, remaining)

    def _error_key(self, payload: ErrorAlert) -> str:
        description = payload.description or ""
        return f"error:{payload.step}:{description}".strip()

    def _format_error_alert(self, entries: List[ErrorAlert]) -> str:
        settings = get_settings()
        mention = self._mention_line(settings)
        count = len(entries)
        title = "🧯 Ошибка в боте"
        if count > 1:
            title += f" (×{count})"
        header = [title, f"Шаг: <code>{html.escape(entries[0].step or 'unknown')}</code>"]
        header.append(f"Описание: <code>{html.escape(entries[0].description or 'не указано')}</code>")
        detail_lines = []
        for item in entries:
            username = item.username or "—"
            last_action = item.last_action or "—"
            detail_lines.append(
                " • "
                + (
                    f"Trace <code>{html.escape(item.trace_id)}</code> — "
                    f"{item.when.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    f", user <code>{item.user_id or '—'}</code>, "
                    f"username <code>{html.escape(username)}</code>"
                    f", last_action <code>{html.escape(last_action)}</code>"
                )
            )
        header.append("Trace IDs:")
        header.extend(detail_lines)
        return self._compose_message(mention, header)


_alert_manager: AlertManager | None = None


def get_alert_manager() -> AlertManager:
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


async def notify_new_lead(
    bot: Bot,
    *,
    user_id: int,
    username: str | None,
    phone: str,
    campaign: str,
    created_at: dt.datetime,
) -> None:
    manager = get_alert_manager()
    await manager.notify_new_lead(
        bot,
        user_id=user_id,
        username=username,
        phone=phone,
        campaign=campaign,
        created_at=created_at,
    )


async def notify_no_coupons(bot: Bot, *, campaign: str) -> None:
    manager = get_alert_manager()
    await manager.notify_no_coupons(bot, campaign=campaign)


async def reset_no_coupons(campaign: str) -> None:
    manager = get_alert_manager()
    await manager.reset_no_coupons(campaign)


def _format_lead_timestamp(moment: dt.datetime, settings) -> str:
    timezone = _resolve_timezone(settings.alerts_timezone)
    time_format = settings.alerts_time_format or DEFAULT_ALERT_TIME_FORMAT
    aware = moment
    if aware.tzinfo is None:
        aware = aware.replace(tzinfo=dt.timezone.utc)
    else:
        aware = aware.astimezone(dt.timezone.utc)
    localized = aware.astimezone(timezone)
    try:
        formatted = localized.strftime(time_format)
    except Exception:
        logger.warning(
            "Invalid ALERTS_TIME_FORMAT '%s', falling back to default", time_format
        )
        formatted = localized.strftime(DEFAULT_ALERT_TIME_FORMAT)
    tz_label = _timezone_label(localized)
    return f"{formatted} {tz_label}".strip()


@lru_cache(maxsize=8)
def _resolve_timezone(name: str | None) -> dt.tzinfo:
    if not name:
        return dt.timezone.utc
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        logger.warning("Unknown ALERTS_TZ '%s', falling back to UTC", name)
        return dt.timezone.utc


def _timezone_label(moment: dt.datetime) -> str:
    label = (moment.tzname() or "").strip()
    if label and label.upper() != "UTC":
        return label
    offset = moment.utcoffset()
    if not offset:
        return "UTC"
    total_seconds = int(offset.total_seconds())
    if total_seconds == 0:
        return "UTC"
    sign = "+" if total_seconds >= 0 else "-"
    total_seconds = abs(total_seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes = remainder // 60
    if minutes:
        return f"UTC{sign}{hours:02d}:{minutes:02d}"
    return f"UTC{sign}{hours}"


async def notify_error(
    bot: Bot,
    *,
    step: str,
    description: str,
    trace_id: str,
    when: dt.datetime,
    last_action: str,
    user_id: int | None,
    username: str | None,
) -> None:
    payload = ErrorAlert(
        step=step,
        description=description,
        trace_id=trace_id,
        when=when,
        last_action=last_action,
        user_id=user_id,
        username=username,
    )
    manager = get_alert_manager()
    await manager.notify_error(bot, payload)


def setup_error_handler(dp: Dispatcher) -> None:
    @dp.errors_handler()
    async def _handle(update: types.Update, exception: Exception) -> bool:  # type: ignore[override]
        bot = dp.bot
        trace_id = uuid.uuid4().hex
        step = "unknown"
        handler = current_handler.get()
        if handler:
            step = getattr(handler, "__name__", repr(handler))
        description = f"{exception.__class__.__name__}: {exception}"
        when = dt.datetime.utcnow()
        user_id: int | None = None
        username: str | None = None
        last_action = ""
        if update.message:
            user_id = update.message.from_user.id if update.message.from_user else None
            username = update.message.from_user.username if update.message.from_user else None
            last_action = update.message.text or update.message.caption or "message"
        elif update.callback_query:
            user_id = update.callback_query.from_user.id
            username = update.callback_query.from_user.username
            last_action = update.callback_query.data or "callback"
        elif update.inline_query:
            user_id = update.inline_query.from_user.id
            username = update.inline_query.from_user.username
            last_action = update.inline_query.query or "inline"
        else:
            user = getattr(update, "effective_user", None)
            if user:
                user_id = user.id
                username = user.username
        await notify_error(
            bot,
            step=step,
            description=description,
            trace_id=trace_id,
            when=when,
            last_action=last_action,
            user_id=user_id,
            username=username,
        )
        logger.exception("Unhandled error %s", trace_id, exc_info=exception)
        return True


def mask_phone(phone: str) -> str:
    digits = [ch for ch in phone if ch.isdigit()]
    if len(digits) < 4:
        return "***"
    last_four = "".join(digits[-4:])
    if phone.startswith("+"):
        prefix = phone[:2]
    else:
        prefix = "+" + digits[0] if digits else "+"
    return f"{prefix}•••****{last_four}"
