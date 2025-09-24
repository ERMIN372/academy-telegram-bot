from __future__ import annotations

import logging
from html import escape

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.config import get_settings
from app.keyboards.common import kb_after_coupon, kb_check_sub, kb_subscribe
from app.services import alerts, coupons, reminders, stats, sub_check
from app.services import lottery as lottery_service
from app.services.deep_link import parse_start_payload
from app.storage import db

logger = logging.getLogger(__name__)


def _meta(user_id: int, campaign: str, username: str | None, extra: dict | None = None) -> dict:
    payload: dict = {}
    if extra:
        payload.update(extra)
    payload.setdefault("user_id", user_id)
    payload.setdefault("campaign", campaign or "default")
    if username:
        payload.setdefault("username", username)
    return payload


def _after_sub_keyboard(
    campaign: str,
    *,
    include_lottery: bool,
    lottery_label: str,
) -> types.InlineKeyboardMarkup:
    campaign_value = campaign or "default"
    markup = types.InlineKeyboardMarkup(row_width=1)
    if include_lottery:
        markup.add(
            types.InlineKeyboardButton(
                text=lottery_label,
                callback_data=f"start_lottery:{campaign_value}",
            )
        )
    markup.add(
        types.InlineKeyboardButton(
            text="🎁 Забрать подарок", callback_data=f"get_gift:{campaign_value}"
        )
    )
    markup.add(
        types.InlineKeyboardButton(
            text="📞 Оставить контакт", callback_data=f"leave_phone:{campaign_value}"
        )
    )
    return markup


async def cmd_start(message: types.Message, state: FSMContext) -> None:
    payload = parse_start_payload(message.text)
    config = lottery_service.get_config()
    username = message.from_user.username if message.from_user else None
    campaign = payload
    is_draw_payload = False
    if payload.startswith(config.draw_prefix):
        suffix = payload[len(config.draw_prefix) :].strip()
        if suffix:
            campaign = suffix
            is_draw_payload = True
        else:
            campaign = "default"

    await state.update_data(campaign=campaign)

    await stats.log_event(
        message.from_user.id,
        campaign,
        "start",
        _meta(
            message.from_user.id,
            campaign,
            username,
            {"payload": payload, "draw": is_draw_payload},
        ),
        username=username,
    )

    settings = get_settings()
    subscribe_markup = kb_subscribe(f"https://t.me/{settings.channel_username.lstrip('@')}")

    if is_draw_payload and not config.enabled:
        await message.answer(
            "Привет! Лотерея скоро вернётся — загляни позже.",
            reply_markup=subscribe_markup,
        )
        await message.answer(
            "Когда подпишешься, нажми кнопку ниже.",
            reply_markup=kb_check_sub(campaign),
        )
        return

    if is_draw_payload and config.enabled and config.results:
        await state.update_data(
            lottery_autostart={"campaign": campaign, "source": "deeplink"}
        )
        is_member = await sub_check.is_member(message.bot, message.from_user.id)
        if is_member:
            show_lottery_button, bucket = lottery_service.should_show_button(
                message.from_user.id
            )
            if bucket is not None:
                await stats.log_event(
                    message.from_user.id,
                    campaign,
                    "draw_bucket",
                    _meta(
                        message.from_user.id,
                        campaign,
                        username,
                        {
                            "bucket": bucket,
                            "show_lottery_button": show_lottery_button,
                            "source": "deeplink",
                        },
                    ),
                    username=username,
                )
            await stats.log_event(
                message.from_user.id,
                campaign,
                "sub_ok",
                _meta(
                    message.from_user.id,
                    campaign,
                    username,
                    {"source": "deeplink"},
                ),
                username=username,
            )
            from app.handlers import lottery as lottery_handlers

            await message.answer("Привет! Ты уже в клубе — запускаем розыгрыш.")
            await lottery_handlers.present_lottery(
                message,
                message.from_user.id,
                campaign,
                source="deeplink",
                trigger="deeplink_auto",
                username=username,
            )
            await state.update_data(lottery_autostart=None)
            return

    await message.answer(
        "Привет! Чтобы получить подарок, подпишись на канал и вернись сюда за проверкой.",
        reply_markup=subscribe_markup,
    )
    await message.answer(
        "Когда подпишешься, нажми кнопку ниже.",
        reply_markup=kb_check_sub(campaign),
    )


async def callback_check_sub(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = call.data.split(":", 1)[1] if call.data else "default"
    await state.update_data(campaign=campaign)
    is_member = await sub_check.is_member(call.bot, call.from_user.id)
    username = call.from_user.username if call.from_user else None
    if is_member:
        config = lottery_service.get_config()
        include_lottery = bool(config.enabled and config.results)
        show_lottery_button = include_lottery
        bucket_meta: dict[str, object] = {}
        if include_lottery:
            show_lottery_button, bucket = lottery_service.should_show_button(
                call.from_user.id
            )
            bucket_meta = {"bucket": bucket, "show_lottery_button": show_lottery_button}
            if bucket is not None:
                await stats.log_event(
                    call.from_user.id,
                    campaign,
                    "draw_bucket",
                    _meta(
                        call.from_user.id,
                        campaign,
                        username,
                        {"bucket": bucket, "show_lottery_button": show_lottery_button},
                    ),
                    username=username,
                )
        else:
            show_lottery_button = False

        await stats.log_event(
            call.from_user.id,
            campaign,
            "sub_ok",
            _meta(
                call.from_user.id,
                campaign,
                username,
                {"source": "button", **bucket_meta},
            ),
            username=username,
        )
        keyboard = _after_sub_keyboard(
            campaign,
            include_lottery=show_lottery_button,
            lottery_label=config.button_label,
        )
        await call.message.answer(
            "Отлично! Теперь забери свой подарок.",
            reply_markup=keyboard,
        )
        data = await state.get_data()
        autostart = data.get("lottery_autostart") if isinstance(data, dict) else None
        if (
            autostart
            and autostart.get("campaign") == campaign
            and include_lottery
        ):
            from app.handlers import lottery as lottery_handlers

            await lottery_handlers.present_lottery(
                call.message,
                call.from_user.id,
                campaign,
                source=autostart.get("source", "deeplink"),
                trigger="deeplink_post_check",
                username=username,
            )
            await state.update_data(lottery_autostart=None)
    else:
        await stats.log_event(
            call.from_user.id,
            campaign,
            "sub_fail",
            _meta(
                call.from_user.id,
                campaign,
                username,
                {"source": "button"},
            ),
            username=username,
        )
        await call.message.answer("Похоже, подписка еще не оформлена. Попробуй снова позже.")


async def _send_coupon(message: types.Message, code: str, campaign: str) -> None:
    text = (
        f"Твой уникальный купон: <b>{escape(code)}</b>\n\n"
        "Условия использования:\n"
        "• Купон не суммируется с другими акциями.\n"
        "• Не подходит для продления действующей подписки.\n"
        "• Действует до дедлайна кампании.\n\n"
        "Оставь контакт, чтобы получить напоминание и инструкции."
    )
    await message.answer(text, reply_markup=kb_after_coupon(campaign))


async def issue_coupon(
    message: types.Message,
    user_id: int,
    campaign: str,
    *,
    stats_campaign: str | None = None,
    no_coupons_message: str | None = None,
) -> bool:
    coupon_campaign = campaign or "default"
    stats_campaign = stats_campaign or coupon_campaign
    username = message.from_user.username if message.from_user else None

    stored = await db.fetch_user_coupon(user_id, coupon_campaign)
    if stored and stored.get("code"):
        code = stored["code"]
        await stats.log_event(
            user_id,
            stats_campaign,
            "gift_repeat",
            _meta(
                user_id,
                stats_campaign,
                username,
                {"code": code, "coupon_campaign": coupon_campaign},
            ),
            username=username,
        )
        await alerts.reset_no_coupons(coupon_campaign)
        await _send_coupon(message, code, coupon_campaign)
        await reminders.schedule_reminder(user_id, coupon_campaign, code)
        return True

    sheet_coupon = await coupons.get_user_coupon(user_id, coupon_campaign)
    if sheet_coupon and sheet_coupon.get("code"):
        code = sheet_coupon["code"]
        await db.insert_coupon(user_id, coupon_campaign, code)
        await stats.log_event(
            user_id,
            stats_campaign,
            "gift_repeat",
            _meta(
                user_id,
                stats_campaign,
                username,
                {"code": code, "coupon_campaign": coupon_campaign},
            ),
            username=username,
        )
        await alerts.reset_no_coupons(coupon_campaign)
        await _send_coupon(message, code, coupon_campaign)
        await reminders.schedule_reminder(user_id, coupon_campaign, code)
        return True

    coupon = await coupons.find_first_free_coupon(coupon_campaign)
    if not coupon or not coupon.get("code"):
        await stats.log_event(
            user_id,
            stats_campaign,
            "no_coupons",
            _meta(
                user_id,
                stats_campaign,
                username,
                {"coupon_campaign": coupon_campaign},
            ),
            username=username,
        )
        await message.answer(
            no_coupons_message
            or "Упс! Похоже, подарков временно нет. Мы уже работаем над этим."
        )
        await alerts.notify_no_coupons(message.bot, campaign=coupon_campaign)
        return False

    reservation = await coupons.reserve_coupon(coupon["row"], user_id, coupon["code"])
    code = reservation["code"]
    await db.insert_coupon(user_id, coupon_campaign, code)
    await stats.log_event(
        user_id,
        stats_campaign,
        "gift",
        _meta(
            user_id,
            stats_campaign,
            username,
            {"code": code, "coupon_campaign": coupon_campaign},
        ),
        username=username,
    )
    await alerts.reset_no_coupons(coupon_campaign)
    await _send_coupon(message, code, coupon_campaign)
    await reminders.schedule_reminder(user_id, coupon_campaign, code)
    return True


async def callback_get_gift(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = call.data.split(":", 1)[1] if call.data else "default"
    await state.update_data(campaign=campaign)
    await _start_lottery_flow(call, campaign, trigger="gift_button")


async def _start_lottery_flow(
    call: types.CallbackQuery, campaign: str, *, trigger: str
) -> None:
    username = call.from_user.username if call.from_user else None
    is_member = await sub_check.is_member(call.bot, call.from_user.id)
    if not is_member:
        await stats.log_event(
            call.from_user.id,
            campaign,
            "sub_fail",
            _meta(
                call.from_user.id,
                campaign,
                username,
                {"source": "button", "trigger": trigger},
            ),
            username=username,
        )
        await call.message.answer(
            "Подписка не найдена. Подпишись на канал и повтори проверку.",
            reply_markup=kb_check_sub(campaign),
        )
        return

    from app.handlers import lottery as lottery_handlers

    await lottery_handlers.present_lottery(
        call.message,
        call.from_user.id,
        campaign,
        source="button",
        trigger=trigger,
        username=username,
    )


async def callback_start_lottery(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = call.data.split(":", 1)[1] if call.data else "default"
    await state.update_data(campaign=campaign)
    await _start_lottery_flow(call, campaign, trigger="lottery_button")


async def callback_leave_phone(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = call.data.split(":", 1)[1] if call.data else "default"
    await state.update_data(campaign=campaign)
    username = call.from_user.username if call.from_user else None
    await stats.log_event(
        call.from_user.id,
        campaign,
        "lead_prompt",
        _meta(call.from_user.id, campaign, username),
        username=username,
    )
    from app.keyboards.common import kb_send_contact

    await call.message.answer("Отправь, пожалуйста, свой номер.", reply_markup=kb_send_contact())


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_start, commands=["start"], state="*")
    dp.register_callback_query_handler(callback_check_sub, lambda c: c.data and c.data.startswith("check_sub:"))
    dp.register_callback_query_handler(callback_get_gift, lambda c: c.data and c.data.startswith("get_gift:"))
    dp.register_callback_query_handler(callback_start_lottery, lambda c: c.data and c.data.startswith("start_lottery:"))
    dp.register_callback_query_handler(callback_leave_phone, lambda c: c.data and c.data.startswith("leave_phone:"))
