from __future__ import annotations

import logging
from html import escape

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.config import get_settings
from app.keyboards.common import kb_after_coupon, kb_check_sub, kb_get_gift, kb_subscribe
from app.services import coupons, reminders, stats, sub_check
from app.services.deep_link import parse_start_payload
from app.storage import db

logger = logging.getLogger(__name__)


async def cmd_start(message: types.Message, state: FSMContext) -> None:
    campaign = parse_start_payload(message.text)
    await state.update_data(campaign=campaign)
    await stats.log_event(message.from_user.id, campaign, "start")

    settings = get_settings()
    text = (
        "Привет! Чтобы получить подарок, подпишись на канал и вернись сюда за проверкой."
    )
    await message.answer(
        text,
        reply_markup=kb_subscribe(f"https://t.me/{settings.channel_username.lstrip('@')}")
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
    if is_member:
        await stats.log_event(call.from_user.id, campaign, "sub_ok")
        await call.message.answer(
            "Отлично! Теперь забери свой подарок.",
            reply_markup=kb_get_gift(campaign),
        )
    else:
        await stats.log_event(call.from_user.id, campaign, "sub_fail")
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

    stored = await db.fetch_user_coupon(user_id, coupon_campaign)
    if stored and stored.get("code"):
        code = stored["code"]
        await stats.log_event(
            user_id,
            stats_campaign,
            "gift_repeat",
            {"code": code, "coupon_campaign": coupon_campaign},
        )
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
            {"code": code, "coupon_campaign": coupon_campaign},
        )
        await _send_coupon(message, code, coupon_campaign)
        await reminders.schedule_reminder(user_id, coupon_campaign, code)
        return True

    coupon = await coupons.find_first_free_coupon(coupon_campaign)
    if not coupon or not coupon.get("code"):
        await stats.log_event(user_id, stats_campaign, "no_coupons", {"coupon_campaign": coupon_campaign})
        await message.answer(
            no_coupons_message
            or "Упс! Похоже, подарков временно нет. Мы уже работаем над этим."
        )
        settings = get_settings()
        if settings.admin_chat_id:
            await message.bot.send_message(
                settings.admin_chat_id,
                f"Нет купонов для кампании {coupon_campaign}. Пользователь {user_id}",
            )
        return False

    reservation = await coupons.reserve_coupon(coupon["row"], user_id, coupon["code"])
    code = reservation["code"]
    await db.insert_coupon(user_id, coupon_campaign, code)
    await stats.log_event(
        user_id,
        stats_campaign,
        "gift",
        {"code": code, "coupon_campaign": coupon_campaign},
    )
    await _send_coupon(message, code, coupon_campaign)
    await reminders.schedule_reminder(user_id, coupon_campaign, code)
    return True


async def callback_get_gift(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = call.data.split(":", 1)[1] if call.data else "default"
    await state.update_data(campaign=campaign)
    is_member = await sub_check.is_member(call.bot, call.from_user.id)
    if not is_member:
        await stats.log_event(call.from_user.id, campaign, "sub_fail")
        await call.message.answer(
            "Подписка не найдена. Подпишись на канал и повтори проверку.",
            reply_markup=kb_check_sub(campaign),
        )
        return
    from app.handlers import lottery as lottery_handlers

    await lottery_handlers.present_lottery(call.message, call.from_user.id, campaign)


async def callback_leave_phone(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = call.data.split(":", 1)[1] if call.data else "default"
    await state.update_data(campaign=campaign)
    await stats.log_event(call.from_user.id, campaign, "lead_prompt")
    from app.keyboards.common import kb_send_contact

    await call.message.answer("Отправь, пожалуйста, свой номер.", reply_markup=kb_send_contact())


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_start, commands=["start"], state="*")
    dp.register_callback_query_handler(callback_check_sub, lambda c: c.data and c.data.startswith("check_sub:"))
    dp.register_callback_query_handler(callback_get_gift, lambda c: c.data and c.data.startswith("get_gift:"))
    dp.register_callback_query_handler(callback_leave_phone, lambda c: c.data and c.data.startswith("leave_phone:"))
