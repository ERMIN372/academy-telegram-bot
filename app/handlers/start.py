from __future__ import annotations

import logging

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.config import get_settings
from app.keyboards.common import kb_after_coupon, kb_check_sub, kb_get_gift, kb_subscribe
from app.services import coupons, stats, sub_check
from app.services.deep_link import parse_start_payload
from app.storage import db

logger = logging.getLogger(__name__)


async def cmd_start(message: types.Message, state: FSMContext) -> None:
    campaign = parse_start_payload(message.text)
    await state.update_data(campaign=campaign)
    await stats.log_event(message.from_user.id, campaign, "start")

    settings = get_settings()
    text = (
        "Привет! Чтобы получить подарок, подпишись на канал и нажми проверку."
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
    text = f"Твой уникальный купон: <b>{code}</b>\nНе забудь сохранить его!"
    await message.answer(text, parse_mode="HTML", reply_markup=kb_after_coupon(campaign))


async def issue_coupon(message: types.Message, user_id: int, campaign: str) -> None:
    stored = await db.fetch_user_coupon(user_id, campaign)
    if stored and stored.get("code"):
        await stats.log_event(user_id, campaign, "gift_repeat")
        await _send_coupon(message, stored["code"], campaign)
        return

    existing = await coupons.get_user_coupon(user_id, campaign)
    if existing and existing.get("code"):
        await db.insert_coupon(user_id, campaign, existing["code"])
        await stats.log_event(user_id, campaign, "gift_restore")
        await _send_coupon(message, existing["code"], campaign)
        return

    coupon = await coupons.find_first_free_coupon(campaign)
    if not coupon or not coupon.get("code"):
        await stats.log_event(user_id, campaign, "no_coupons")
        await message.answer("Упс! Похоже, подарков временно нет. Мы уже работаем над этим.")
        settings = get_settings()
        if settings.admin_chat_id:
            await message.bot.send_message(settings.admin_chat_id, f"Нет купонов для кампании {campaign}")
        return

    await coupons.reserve_coupon(coupon["row"], user_id, coupon["code"])
    await db.insert_coupon(user_id, campaign, coupon["code"])
    await stats.log_event(user_id, campaign, "gift_issued", {"code": coupon["code"]})
    await _send_coupon(message, coupon["code"], campaign)


async def callback_get_gift(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = call.data.split(":", 1)[1] if call.data else "default"
    await state.update_data(campaign=campaign)
    await issue_coupon(call.message, call.from_user.id, campaign)


async def callback_leave_phone(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = call.data.split(":", 1)[1] if call.data else "default"
    await state.update_data(campaign=campaign)
    await stats.log_event(call.from_user.id, campaign, "leave_phone")
    from app.keyboards.common import kb_send_contact

    await call.message.answer("Отправь, пожалуйста, свой номер.", reply_markup=kb_send_contact())


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_start, commands=["start"], state="*")
    dp.register_callback_query_handler(callback_check_sub, lambda c: c.data and c.data.startswith("check_sub:"))
    dp.register_callback_query_handler(callback_get_gift, lambda c: c.data and c.data.startswith("get_gift:"))
    dp.register_callback_query_handler(callback_leave_phone, lambda c: c.data and c.data.startswith("leave_phone:"))
