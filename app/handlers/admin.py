from __future__ import annotations

import logging

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

from app.config import get_settings, is_admin_user
from app.services import sheets
from app.utils import safe_text

logger = logging.getLogger(__name__)

class AdminCouponStates(StatesGroup):
    waiting_code = State()
    waiting_campaign = State()


async def cmd_ping(message: types.Message) -> None:
    if not is_admin_user(message.from_user.id, message.from_user.username):
        return
    await message.answer("pong")


async def cmd_report(message: types.Message) -> None:
    if not is_admin_user(message.from_user.id, message.from_user.username):
        return
    events = await sheets.read("events")
    leads = await sheets.read("leads")
    coupons_data = await sheets.read("coupons")
    text = (
        "Отчет:\n"
        f"Событий: {len(events)}\n"
        f"Лидов: {len(leads)}\n"
        f"Купонов в таблице: {len(coupons_data)}"
    )
    await message.answer(text)


def _admin_panel_kb() -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton(text="➕ Добавить купон", callback_data="admin_add_coupon")
    )
    markup.add(
        types.InlineKeyboardButton(text="📊 Отчет", callback_data="admin_report")
    )
    return markup


async def cmd_admin(message: types.Message, state: FSMContext) -> None:
    logger.info(f"cmd_admin called: user_id={message.from_user.id}, username={message.from_user.username}")
    if not is_admin_user(message.from_user.id, message.from_user.username):
        logger.warning(f"User {message.from_user.id} is not admin, blocking access")
        return
    logger.info("Opening admin panel")
    await state.finish()
    await message.answer("Админ-панель:", reply_markup=_admin_panel_kb())


async def cmd_cancel(message: types.Message, state: FSMContext) -> None:
    if not is_admin_user(message.from_user.id, message.from_user.username):
        return
    await state.finish()
    await message.answer("Действие отменено.", reply_markup=_admin_panel_kb())


async def callback_admin_report(call: types.CallbackQuery) -> None:
    if not is_admin_user(call.from_user.id, call.from_user.username):
        await call.answer()
        return
    await call.answer()
    await cmd_report(call.message)


async def callback_admin_add_coupon(call: types.CallbackQuery, state: FSMContext) -> None:
    if not is_admin_user(call.from_user.id, call.from_user.username):
        await call.answer()
        return
    await call.answer()
    await state.finish()
    await AdminCouponStates.waiting_code.set()
    await call.message.answer(
        "Отправьте код купона. Для отмены — /cancel."
    )


async def message_admin_coupon_code(message: types.Message, state: FSMContext) -> None:
    if not is_admin_user(message.from_user.id, message.from_user.username):
        return
    if not message.text:
        await message.answer("Пришлите текстовый код купона.")
        return
    code = safe_text(message.text)
    if not code:
        await message.answer("Код не распознан. Попробуйте ещё раз.")
        return
    await state.update_data(code=code)
    await AdminCouponStates.waiting_campaign.set()
    await message.answer(
        "Укажите кампанию для купона или отправьте «-», чтобы оставить пустым."
    )


async def message_admin_coupon_campaign(message: types.Message, state: FSMContext) -> None:
    if not is_admin_user(message.from_user.id, message.from_user.username):
        return
    if not message.text:
        await message.answer("Пришлите кампанию текстом или «-».")
        return
    raw = safe_text(message.text)
    campaign = raw
    if raw in {"-", "—", "нет", "без", "none"}:
        campaign = ""
    data = await state.get_data()
    code = safe_text(data.get("code")) if isinstance(data, dict) else ""
    if not code:
        await state.finish()
        await message.answer("Не удалось получить код. Попробуйте снова.", reply_markup=_admin_panel_kb())
        return
    await sheets.append(
        "coupons",
        {
            "code": code,
            "status": "free",
            "campaign": campaign,
        },
    )
    await state.finish()
    campaign_note = campaign or "без кампании"
    await message.answer(
        f"Купон добавлен: <b>{code}</b> ({campaign_note}).",
        reply_markup=_admin_panel_kb(),
    )


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_ping, commands=["ping"], state="*")
    dp.register_message_handler(cmd_report, commands=["report"], state="*")
    dp.register_message_handler(cmd_admin, commands=["admin"], state="*")
    dp.register_message_handler(cmd_admin, lambda message: message.text == "Админ-панель", state="*")
    dp.register_message_handler(cmd_cancel, commands=["cancel"], state="*")
    dp.register_callback_query_handler(callback_admin_report, lambda c: c.data == "admin_report")
    dp.register_callback_query_handler(callback_admin_add_coupon, lambda c: c.data == "admin_add_coupon")
    dp.register_message_handler(
        message_admin_coupon_code,
        state=AdminCouponStates.waiting_code,
        content_types=types.ContentTypes.TEXT,
    )
    dp.register_message_handler(
        message_admin_coupon_campaign,
        state=AdminCouponStates.waiting_campaign,
        content_types=types.ContentTypes.TEXT,
    )
