from __future__ import annotations

import logging

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.config import get_settings, is_admin
from app.keyboards.common import kb_admin_panel, kb_cancel_admin, kb_main_menu
from app.services import coupons, sheets

logger = logging.getLogger(__name__)

# FSM states for admin operations
ADMIN_ADD_COUPON = "admin_add_coupon"
ADMIN_ADD_COUPON_CAMPAIGN = "admin_add_coupon_campaign"
ADMIN_ADD_BULK = "admin_add_bulk"
ADMIN_ADD_BULK_CAMPAIGN = "admin_add_bulk_campaign"


async def cmd_ping(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer("pong")


async def cmd_report(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
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


async def handle_admin_panel(message: types.Message, state: FSMContext) -> None:
    """Show admin panel when user clicks '⚙️ Админ панель' button."""
    if not is_admin(message.from_user.id):
        return

    await state.finish()
    await message.answer(
        "⚙️ <b>Админ панель</b>\n\nВыберите действие:",
        reply_markup=kb_admin_panel(),
    )


async def callback_admin_action(callback: types.CallbackQuery, state: FSMContext) -> None:
    """Handle admin panel callbacks."""
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет доступа", show_alert=True)
        return

    action = callback.data.split(":")[1] if ":" in callback.data else ""

    if action == "add_coupon":
        await callback.message.edit_text(
            "➕ <b>Добавление купона</b>\n\n"
            "Отправьте код купона (например: PROMO123)\n"
            "или нажмите Отмена для выхода.",
        )
        await state.set_state(ADMIN_ADD_COUPON)
        await callback.message.answer("👇", reply_markup=kb_cancel_admin())

    elif action == "add_bulk":
        await callback.message.edit_text(
            "➕ <b>Массовое добавление купонов</b>\n\n"
            "Отправьте купоны, каждый с новой строки:\n"
            "PROMO1\n"
            "PROMO2\n"
            "PROMO3\n\n"
            "или нажмите Отмена для выхода.",
        )
        await state.set_state(ADMIN_ADD_BULK)
        await callback.message.answer("👇", reply_markup=kb_cancel_admin())

    elif action == "stats":
        await callback.answer("Загружаю статистику...")
        events = await sheets.read("events")
        leads = await sheets.read("leads")
        coupons_data = await sheets.read("coupons")

        # Count coupon statuses
        free_count = sum(1 for c in coupons_data if c.get("status", "").lower() in ["", "free"])
        reserved_count = sum(1 for c in coupons_data if c.get("status", "").lower() == "reserved")
        used_count = sum(1 for c in coupons_data if c.get("status", "").lower() == "used")

        text = (
            "📊 <b>Статистика</b>\n\n"
            f"📝 Событий: {len(events)}\n"
            f"👥 Лидов: {len(leads)}\n\n"
            f"🎁 Купоны:\n"
            f"├ Всего: {len(coupons_data)}\n"
            f"├ Свободных: {free_count}\n"
            f"├ Зарезервированных: {reserved_count}\n"
            f"└ Использованных: {used_count}"
        )
        await callback.message.edit_text(text, reply_markup=kb_admin_panel())

    elif action == "close":
        await callback.message.delete()
        await callback.message.answer(
            "Админ панель закрыта",
            reply_markup=kb_main_menu(is_admin=True),
        )

    await callback.answer()


async def handle_add_coupon_code(message: types.Message, state: FSMContext) -> None:
    """Handle coupon code input."""
    if not is_admin(message.from_user.id):
        return

    if message.text == "❌ Отмена":
        await state.finish()
        await message.answer(
            "Отменено",
            reply_markup=kb_main_menu(is_admin=True),
        )
        return

    code = message.text.strip()
    if not code:
        await message.answer("❌ Код не может быть пустым. Попробуйте еще раз:")
        return

    # Ask for campaign
    await state.update_data(coupon_code=code)
    await state.set_state(ADMIN_ADD_COUPON_CAMPAIGN)
    await message.answer(
        f"Код купона: <code>{code}</code>\n\n"
        "Теперь отправьте название кампании (например: intensive)\n"
        "или отправьте '-' чтобы оставить пустым.",
    )


async def handle_add_coupon_campaign(message: types.Message, state: FSMContext) -> None:
    """Handle campaign input and save coupon."""
    if not is_admin(message.from_user.id):
        return

    if message.text == "❌ Отмена":
        await state.finish()
        await message.answer(
            "Отменено",
            reply_markup=kb_main_menu(is_admin=True),
        )
        return

    campaign = message.text.strip() if message.text.strip() != "-" else ""
    data = await state.get_data()
    code = data.get("coupon_code", "")

    # Add coupon
    success = await coupons.add_coupon(code, campaign)

    if success:
        campaign_text = f" (кампания: {campaign})" if campaign else ""
        await message.answer(
            f"✅ Купон <code>{code}</code>{campaign_text} успешно добавлен!",
            reply_markup=kb_main_menu(is_admin=True),
        )
    else:
        await message.answer(
            "❌ Ошибка при добавлении купона",
            reply_markup=kb_main_menu(is_admin=True),
        )

    await state.finish()


async def handle_add_bulk_codes(message: types.Message, state: FSMContext) -> None:
    """Handle bulk coupon codes input."""
    if not is_admin(message.from_user.id):
        return

    if message.text == "❌ Отмена":
        await state.finish()
        await message.answer(
            "Отменено",
            reply_markup=kb_main_menu(is_admin=True),
        )
        return

    lines = [line.strip() for line in message.text.split("\n") if line.strip()]
    if not lines:
        await message.answer("❌ Не найдено ни одного купона. Попробуйте еще раз:")
        return

    # Ask for campaign
    await state.update_data(coupon_codes=lines)
    await state.set_state(ADMIN_ADD_BULK_CAMPAIGN)
    await message.answer(
        f"Найдено купонов: {len(lines)}\n\n"
        "Теперь отправьте название кампании (например: intensive)\n"
        "или отправьте '-' чтобы оставить пустым.",
    )


async def handle_add_bulk_campaign(message: types.Message, state: FSMContext) -> None:
    """Handle campaign input and save bulk coupons."""
    if not is_admin(message.from_user.id):
        return

    if message.text == "❌ Отмена":
        await state.finish()
        await message.answer(
            "Отменено",
            reply_markup=kb_main_menu(is_admin=True),
        )
        return

    campaign = message.text.strip() if message.text.strip() != "-" else ""
    data = await state.get_data()
    codes = data.get("coupon_codes", [])

    # Add coupons
    count = await coupons.add_multiple_coupons(codes, campaign)

    campaign_text = f" в кампанию '{campaign}'" if campaign else ""
    await message.answer(
        f"✅ Добавлено купонов{campaign_text}: {count} из {len(codes)}",
        reply_markup=kb_main_menu(is_admin=True),
    )

    await state.finish()


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_ping, commands=["ping"], state="*")
    dp.register_message_handler(cmd_report, commands=["report"], state="*")

    # Admin panel
    dp.register_message_handler(
        handle_admin_panel,
        lambda message: message.text == "⚙️ Админ панель",
        state="*",
    )
    dp.register_callback_query_handler(
        callback_admin_action,
        lambda c: c.data.startswith("admin:"),
        state="*",
    )

    # Add single coupon flow
    dp.register_message_handler(
        handle_add_coupon_code,
        state=ADMIN_ADD_COUPON,
    )
    dp.register_message_handler(
        handle_add_coupon_campaign,
        state=ADMIN_ADD_COUPON_CAMPAIGN,
    )

    # Add bulk coupons flow
    dp.register_message_handler(
        handle_add_bulk_codes,
        state=ADMIN_ADD_BULK,
    )
    dp.register_message_handler(
        handle_add_bulk_campaign,
        state=ADMIN_ADD_BULK_CAMPAIGN,
    )
