from __future__ import annotations

import logging

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.config import get_settings
from app.keyboards.common import kb_send_contact
from app.services import phone, sheets, stats

logger = logging.getLogger(__name__)


async def handle_contact(message: types.Message, state: FSMContext) -> None:
    if message.text and message.text.startswith("/"):
        return

    data = await state.get_data()
    campaign = data.get("campaign", "default")

    if message.text and message.text.lower() == "отмена":
        await message.answer("Отменено.", reply_markup=types.ReplyKeyboardRemove())
        return

    contact_phone = None
    if message.contact and message.contact.phone_number:
        contact_phone = message.contact.phone_number
    elif message.text:
        contact_phone = message.text

    if not contact_phone:
        await message.answer("Пожалуйста, отправь номер телефона или нажми Отмена.", reply_markup=kb_send_contact())
        return

    normalized = phone.normalize(contact_phone)
    if not normalized:
        await message.answer("Не удалось распознать номер. Попробуй в формате +7XXXXXXXXXX.")
        return

    await stats.log_event(message.from_user.id, campaign, "lead_saved", {"phone": normalized})
    await sheets.append("leads", {
        "user_id": message.from_user.id,
        "username": message.from_user.username or "",
        "phone": normalized,
        "campaign": campaign,
    })

    settings = get_settings()
    await message.answer("Спасибо! Мы свяжемся с тобой в ближайшее время.", reply_markup=types.ReplyKeyboardRemove())
    if settings.admin_chat_id:
        username = message.from_user.username
        user_ref = f"@{username}" if username else str(message.from_user.id)
        await message.bot.send_message(settings.admin_chat_id, f"Новый лид {normalized} от {user_ref}")


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(handle_contact, content_types=["contact", "text"], state="*")
