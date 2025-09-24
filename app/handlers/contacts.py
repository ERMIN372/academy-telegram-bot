from __future__ import annotations

import datetime as dt
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
    campaign = str(data.get("campaign") or "default")

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

    normalized_phone = phone.normalize(contact_phone)
    if not normalized_phone:
        await message.answer("Не удалось распознать номер. Попробуй в формате +7XXXXXXXXXX.")
        return

    raw_username = message.from_user.username or ""
    normalized_username = ""
    if raw_username:
        normalized_username = f"@{raw_username.lstrip('@').lower()}"

    lead_payload = {
        "user_id": message.from_user.id,
        "username": normalized_username,
        "phone": normalized_phone,
        "campaign": campaign,
        "created_at": dt.datetime.utcnow().isoformat(),
        "status": "new",
    }

    settings = get_settings()

    if settings.leads_upsert:
        leads = await sheets.read("leads")
        existing = next(
            (
                item
                for item in leads
                if str(item.get("user_id")) == str(message.from_user.id)
                and str(item.get("campaign") or "default") == campaign
            ),
            None,
        )
        if existing:
            await sheets.update_row(
                "leads",
                existing["row"],
                {
                    "phone": normalized_phone,
                    "username": normalized_username,
                    "updated_at": dt.datetime.utcnow().isoformat(),
                },
            )
        else:
            await sheets.append("leads", lead_payload)
    else:
        await sheets.append("leads", lead_payload)

    await stats.log_event(
        message.from_user.id,
        campaign,
        "lead",
        {
            "user_id": message.from_user.id,
            "campaign": campaign,
            "username": normalized_username,
        },
    )
    await message.answer("Спасибо! Мы свяжемся с тобой в ближайшее время.", reply_markup=types.ReplyKeyboardRemove())
    if settings.admin_chat_id:
        if normalized_username:
            user_link = f"https://t.me/{normalized_username.lstrip('@')}"
            user_ref = f"{normalized_username} ({user_link})"
        else:
            user_link = f"tg://user?id={message.from_user.id}"
            user_ref = user_link
        await message.bot.send_message(
            settings.admin_chat_id,
            f"Новый лид {normalized_phone} по кампании {campaign} от {user_ref}",
        )


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(handle_contact, content_types=["contact", "text"], state="*")
