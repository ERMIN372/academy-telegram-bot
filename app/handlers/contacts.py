from __future__ import annotations

import datetime as dt
from typing import Any

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.config import get_settings
from app.keyboards.common import kb_send_contact
from app.services import phone, sheets, stats


def _normalize_username(raw: str | None) -> str:
    if not raw:
        return ""
    username = raw.strip().lstrip("@")
    if not username:
        return ""
    return f"@{username.lower()}"


def _values_equal(value: Any, expected: str) -> bool:
    if value is None:
        return expected == ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value) == expected


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

    normalized = phone.normalize(contact_phone)
    if not normalized:
        await message.answer("Не удалось распознать номер. Попробуй в формате +7XXXXXXXXXX.")
        return

    settings = get_settings()
    normalized_username = _normalize_username(message.from_user.username)
    required_columns = ["username"]
    if settings.leads_upsert:
        required_columns.append("updated_at")
    await sheets.ensure_columns("leads", required_columns)

    now_iso = dt.datetime.utcnow().isoformat()
    lead_row = {
        "user_id": message.from_user.id,
        "username": normalized_username,
        "phone": normalized,
        "campaign": campaign,
        "created_at": now_iso,
        "status": "new",
    }

    if settings.leads_upsert:
        existing_leads = await sheets.read("leads")
        target_row: int | None = None
        for lead in existing_leads:
            if _values_equal(lead.get("user_id"), str(message.from_user.id)) and _values_equal(
                lead.get("campaign"), campaign
            ):
                target_row = int(lead.get("row")) if lead.get("row") is not None else None
                break
        if target_row:
            await sheets.update_row(
                "leads",
                target_row,
                {
                    "phone": normalized,
                    "username": normalized_username,
                    "updated_at": now_iso,
                },
            )
        else:
            lead_row["updated_at"] = now_iso
            await sheets.append("leads", lead_row)
    else:
        await sheets.append("leads", lead_row)

    await stats.log_event(
        message.from_user.id,
        campaign,
        "lead",
        {"username": normalized_username},
    )

    await message.answer("Спасибо! Мы свяжемся с тобой в ближайшее время.", reply_markup=types.ReplyKeyboardRemove())
    if settings.admin_chat_id:
        if normalized_username:
            user_ref = normalized_username
            profile_link = f"https://t.me/{normalized_username.lstrip('@')}"
        else:
            user_ref = str(message.from_user.id)
            profile_link = f"tg://user?id={message.from_user.id}"
        await message.bot.send_message(
            settings.admin_chat_id,
            "\n".join(
                [
                    f"Новый лид {normalized} по кампании {campaign}",
                    f"Пользователь: {user_ref}",
                    f"Профиль: {profile_link}",
                ]
            ),
        )


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(handle_contact, content_types=["contact", "text"], state="*")
