from __future__ import annotations

import datetime as dt
import logging

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.config import get_settings
from app.handlers import intensive as intensive_handlers
from app.keyboards.common import kb_main_menu, kb_send_contact
from app.services import alerts, phone, reminders, sheets, stats
from app.storage import db

logger = logging.getLogger(__name__)


async def handle_contact(message: types.Message, state: FSMContext) -> None:
    if message.text and message.text.startswith("/"):
        return

    data = await state.get_data()
    lead_context = data.get("lead_context")
    if not lead_context:
        return

    flow = str(lead_context.get("flow") or "default").lower()
    campaign = str(lead_context.get("campaign") or data.get("campaign") or "default")

    if flow == "intensive":
        await intensive_handlers.process_lead_message(message, state, lead_context)
        return

    if message.text and message.text.lower() == "отмена":
        await message.answer("Отменено.", reply_markup=types.ReplyKeyboardRemove())
        await state.update_data(lead_context=None)
        await message.answer(
            "Хорошо, действия доступны на клавиатуре ниже.",
            reply_markup=kb_main_menu(),
        )
        return

    contact_phone: str | None = None
    if message.contact and message.contact.phone_number:
        contact_phone = message.contact.phone_number
    elif message.text:
        contact_phone = message.text

    if not contact_phone:
        await message.answer(
            "Пожалуйста, отправьте номер телефона или нажмите Отмена.",
            reply_markup=kb_send_contact(),
        )
        return

    normalized_phone = phone.normalize(contact_phone)
    if not normalized_phone:
        await message.answer(
            "Не удалось распознать номер. Попробуйте в формате +7XXXXXXXXXX."
        )
        return

    raw_username = message.from_user.username or ""
    normalized_username = ""
    if raw_username:
        normalized_username = f"@{raw_username.lstrip('@').lower()}"

    timestamp = sheets.current_timestamp()
    created_at = timestamp.moment
    lead_payload = {
        "user_id": message.from_user.id,
        "username": normalized_username,
        "phone": normalized_phone,
        "campaign": campaign,
        "created_at": timestamp.utc_text,
        "created_at_msk": timestamp.local_text,
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
            await sheets.append(
                "leads",
                lead_payload,
                optional_headers=["created_at_msk"],
                meta=timestamp.meta,
            )
    else:
        await sheets.append(
            "leads",
            lead_payload,
            optional_headers=["created_at_msk"],
            meta=timestamp.meta,
        )

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
    await db.upsert_lead(message.from_user.id, campaign)
    await reminders.cancel_due_to_lead(message.from_user.id, campaign)
    await message.answer(
        "Спасибо! Мы свяжемся с вами в ближайшее время.",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    await message.answer(
        "Если понадобится, воспользуйтесь клавиатурой ниже.",
        reply_markup=kb_main_menu(),
    )
    await alerts.notify_new_lead(
        message.bot,
        user_id=message.from_user.id,
        username=normalized_username or None,
        phone=normalized_phone,
        campaign=campaign,
        created_at=created_at,
    )
    await state.update_data(lead_context=None)


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(handle_contact, content_types=["contact", "text"], state="*")
