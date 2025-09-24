from __future__ import annotations

from aiogram import Dispatcher, types

from app.config import get_settings
from app.services import sheets


def _is_admin(user_id: int) -> bool:
    settings = get_settings()
    return settings.admin_chat_id and int(settings.admin_chat_id) == user_id


async def cmd_ping(message: types.Message) -> None:
    if not _is_admin(message.from_user.id):
        return
    await message.answer("pong")


async def cmd_report(message: types.Message) -> None:
    if not _is_admin(message.from_user.id):
        return
    events = await sheets.read("events")
    leads = await sheets.read("leads")
    coupons = await sheets.read("coupons")
    text = (
        "Отчет:\n"
        f"Событий: {len(events)}\n"
        f"Лидов: {len(leads)}\n"
        f"Купонов в таблице: {len(coupons)}"
    )
    await message.answer(text)


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_ping, commands=["ping"], state="*")
    dp.register_message_handler(cmd_report, commands=["report"], state="*")
