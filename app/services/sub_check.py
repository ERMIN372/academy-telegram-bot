from __future__ import annotations

from aiogram import Bot
from aiogram.utils import exceptions

from app.config import get_settings


async def is_member(bot: Bot, user_id: int) -> bool:
    settings = get_settings()
    try:
        member = await bot.get_chat_member(settings.channel_username, user_id)
    except exceptions.TelegramAPIError:
        return False
    return member.status in {"member", "administrator", "creator"}
