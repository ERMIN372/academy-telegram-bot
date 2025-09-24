from __future__ import annotations

import logging

from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from app.config import get_settings
from app.handlers import admin, contacts, fun_interactive, lottery, start

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

settings = get_settings()
bot = Bot(token=settings.telegram_bot_token, parse_mode="HTML")
dp = Dispatcher(bot, storage=MemoryStorage())

start.register(dp)
contacts.register(dp)
lottery.register(dp)
fun_interactive.register(dp)
admin.register(dp)
