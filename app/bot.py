from __future__ import annotations

import logging

from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from app.config import get_settings
from app.handlers import admin, contacts, fun_interactive, intensive, lottery, start
from app.services import alerts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

settings = get_settings()
bot = Bot(token=settings.telegram_bot_token, parse_mode="HTML")
dp = Dispatcher(bot, storage=MemoryStorage())

admin.register(dp)
start.register(dp)
intensive.register(dp)
contacts.register(dp)
lottery.register(dp)
fun_interactive.register(dp)
alerts.setup_error_handler(dp)
