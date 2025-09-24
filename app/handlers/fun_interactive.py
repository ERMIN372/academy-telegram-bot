from __future__ import annotations

import random

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.keyboards.common import kb_get_gift
from app.services import stats
from app.services.deep_link import parse_start_payload

FORTUNES = [
    "Сегодня тебя ждет полезный инсайт на вебинаре!",
    "Наставник уже готов поделиться секретами успеха.",
    "Ты встретишь людей, которые помогут в развитии.",
    "Твой проект сделает рывок благодаря новым знаниям.",
]


async def cmd_fortune(message: types.Message, state: FSMContext) -> None:
    campaign = parse_start_payload(message.text)
    await state.update_data(campaign=campaign)
    fortune = random.choice(FORTUNES)
    username = message.from_user.username if message.from_user else None
    await stats.log_event(
        message.from_user.id,
        campaign,
        "fortune",
        {"fortune": fortune},
        username=username,
    )
    await message.answer(
        f"🔮 {fortune}\nГотов забрать подарок?",
        reply_markup=kb_get_gift(campaign),
    )


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_fortune, commands=["fortune"], state="*")
