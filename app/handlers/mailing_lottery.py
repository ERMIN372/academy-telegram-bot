from __future__ import annotations

import random

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.handlers.start import issue_coupon
from app.services import stats
from app.services.deep_link import parse_start_payload

WINDOWS = [
    ("Выигрыш!", 3),
    ("Дополнительный бонус", 2),
    ("Супер подарок", 1),
    ("Попробуй снова", 2),
    ("Подарок в кармане", 3),
    ("Секретное окно", 1),
]


def _choose_window() -> str:
    total = sum(weight for _, weight in WINDOWS)
    rnd = random.randint(1, total)
    upto = 0
    for label, weight in WINDOWS:
        upto += weight
        if rnd <= upto:
            return label
    return WINDOWS[-1][0]


async def cmd_lottery(message: types.Message, state: FSMContext) -> None:
    campaign = parse_start_payload(message.text)
    await state.update_data(campaign=campaign)
    window = _choose_window()
    await stats.log_event(message.from_user.id, campaign, "lottery_play", {"result": window})
    await message.answer(f"Ты открыл окно: {window}! Забирай подарок.")
    await issue_coupon(message, message.from_user.id, campaign)


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_lottery, commands=["lottery"], state="*")
