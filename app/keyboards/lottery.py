from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def kb_lottery_windows(session_id: str, variants: int, emoji: str) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=3)
    variants = max(1, variants)
    for index in range(variants):
        button_text = f"{emoji} {index + 1}"
        callback_data = f"lottery_pick:{session_id}:{index}"
        markup.insert(InlineKeyboardButton(text=button_text, callback_data=callback_data))
    return markup


def kb_lottery_result(campaign: str) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton(text="🎁 Забрать подарок", callback_data=f"lottery_claim:{campaign}"))
    markup.add(InlineKeyboardButton(text="📞 Оставить контакт", callback_data=f"leave_phone:{campaign}"))
    return markup
