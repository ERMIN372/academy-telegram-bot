from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup


def kb_subscribe(url: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(text="📣 Подписаться", url=url))
    return kb


def kb_check_sub(campaign: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(text="✅ Проверить подписку", callback_data=f"check_sub:{campaign}"))
    return kb


def kb_get_gift(campaign: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(text="🎁 Забрать подарок", callback_data=f"get_gift:{campaign}"))
    kb.add(InlineKeyboardButton(text="📞 Оставить контакт", callback_data=f"leave_phone:{campaign}"))
    return kb


def kb_after_coupon(campaign: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(text="📞 Оставить контакт", callback_data=f"leave_phone:{campaign}"))
    return kb


def kb_send_contact() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton(text="📞 Отправить номер", request_contact=True))
    kb.add(KeyboardButton(text="Отмена"))
    return kb
