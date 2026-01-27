from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from app.config import is_admin_user


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
    return kb


def kb_main_menu(user_id: int | None = None, username: str | None = None) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(text="📞 Оставить контакт"))
    kb.add(KeyboardButton(text="🥐 Производственный интенсив"))
    if is_admin_user(user_id, username):
        kb.add(KeyboardButton(text="Админ-панель"))
    return kb


def kb_after_coupon(
    campaign: str,
    user_id: int | None = None,
    username: str | None = None,
) -> ReplyKeyboardMarkup:
    return kb_main_menu(user_id=user_id, username=username)


def kb_send_contact() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton(text="📞 Отправить номер", request_contact=True))
    kb.add(KeyboardButton(text="Отмена"))
    return kb
