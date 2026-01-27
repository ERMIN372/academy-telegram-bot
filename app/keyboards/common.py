from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)


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


def kb_main_menu(is_admin: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(text="📞 Оставить контакт"))
    kb.add(KeyboardButton(text="🥐 Производственный интенсив"))
    if is_admin:
        kb.add(KeyboardButton(text="⚙️ Админ панель"))
    return kb


def kb_after_coupon(campaign: str, is_admin: bool = False) -> ReplyKeyboardMarkup:
    return kb_main_menu(is_admin=is_admin)


def kb_send_contact() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton(text="📞 Отправить номер", request_contact=True))
    kb.add(KeyboardButton(text="Отмена"))
    return kb


def kb_admin_panel() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(text="➕ Добавить купон", callback_data="admin:add_coupon"))
    kb.add(InlineKeyboardButton(text="➕ Добавить несколько купонов", callback_data="admin:add_bulk"))
    kb.add(InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"))
    kb.add(InlineKeyboardButton(text="❌ Закрыть", callback_data="admin:close"))
    return kb


def kb_cancel_admin() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton(text="❌ Отмена"))
    return kb
