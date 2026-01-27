from __future__ import annotations

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from app.utils import safe_text


def qa_topics_keyboard(
    campaign: str, topics: list[tuple[str, str]]
) -> InlineKeyboardMarkup:
    campaign_value = safe_text(campaign) or "default"
    markup = InlineKeyboardMarkup(row_width=1)
    for topic_key, label in topics:
        safe_key = safe_text(topic_key) or "info"
        markup.add(
            InlineKeyboardButton(
                text=label,
                callback_data=f"qa_topic:{campaign_value}:{safe_key}",
            )
        )
    return markup


def qa_answer_keyboard(campaign: str) -> InlineKeyboardMarkup:
    campaign_value = safe_text(campaign) or "default"
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(
            text="📝Записаться",
            callback_data=f"intensive_lead:{campaign_value}",
        )
    )
    markup.add(
        InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=f"qa_menu:{campaign_value}",
        )
    )
    return markup


def qa_menu_keyboard(campaign: str, topics: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    return qa_topics_keyboard(campaign, topics)


def kb_request_phone() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton(text="📱 Отправить телефон", request_contact=True))
    keyboard.add(KeyboardButton(text="Ввести номер вручную"))
    keyboard.add(KeyboardButton(text="Отмена"))
    return keyboard
