from __future__ import annotations

import datetime as dt
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext

from app.config import get_settings
from app.keyboards.common import kb_subscribe
from app.keyboards.intensive import (
    kb_request_phone,
    qa_answer_keyboard,
    qa_menu_keyboard,
)
from app.services import alerts, phone, sheets, stats, sub_check
from app.storage import db
from app.utils import safe_text

logger = logging.getLogger(__name__)


LEAD_DUPLICATE_WINDOW_SECONDS = 60


@dataclass(frozen=True)
class QATopic:
    key: str
    button: str
    answer: str
    keywords: tuple[str, ...]


TOPICS: tuple[QATopic, ...] = (
    QATopic(
        key="about",
        button="📌 Что за интенсив",
        answer=(
            "2-дневная программа (10:00–18:00): экскурсии на две фабрики-кухни + "
            "разбор процессов и технологий с экспертами Академии. По итогу — сертификат."
        ),
        keywords=("что за", "интенсив", "формат", "описание"),
    ),
    QATopic(
        key="schedule",
        button="📅 Расписание",
        answer=(
            "Интенсив длится 2 дня, каждый день 10:00–18:00. Точные даты подтверждаем "
            "после записи — координатор свяжется."
        ),
        keywords=("расписание", "когда", "даты", "время", "график"),
    ),
    QATopic(
        key="program",
        button="🏭 Программа",
        answer=(
            "Экскурсии по цехам (мясной, рыбный, салаты/горячее, хлебо-булочный, "
            "кондитерский, фасовка); разбор процессов: приём сырья, контроль качества, "
            "упаковка/маркировка, логистика, зонирование, склад, обучение; выступления "
            "экспертов + Q&A."
        ),
        keywords=("программа", "что внутри", "контент", "чем занимаемся", "темы"),
    ),
    QATopic(
        key="location",
        button="📍 Где проходит",
        answer=(
            "Фабрики-кухни “Варшавка” (≈2800 м²; мясной/хлебо-булочный/кондитерский/"
            "кофейный; HACCP/ISO 22000) и “Волгоградский проспект” (≈2500 м²; "
            "мясной/рыбный/салаты/горячее/фасовка; HACCP/ISO 22000:2018). Плюс визит в "
            "Кампус Академии."
        ),
        keywords=("где", "адрес", "локация", "фабрика", "место"),
    ),
    QATopic(
        key="speakers",
        button="👩‍🔬 Спикеры и темы",
        answer=(
            "Спикеры: руководители и технологи фабрик-кухонь, эксперты по пищевой "
            "безопасности/качеству, лаборатория/НИОКР, инженеры по оборудованию и "
            "логистике. Темы: HACCP/ISO 22000; контроль качества и аллергены; выбор "
            "сырья/поставщиков; метрики и аналитика; постановка рецептур; механизация/"
            "оборудование; организация потоков/зонирование; сроки годности/упаковка; "
            "санитария/проф-химия; обучение персонала."
        ),
        keywords=("спикеры", "эксперты", "темы", "кто ведет", "кто будет"),
    ),
    QATopic(
        key="included",
        button="🍽 Что включено",
        answer="Экскурсии, выступления, материалы, питание (завтрак/обед/кофе-брейки) и сертификат.",
        keywords=("что включено", "еда", "питание", "перерыв", "что входит"),
    ),
    QATopic(
        key="price",
        button="💳 Стоимость",
        answer=(
            "Ближайшие потоки: октябрь и ноябрь. Стоимость: октябрь — 35 000 ₽, ноябрь — 40 000 ₽. "
            "Точные даты и оплата подтверждаются после записи (ссылка на e-mail)."
        ),
        keywords=("стоимость", "цена", "сколько", "прайс", "оплата"),
    ),
    QATopic(
        key="certificate",
        button="🎓 Сертификат",
        answer="Да, по итогам обучения выдаём сертификат Академии.",
        keywords=("сертификат", "диплом", "документ"),
    ),
    QATopic(
        key="lead",
        button="📝 Записаться",
        answer="Оставьте номер — свяжемся, уточним даты и пришлём ссылку на оплату.",
        keywords=("записаться", "заявка", "оставить номер", "регистрация", "связаться"),
    ),
)

TOPIC_BY_KEY: Dict[str, QATopic] = {topic.key: topic for topic in TOPICS}


MEDIA_DIR = Path(__file__).resolve().parents[2]

TOPIC_PHOTOS: Dict[str, Path] = {
    "schedule": MEDIA_DIR / "14.jpg",
    "certificate": MEDIA_DIR / "11.png.webp",
    "included": MEDIA_DIR / "10.jpg.webp",
    "speakers": MEDIA_DIR / "9.jpg.webp",
    "program": MEDIA_DIR / "12.jpg",
    "location": MEDIA_DIR / "13.jpg",
}


def _topics_for_keyboard() -> list[tuple[str, str]]:
    return [(topic.key, topic.button) for topic in TOPICS]


def _match_topic(text: str) -> Optional[QATopic]:
    normalized = safe_text(text).lower()
    if not normalized:
        return None
    for topic in TOPICS:
        if topic.button.lower() == normalized:
            return topic
        for keyword in topic.keywords:
            if keyword in normalized:
                return topic
    return None


async def cmd_intensive(message: types.Message, state: FSMContext) -> None:
    args = safe_text(message.get_args()) or "default"
    campaign = args or "default"
    await state.update_data(campaign=campaign)
    intensive_state = {
        "campaign": campaign,
        "sub_ok": False,
        "qa_last_response": 0.0,
    }
    await state.update_data(intensive=intensive_state)

    settings = get_settings()
    subscribe_markup = kb_subscribe(
        f"https://t.me/{settings.channel_username.lstrip('@')}"
    )
    await message.answer(
        "Привет! Это производственный интенсив Академии Нефтьмагистраль и Караваевы. Подписывайтесь на канал, "
        "а я подскажу детали и помогу записаться.",
        reply_markup=subscribe_markup,
    )

    is_member = await sub_check.is_member(message.bot, message.from_user.id)
    if is_member:
        intensive_state["sub_ok"] = True
        intensive_state["sub_confirmed_at"] = time.time()
        await state.update_data(intensive=intensive_state)
        await _show_menu(message, campaign, source="command")
    else:
        await _prompt_check_subscription(message.chat.id, campaign, message.bot)


async def _prompt_check_subscription(chat_id: int, campaign: str, bot: Bot) -> None:
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton(
            text="✅ Проверить подписку",
            callback_data=f"intensive_check_sub:{safe_text(campaign) or 'default'}",
        )
    )
    await bot.send_message(
        chat_id,
        "Когда подпишетесь, нажмите «Проверить подписку».",
        reply_markup=markup,
    )



async def callback_check_sub(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = "default"
    if call.data and ":" in call.data:
        campaign = safe_text(call.data.split(":", 1)[1]) or "default"
    await state.update_data(campaign=campaign)
    is_member = await sub_check.is_member(call.bot, call.from_user.id)
    data = await state.get_data()
    intensive_state = data.get("intensive") or {"campaign": campaign}
    intensive_state["campaign"] = campaign
    username = safe_text(call.from_user.username) or None
    if is_member:
        intensive_state["sub_ok"] = True
        intensive_state["sub_confirmed_at"] = time.time()
        await state.update_data(intensive=intensive_state)
        await stats.log_event(
            call.from_user.id,
            campaign,
            "sub_ok",
            {"source": "intensive"},
            username=username,
        )
        await call.message.answer("Спасибо! Вы в списке канала 👌")
        await _show_menu(call.message, campaign, source="menu_button")
    else:
        intensive_state["sub_ok"] = False
        await state.update_data(intensive=intensive_state)
        await call.message.answer(
            "Похоже, подписка ещё не оформлена. Подписывайтесь и жмите кнопку снова."
        )


async def _show_menu(message: types.Message, campaign: str, *, source: str) -> None:
    settings = get_settings()
    username = safe_text(message.from_user.username) or None
    if settings.qa_buttons_shown:
        keyboard = qa_menu_keyboard(campaign, _topics_for_keyboard())
    else:
        keyboard = None
    text = "Что рассказать про интенсив? Выберите тему или задайте вопрос."
    await message.answer(text, reply_markup=keyboard)
    if settings.qa_enabled:
        await stats.log_event(
            message.from_user.id,
            campaign,
            "qa_entry",
            {"source": source},
            username=username,
        )


def _get_intensive_state(data: Dict[str, object]) -> Dict[str, object]:
    state = data.get("intensive")
    if isinstance(state, dict):
        return state
    return {}


def _rate_limit_ok(state: Dict[str, object]) -> bool:
    settings = get_settings()
    cooldown = settings.qa_rate_limit_seconds
    if cooldown <= 0:
        return True
    last = float(state.get("qa_last_response") or 0.0)
    now = time.time()
    return now - last >= cooldown


async def callback_topic(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    settings = get_settings()
    if not settings.qa_enabled:
        return
    campaign = "default"
    topic_key = "about"
    if call.data:
        parts = call.data.split(":", 2)
        if len(parts) >= 3:
            campaign = safe_text(parts[1]) or "default"
            topic_key = safe_text(parts[2]) or "about"
    data = await state.get_data()
    intensive_state = _get_intensive_state(data)
    if not _rate_limit_ok(intensive_state):
        return
    topic = TOPIC_BY_KEY.get(topic_key)
    if not topic:
        return
    username = safe_text(call.from_user.username) or None
    await stats.log_event(
        call.from_user.id,
        campaign,
        "qa_question",
        {"topic": topic.key, "source": "button"},
        username=username,
    )
    if topic.key == "lead":
        intensive_state["last_topic"] = topic.key
        await state.update_data(intensive=intensive_state)
        await stats.log_event(
            call.from_user.id,
            campaign,
            "qa_clicked_cta",
            {"source": "menu_button", "topic": topic.key},
            username=username,
        )
        await _start_lead_flow(
            call.message, state, campaign, call.from_user, source="menu_button"
        )
        return

    await _send_topic_answer(call.message, topic, campaign)
    intensive_state["qa_last_response"] = time.time()
    intensive_state["last_topic"] = topic.key
    await state.update_data(intensive=intensive_state)
    await stats.log_event(
        call.from_user.id,
        campaign,
        "qa_answered",
        {"topic": topic.key},
        username=username,
    )


def _answer_with_cta(text: str) -> str:
    return f"{text}\n\nГотовы присоединиться? Жмите «📝 Записаться»."


def _photo_for_topic(topic_key: str) -> Optional[Path]:
    path = TOPIC_PHOTOS.get(topic_key)
    if path and path.exists():
        return path
    return None


async def _send_topic_answer(
    message: types.Message, topic: QATopic, campaign: str
) -> None:
    reply_markup = qa_answer_keyboard(campaign)
    photo_path = _photo_for_topic(topic.key)
    text = _answer_with_cta(topic.answer)
    if photo_path:
        await message.answer_photo(
            types.InputFile(str(photo_path)),
            caption=text,
            reply_markup=reply_markup,
        )
        return
    await message.answer(text, reply_markup=reply_markup)


async def callback_menu(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = "default"
    if call.data and ":" in call.data:
        campaign = safe_text(call.data.split(":", 1)[1]) or "default"
    await _show_menu(call.message, campaign, source="back_button")
    data = await state.get_data()
    intensive_state = _get_intensive_state(data)
    intensive_state["qa_last_response"] = time.time()
    intensive_state["last_topic"] = None
    await state.update_data(intensive=intensive_state)


async def qa_text_handler(message: types.Message, state: FSMContext) -> None:
    if not message.text or message.text.startswith("/"):
        return
    data = await state.get_data()
    intensive_state = _get_intensive_state(data)
    if not intensive_state:
        return
    campaign = safe_text(intensive_state.get("campaign")) or "default"
    settings = get_settings()
    username = safe_text(message.from_user.username) or None

    if message.text.lower() in {"назад", "меню"}:
        await _show_menu(message, campaign, source="text_back")
        intensive_state["qa_last_response"] = time.time()
        await state.update_data(intensive=intensive_state)
        return

    topic = _match_topic(message.text) if settings.qa_enabled else None
    if topic:
        if not _rate_limit_ok(intensive_state):
            return
        await stats.log_event(
            message.from_user.id,
            campaign,
            "qa_question",
            {
                "topic": topic.key,
                "source": "text",
                "raw_text": message.text,
            },
            username=username,
        )
        if topic.key == "lead":
            intensive_state["last_topic"] = topic.key
            await state.update_data(intensive=intensive_state)
            await stats.log_event(
                message.from_user.id,
                campaign,
                "qa_clicked_cta",
                {"source": "text_intent", "topic": topic.key},
                username=username,
            )
            await _start_lead_flow(
                message,
                state,
                campaign,
                message.from_user,
                source="text_intent",
            )
            return
        await _send_topic_answer(message, topic, campaign)
        intensive_state["qa_last_response"] = time.time()
        intensive_state["last_topic"] = topic.key
        await state.update_data(intensive=intensive_state)
        await stats.log_event(
            message.from_user.id,
            campaign,
            "qa_answered",
            {"topic": topic.key},
            username=username,
        )
        return

    await stats.log_event(
        message.from_user.id,
        campaign,
        "qa_unknown",
        {"raw_text": message.text},
        username=username,
    )
    intensive_state["qa_last_response"] = time.time()
    intensive_state["last_topic"] = None
    await state.update_data(intensive=intensive_state)
    if settings.qa_fallback_to_menu:
        if settings.qa_buttons_shown:
            await message.answer(
                settings.qa_unknown_answer,
                reply_markup=qa_menu_keyboard(campaign, _topics_for_keyboard()),
            )
        else:
            await message.answer(settings.qa_unknown_answer)
    else:
        await message.answer(
            "Не нашёл ответ 🤔. Могу записать? Нажмите «📝 Записаться».",
            reply_markup=qa_answer_keyboard(campaign),
        )


async def callback_lead(call: types.CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    campaign = "default"
    if call.data and ":" in call.data:
        campaign = safe_text(call.data.split(":", 1)[1]) or "default"
    data = await state.get_data()
    intensive_state = _get_intensive_state(data)
    last_topic = safe_text(intensive_state.get("last_topic")) or None
    await stats.log_event(
        call.from_user.id,
        campaign,
        "qa_clicked_cta",
        {"source": "cta_button", "topic": last_topic},
        username=safe_text(call.from_user.username) or None,
    )
    await _start_lead_flow(
        call.message, state, campaign, call.from_user, source="cta_button"
    )


async def _start_lead_flow(
    message: types.Message,
    state: FSMContext,
    campaign: str,
    user: types.User,
    *,
    source: str,
) -> None:
    data = await state.get_data()
    intensive_state = _get_intensive_state(data)
    if not intensive_state.get("sub_ok"):
        await message.answer(
            "Сначала подтвердите подписку через кнопку «Проверить подписку»."
        )
        return

    existing = await db.get_lead(user.id, campaign)
    if existing:
        created_at = existing.get("created_at")
        if created_at:
            try:
                dt_created = dt.datetime.fromisoformat(created_at)
            except ValueError:
                dt_created = None
            if dt_created and (
                dt.datetime.utcnow() - dt_created
            ).total_seconds() < LEAD_DUPLICATE_WINDOW_SECONDS:
                await message.answer("Заявка уже принята 👍")
                await stats.log_event(
                    user.id,
                    campaign,
                    "lead_duplicate",
                    {"window_seconds": LEAD_DUPLICATE_WINDOW_SECONDS},
                    username=safe_text(user.username) or None,
                )
                return

    await state.update_data(
        lead_context={
            "flow": "intensive",
            "campaign": campaign,
            "started_at": dt.datetime.utcnow().isoformat(),
        }
    )
    await stats.log_event(
        user.id,
        campaign,
        "lead_init",
        {"source": "intensive", "trigger": source},
        username=safe_text(user.username) or None,
    )
    await message.answer(
        "Оставьте номер — свяжемся, уточним даты и пришлём ссылку на оплату.",
        reply_markup=kb_request_phone(),
    )
    await message.answer("Можно отправить контактом или написать +7XXXXXXXXXX.")


async def process_lead_message(
    message: types.Message, state: FSMContext, context: Dict[str, object]
) -> None:
    campaign = safe_text(context.get("campaign")) or "default"
    if message.text and message.text.lower() == "отмена":
        await message.answer(
            "Отменено.", reply_markup=types.ReplyKeyboardRemove()
        )
        await state.update_data(lead_context=None)
        return

    if message.text and message.text.lower() == "ввести номер вручную":
        await message.answer("Введите номер в формате +7XXXXXXXXXX.")
        return

    phone_source = "manual"
    contact_phone: Optional[str] = None
    if message.contact and message.contact.phone_number:
        contact_phone = message.contact.phone_number
        phone_source = "contact"
    elif message.text:
        contact_phone = message.text

    if not contact_phone:
        await message.answer(
            "Пожалуйста, пришлите номер телефона или нажмите «Отмена».",
            reply_markup=kb_request_phone(),
        )
        return

    normalized_phone = phone.normalize(contact_phone)
    if not normalized_phone:
        await message.answer(
            "Не удалось распознать номер. Попробуйте формат +7XXXXXXXXXX."
        )
        return

    await state.update_data(lead_context=None)

    raw_username = message.from_user.username or ""
    normalized_username = (
        f"@{raw_username.lstrip('@').lower()}" if raw_username else ""
    )

    await stats.log_event(
        message.from_user.id,
        campaign,
        "phone_received",
        {"method": phone_source},
        username=normalized_username or None,
    )

    timestamp = sheets.current_timestamp()
    lead_payload = {
        "user_id": message.from_user.id,
        "username": normalized_username,
        "phone": normalized_phone,
        "campaign": campaign,
        "created_at": timestamp.utc_text,
        "created_at_msk": timestamp.local_text,
        "status": "new",
    }

    settings = get_settings()
    sheet_error: Optional[str] = None
    operation = "append"
    try:
        if settings.leads_upsert:
            leads = await sheets.read("leads")
            existing = next(
                (
                    item
                    for item in leads
                    if str(item.get("user_id")) == str(message.from_user.id)
                    and safe_text(item.get("campaign")) == campaign
                ),
                None,
            )
            if existing:
                operation = "update"
                await sheets.update_row(
                    "leads",
                    existing["row"],
                    {
                        "phone": normalized_phone,
                        "username": normalized_username,
                        "updated_at": dt.datetime.utcnow().isoformat(),
                    },
                    optional_headers=["updated_at_msk"],
                    meta=timestamp.meta,
                )
            else:
                await sheets.append(
                    "leads",
                    lead_payload,
                    optional_headers=["created_at_msk"],
                    meta=timestamp.meta,
                )
        else:
            await sheets.append(
                "leads",
                lead_payload,
                optional_headers=["created_at_msk"],
                meta=timestamp.meta,
            )
        await stats.log_event(
            message.from_user.id,
            campaign,
            "sheets_write_ok",
            {"sheet": "leads", "operation": operation},
            username=normalized_username or None,
        )
    except Exception:  # pragma: no cover - defensive
        sheet_error = "DB_WRITE_FAILED"
        logger.exception("Failed to write lead to Google Sheets")
        await stats.log_event(
            message.from_user.id,
            campaign,
            "sheets_write_failed",
            {"sheet": "leads", "operation": operation, "error": sheet_error},
            username=normalized_username or None,
        )

    await db.upsert_lead(message.from_user.id, campaign)

    await stats.log_event(
        message.from_user.id,
        campaign,
        "lead",
        {"username": normalized_username, "phone_saved": sheet_error is None},
        username=normalized_username or None,
    )
    await message.answer(
        "Спасибо! Свяжемся, уточним точные даты и оплату 👌",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    await alerts.notify_new_lead(
        message.bot,
        user_id=message.from_user.id,
        username=normalized_username or None,
        phone=normalized_phone,
        campaign=campaign,
        created_at=timestamp.moment,
        title="🆕 Новый лид (Интенсив)",
        error=sheet_error,
    )


def register(dp: Dispatcher) -> None:
    dp.register_message_handler(cmd_intensive, commands=["intensive"], state="*")
    dp.register_callback_query_handler(
        callback_check_sub,
        lambda c: c.data and c.data.startswith("intensive_check_sub:"),
    )
    dp.register_callback_query_handler(
        callback_topic, lambda c: c.data and c.data.startswith("qa_topic:")
    )
    dp.register_callback_query_handler(
        callback_menu, lambda c: c.data and c.data.startswith("qa_menu:")
    )
    dp.register_callback_query_handler(
        callback_lead, lambda c: c.data and c.data.startswith("intensive_lead:")
    )
    dp.register_message_handler(qa_text_handler, content_types=["text"], state="*")
