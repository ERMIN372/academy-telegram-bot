from __future__ import annotations

import datetime as dt

from aiogram import Dispatcher, types

from app.keyboards.lottery import kb_lottery_result, kb_lottery_windows
from app.services import lottery as lottery_service, stats
from app.storage import db

RESULT_FOLLOW_UP = (
    "Нажмите кнопку, чтобы забрать подарок. Если понадобится помощь — оставьте контакт."
)

ENTRY_PROMPT = "Сегодня разыгрываем призы! Выберите одно из окошек 👇"


def _meta(user_id: int, campaign: str, username: str | None, extra: dict | None = None) -> dict:
    payload: dict = {}
    if extra:
        payload.update(extra)
    payload.setdefault("user_id", user_id)
    payload.setdefault("campaign", campaign or "default")
    if username:
        payload.setdefault("username", username)
    return payload


def _result_text(result: str, variant_index: int | None, repeat: bool = False) -> str:
    window_part = f"Окно №{(variant_index or 0) + 1} раскрыто!" if variant_index is not None else "Окно раскрыто!"
    if repeat:
        return (
            f"🔁 {window_part}\n"
            f"Ваш приз — <b>{result}</b>.\n\n"
            f"{RESULT_FOLLOW_UP}"
        )
    return (
        f"🎉 {window_part}\n"
        f"Ваш приз — <b>{result}</b>!\n\n"
        f"{RESULT_FOLLOW_UP}"
    )


def _cooldown_text(result: str, variant_index: int | None, until: dt.datetime) -> str:
    window_part = (
        f"Окно №{(variant_index or 0) + 1} уже раскрыто!"
        if variant_index is not None
        else "Окно уже раскрыто!"
    )
    return (
        f"🔁 {window_part}\n"
        f"Вы уже участвовали, вернёмся {until.strftime('%d.%m')}.\n"
        f"Ваш приз — <b>{result}</b>.\n\n"
        f"{RESULT_FOLLOW_UP}"
    )


async def present_lottery(
    message: types.Message,
    user_id: int,
    campaign: str,
    *,
    source: str,
    trigger: str,
    username: str | None = None,
) -> None:
    config = lottery_service.get_config()
    if not config.enabled or not config.results:
        from app.handlers.start import issue_coupon

        await issue_coupon(message, user_id, campaign)
        return

    already_has_coupon = await db.has_any_coupon(user_id)
    if already_has_coupon:
        await stats.log_event(
            user_id,
            campaign,
            "draw_blocked",
            _meta(
                user_id,
                campaign,
                username,
                {
                    "source": source,
                    "trigger": trigger,
                    "reason": "already_gifted",
                },
            ),
            username=username,
        )
        await message.answer(
            "Бонус уже был выдан при первом входе в бота. Повторное участие в лотерее недоступно."
        )
        return

    draw = await lottery_service.get_draw(user_id, campaign)
    if not draw:
        has_any_draw = await lottery_service.has_any_draw(user_id)
        if has_any_draw:
            await stats.log_event(
                user_id,
                campaign,
                "draw_blocked",
                _meta(
                    user_id,
                    campaign,
                    username,
                    {
                        "source": source,
                        "trigger": trigger,
                        "reason": "already_played",
                    },
                ),
                username=username,
            )
            await message.answer(
                "Вы уже участвовали в лотерее. Бонус выдаётся только один раз при первом входе в бота."
            )
            return
    if draw and not draw.is_claimed:
        await stats.log_event(
            user_id,
            campaign,
            "draw_entry",
            _meta(
                user_id,
                campaign,
                username,
                {
                    "source": source,
                    "trigger": trigger,
                    "status": "existing_unclaimed",
                },
            ),
            username=username,
        )
        await message.answer(
            _result_text(draw.result, draw.variant_index, repeat=True),
            reply_markup=kb_lottery_result(campaign),
        )
        await stats.log_event(
            user_id,
            campaign,
            "draw_repeat",
            _meta(
                user_id,
                campaign,
                username,
                {
                    "variant": draw.variant_index + 1
                    if draw.variant_index is not None
                    else None,
                    "result": draw.result,
                    "reason": "existing_unclaimed",
                },
            ),
            username=username,
        )
        return
    if draw and draw.is_claimed:
        await stats.log_event(
            user_id,
            campaign,
            "draw_blocked",
            _meta(
                user_id,
                campaign,
                username,
                {
                    "source": source,
                    "trigger": trigger,
                    "reason": "already_claimed",
                    "result": draw.result,
                },
            ),
            username=username,
        )
        await message.answer(
            f"Вы уже участвовали в лотерее. Ваш приз — <b>{draw.result}</b>.\n\n"
            "Бонус выдаётся только один раз при первом входе в бота."
        )
        return

    session = await lottery_service.create_session(user_id, campaign)
    variants = config.variants or len(config.results) or 1
    await stats.log_event(
        user_id,
        campaign,
        "draw_entry",
        _meta(
            user_id,
            campaign,
            username,
            {
                "source": source,
                "trigger": trigger,
                "status": "new",
                "variants": variants,
                "session": session.session_id,
            },
        ),
        username=username,
    )
    await message.answer(
        f"{ENTRY_PROMPT}\n\n{config.title}",
        reply_markup=kb_lottery_windows(session.session_id, variants, config.button_emoji),
    )


async def callback_lottery_pick(call: types.CallbackQuery) -> None:
    await call.answer()
    if not call.data:
        return
    parts = call.data.split(":", 2)
    if len(parts) != 3:
        return
    _, session_id, variant_raw = parts
    try:
        variant_index = int(variant_raw)
    except ValueError:
        return

    username = call.from_user.username if call.from_user else None
    session = await lottery_service.get_session(session_id)
    if session is None or session.user_id != call.from_user.id:
        await call.answer("Этот розыгрыш недоступен.", show_alert=True)
        return

    config = lottery_service.get_config()
    if not config.enabled or not config.results:
        from app.handlers.start import issue_coupon

        await issue_coupon(call.message, call.from_user.id, session.campaign)
        return

    already_has_coupon = await db.has_any_coupon(call.from_user.id)
    if already_has_coupon:
        await call.message.edit_text(
            "Бонус уже был выдан при первом входе в бота. Повторное участие в лотерее недоступно."
        )
        return

    existing_draw = await lottery_service.get_draw(call.from_user.id, session.campaign)
    if existing_draw:
        if not existing_draw.is_claimed:
            await call.message.edit_text(
                _result_text(existing_draw.result, existing_draw.variant_index, repeat=True),
                reply_markup=kb_lottery_result(session.campaign),
            )
        else:
            await call.message.edit_text(
                f"Вы уже участвовали в лотерее. Ваш приз — <b>{existing_draw.result}</b>.\n\n"
                "Бонус выдаётся только один раз при первом входе в бота."
            )
        return

    if session.status != "active":
        if session.status == "expired":
            await stats.log_event(
                call.from_user.id,
                session.campaign,
                "draw_expired",
                _meta(
                    call.from_user.id,
                    session.campaign,
                    username,
                    {"session": session.session_id},
                ),
                username=username,
            )
        draw = await lottery_service.get_draw(session.user_id, session.campaign)
        if draw:
            await call.message.edit_text(
                _result_text(draw.result, draw.variant_index, repeat=True),
                reply_markup=kb_lottery_result(session.campaign),
            )
        else:
            await call.message.edit_text("Розыгрыш завершён. Попробуйте запустить его снова.")
        return

    if not session.is_active:
        await call.message.edit_text("Время розыгрыша истекло. Попробуйте начать заново.")
        await stats.log_event(
            call.from_user.id,
            session.campaign,
            "draw_expired",
            _meta(
                call.from_user.id,
                session.campaign,
                username,
                {"session": session.session_id},
            ),
            username=username,
        )
        return

    await stats.log_event(
        call.from_user.id,
        session.campaign,
        "draw_choice",
        _meta(
            call.from_user.id,
            session.campaign,
            username,
            {"variant": variant_index + 1, "session": session.session_id},
        ),
        username=username,
    )

    result, result_index, weight = lottery_service.choose_result(config)
    coupon_campaign = config.coupon_map.get(result) or session.campaign
    draw = await lottery_service.store_result(session, variant_index, result, coupon_campaign)
    share = lottery_service.weight_share(config, weight)

    await stats.log_event(
        call.from_user.id,
        session.campaign,
        "draw_result",
        _meta(
            call.from_user.id,
            session.campaign,
            username,
            {
                "variant": variant_index + 1,
                "result": result,
                "session": session.session_id,
                "coupon_campaign": coupon_campaign,
                "weight": weight,
                "weight_share": share,
                "result_index": result_index + 1,
            },
        ),
        username=username,
    )

    await call.message.edit_text(
        _result_text(draw.result, draw.variant_index, repeat=False),
        reply_markup=kb_lottery_result(session.campaign),
    )


async def callback_lottery_claim(call: types.CallbackQuery) -> None:
    await call.answer()
    if not call.data:
        return
    parts = call.data.split(":", 1)
    if len(parts) != 2:
        return
    _, campaign = parts
    campaign = campaign or "default"

    username = call.from_user.username if call.from_user else None
    draw = await lottery_service.get_draw(call.from_user.id, campaign)
    if not draw:
        await call.message.answer(
            "Похоже, результат не найден. Попробуйте начать розыгрыш заново."
        )
        return

    already_has_coupon = await db.has_any_coupon(call.from_user.id)
    if already_has_coupon:
        await call.message.answer(
            "Подарок уже был выдан при первом входе в бота. Повторная выдача недоступна."
        )
        await lottery_service.mark_claimed(call.from_user.id, campaign)
        return

    coupon_campaign = draw.coupon_campaign or campaign
    from app.handlers.start import issue_coupon

    success = await issue_coupon(
        call.message,
        call.from_user.id,
        coupon_campaign,
        stats_campaign=campaign,
        no_coupons_message=(
            "Упс! Похоже, подарки этой категории временно закончились. "
            "Попробуйте выбрать другое окно завтра."
        ),
    )
    if success:
        await lottery_service.mark_claimed(call.from_user.id, campaign)


def register(dp: Dispatcher) -> None:
    dp.register_callback_query_handler(callback_lottery_pick, lambda c: c.data and c.data.startswith("lottery_pick:"))
    dp.register_callback_query_handler(callback_lottery_claim, lambda c: c.data and c.data.startswith("lottery_claim:"))
