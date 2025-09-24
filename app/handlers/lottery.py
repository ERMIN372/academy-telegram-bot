from __future__ import annotations

from aiogram import Dispatcher, types

from app.keyboards.lottery import kb_lottery_result, kb_lottery_windows
from app.services import lottery as lottery_service, stats

RESULT_FOLLOW_UP = (
    "Нажми кнопку, чтобы забрать подарок. Если понадобится помощь — оставь контакт."
)


def _result_text(result: str, variant_index: int | None, repeat: bool = False) -> str:
    window_part = f"Окно №{(variant_index or 0) + 1} раскрыто!" if variant_index is not None else "Окно раскрыто!"
    if repeat:
        return (
            f"🔁 {window_part}\n"
            f"Твой приз — <b>{result}</b>.\n\n"
            f"{RESULT_FOLLOW_UP}"
        )
    return (
        f"🎉 {window_part}\n"
        f"Твой приз — <b>{result}</b>!\n\n"
        f"{RESULT_FOLLOW_UP}"
    )


async def present_lottery(message: types.Message, user_id: int, campaign: str) -> None:
    config = lottery_service.get_config()
    if not config.enabled or not config.results:
        from app.handlers.start import issue_coupon

        await issue_coupon(message, user_id, campaign)
        return

    draw = await lottery_service.get_draw(user_id, campaign)
    if draw:
        if not draw.is_claimed:
            await message.answer(
                _result_text(draw.result, draw.variant_index, repeat=True),
                reply_markup=kb_lottery_result(campaign),
            )
            await stats.log_event(
                user_id,
                campaign,
                "draw_repeat",
                {"variant": draw.variant_index + 1 if draw.variant_index is not None else None, "result": draw.result},
            )
            return
        if lottery_service.is_cooldown_active(draw, config.cooldown_days):
            await message.answer(
                _result_text(draw.result, draw.variant_index, repeat=True),
                reply_markup=kb_lottery_result(campaign),
            )
            await stats.log_event(
                user_id,
                campaign,
                "draw_repeat",
                {"variant": draw.variant_index + 1 if draw.variant_index is not None else None, "result": draw.result},
            )
            return

    session = await lottery_service.create_session(user_id, campaign)
    variants = config.variants or len(config.results) or 1
    await stats.log_event(
        user_id,
        campaign,
        "draw_shown",
        {"variants": variants, "session": session.session_id},
    )
    await message.answer(
        config.title,
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

    session = await lottery_service.get_session(session_id)
    if session is None or session.user_id != call.from_user.id:
        await call.answer("Этот розыгрыш недоступен.", show_alert=True)
        return

    config = lottery_service.get_config()
    if not config.enabled or not config.results:
        from app.handlers.start import issue_coupon

        await issue_coupon(call.message, call.from_user.id, session.campaign)
        return

    if session.status != "active":
        if session.status == "expired":
            await stats.log_event(
                call.from_user.id,
                session.campaign,
                "draw_expired",
                {"session": session.session_id},
            )
        draw = await lottery_service.get_draw(session.user_id, session.campaign)
        if draw:
            await call.message.edit_text(
                _result_text(draw.result, draw.variant_index, repeat=True),
                reply_markup=kb_lottery_result(session.campaign),
            )
        else:
            await call.message.edit_text("Розыгрыш завершён. Попробуй запустить его снова.")
        return

    if not session.is_active:
        await call.message.edit_text("Время розыгрыша истекло. Попробуй начать заново.")
        await stats.log_event(
            call.from_user.id,
            session.campaign,
            "draw_expired",
            {"session": session.session_id},
        )
        return

    await stats.log_event(
        call.from_user.id,
        session.campaign,
        "draw_choice",
        {"variant": variant_index + 1, "session": session.session_id},
    )

    result = lottery_service.choose_result(config)
    coupon_campaign = config.coupon_map.get(result) or session.campaign
    draw = await lottery_service.store_result(session, variant_index, result, coupon_campaign)

    await stats.log_event(
        call.from_user.id,
        session.campaign,
        "draw_result",
        {
            "variant": variant_index + 1,
            "result": result,
            "session": session.session_id,
            "coupon_campaign": coupon_campaign,
        },
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

    draw = await lottery_service.get_draw(call.from_user.id, campaign)
    if not draw:
        await call.message.answer("Похоже, результат не найден. Попробуй начать розыгрыш заново.")
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
            "Попробуй выбрать другое окно завтра."
        ),
    )
    if success:
        await lottery_service.mark_claimed(call.from_user.id, campaign)


def register(dp: Dispatcher) -> None:
    dp.register_callback_query_handler(callback_lottery_pick, lambda c: c.data and c.data.startswith("lottery_pick:"))
    dp.register_callback_query_handler(callback_lottery_claim, lambda c: c.data and c.data.startswith("lottery_claim:"))
