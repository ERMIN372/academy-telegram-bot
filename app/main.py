from __future__ import annotations

import asyncio
import logging

from aiogram import Dispatcher, types
from aiogram.utils import executor
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn

from app.bot import bot, dp
from app.config import get_settings
from app.services import reminders

logger = logging.getLogger(__name__)

app = FastAPI()


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"ok": True})


@app.post("/tg/webhook")
async def tg_webhook(request: Request) -> JSONResponse:
    settings = get_settings()
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret != settings.secret_token:
        raise HTTPException(status_code=403, detail="Forbidden")
    data = await request.json()
    update = types.Update(**data)
    await dp.process_update(update)
    return JSONResponse({"ok": True})


@app.on_event("startup")
async def on_app_startup() -> None:
    await reminders.on_startup(bot)


@app.on_event("shutdown")
async def on_app_shutdown() -> None:
    await reminders.on_shutdown()


async def setup_webhook() -> None:
    settings = get_settings()
    await bot.delete_webhook(drop_pending_updates=True)
    if not settings.webhook_url:
        raise RuntimeError("WEBHOOK_URL is not configured")
    await bot.set_webhook(
        url=f"{settings.webhook_url}/tg/webhook",
        secret_token=settings.secret_token,
        allowed_updates=["message", "callback_query", "chat_member"],
    )
    logger.info("Webhook set to %s", settings.webhook_url)


async def drop_webhook() -> None:
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Webhook dropped; switching to polling")


async def _on_polling_startup(dp: Dispatcher) -> None:
    await reminders.on_startup(dp.bot)


async def _on_polling_shutdown(dp: Dispatcher) -> None:
    await reminders.on_shutdown()


def run() -> None:
    settings = get_settings()
    if settings.mode == "webhook":
        asyncio.run(setup_webhook())
        uvicorn.run(app, host="0.0.0.0", port=settings.port)
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(drop_webhook())
        try:
            executor.start_polling(
                dp,
                skip_updates=False,
                reset_webhook=False,
                loop=loop,
                on_startup=[_on_polling_startup],
                on_shutdown=[_on_polling_shutdown],
            )
        finally:
            loop.run_until_complete(dp.storage.close())
            loop.run_until_complete(dp.storage.wait_closed())
            loop.close()


if __name__ == "__main__":
    run()
