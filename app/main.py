from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from aiogram import types
from aiogram.utils import executor
import uvicorn

from app.bot import bot, dp
from app.config import get_settings

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


def run() -> None:
    settings = get_settings()
    if settings.mode == "webhook":
        asyncio.run(setup_webhook())
        uvicorn.run(app, host="0.0.0.0", port=settings.port)
    else:
        asyncio.run(drop_webhook())
        executor.start_polling(dp, skip_updates=True)


if __name__ == "__main__":
    run()
