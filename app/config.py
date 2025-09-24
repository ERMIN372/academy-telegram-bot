from __future__ import annotations

import base64
import json
from functools import lru_cache
from typing import Any, Dict

from pydantic import BaseSettings, Field, validator


class Settings(BaseSettings):
    telegram_bot_token: str = Field(..., env="TELEGRAM_BOT_TOKEN")
    mode: str = Field("polling", env="MODE")
    webhook_url: str | None = Field(None, env="WEBHOOK_URL")
    secret_token: str = Field(..., env="SECRET_TOKEN")
    admin_chat_id: int = Field(0, env="ADMIN_CHAT_ID")
    channel_username: str = Field(..., env="CHANNEL_USERNAME")
    google_sheets_id: str = Field(..., env="GOOGLE_SHEETS_ID")
    google_service_json_b64: str = Field(..., env="GOOGLE_SERVICE_JSON_B64")
    port: int = Field(8000, env="PORT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @validator("mode")
    def validate_mode(cls, value: str) -> str:
        value = value.lower()
        if value not in {"polling", "webhook"}:
            return "polling"
        return value

    def google_service_credentials(self) -> Dict[str, Any]:
        data = base64.b64decode(self.google_service_json_b64)
        return json.loads(data)


@lru_cache()
def get_settings() -> Settings:
    return Settings()
