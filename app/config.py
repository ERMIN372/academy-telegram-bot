from __future__ import annotations

import base64
import json
from functools import cached_property, lru_cache
from typing import Any, Dict

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    telegram_bot_token: str = Field(alias="TELEGRAM_BOT_TOKEN")
    mode: str = Field(default="polling", alias="MODE")
    webhook_url: str | None = Field(default=None, alias="WEBHOOK_URL")
    secret_token: str = Field(alias="SECRET_TOKEN")
    admin_chat_id: int | None = Field(default=None, alias="ADMIN_CHAT_ID")
    channel_username: str = Field(alias="CHANNEL_USERNAME")
    google_sheets_id: str = Field(alias="GOOGLE_SHEETS_ID")
    google_service_json_b64: str = Field(alias="GOOGLE_SERVICE_JSON_B64")
    port: int = Field(default=8000, alias="PORT")
    leads_upsert: bool = Field(default=False, alias="LEADS_UPSERT")

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, value: str | None) -> str:
        normalized = (value or "polling").lower()
        if normalized not in {"polling", "webhook"}:
            return "polling"
        return normalized

    @field_validator("channel_username")
    @classmethod
    def normalize_channel_username(cls, value: str) -> str:
        username = value.strip()
        if not username.startswith("@"):
            username = f"@{username}"
        return username

    @field_validator("admin_chat_id", mode="before")
    @classmethod
    def normalize_admin_chat_id(cls, value: Any) -> int | None:
        if value in (None, "", 0, "0"):
            return None
        return int(value)

    @field_validator("leads_upsert", mode="before")
    @classmethod
    def normalize_leads_upsert(cls, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value in (None, ""):
            return False
        if isinstance(value, (int, float)):
            return bool(value)
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @cached_property
    def google_service_credentials(self) -> Dict[str, Any]:
        decoded = base64.b64decode(self.google_service_json_b64)
        return json.loads(decoded)


@lru_cache()
def get_settings() -> Settings:
    return Settings()
