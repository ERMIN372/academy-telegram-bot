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
    secret_token: str = Field(default="change_me_secret_token", alias="SECRET_TOKEN")
    admin_chat_id_raw: str | None = Field(default=None, alias="ADMIN_CHAT_ID")
    channel_username: str = Field(default="@change_me", alias="CHANNEL_USERNAME")
    google_sheets_id: str = Field(default="", alias="GOOGLE_SHEETS_ID")
    google_service_json_b64: str = Field(default="e30=", alias="GOOGLE_SERVICE_JSON_B64")
    sheets_tz: str = Field(default="Europe/Moscow", alias="SHEETS_TZ")
    sheets_time_format: str = Field(
        default="%Y-%m-%d %H:%M:%S", alias="SHEETS_TIME_FORMAT"
    )
    port: int = Field(default=8000, alias="PORT")
    leads_upsert: bool = Field(default=False, alias="LEADS_UPSERT")
    reminder_enabled: bool = Field(default=False, alias="REMINDER_ENABLED")
    reminder_delay_hours: int = Field(default=6, alias="REMINDER_DELAY_HOURS")
    reminder_only_if_no_lead: bool = Field(default=True, alias="REMINDER_ONLY_IF_NO_LEAD")
    reminder_only_if_not_used: bool = Field(default=True, alias="REMINDER_ONLY_IF_NOT_USED")
    reminder_work_hours: tuple[int, int] = Field(default=(10, 20), alias="REMINDER_WORK_HOURS")
    reminder_text: str = Field(
        default=(
            "Хэй! Подарок всё ещё ждёт 😊 Используй код <b>{code}</b>, "
            "чтобы забрать скидку. Нужна подсказка? Нажми «Оставить контакт»."
        ),
        alias="REMINDER_TEXT",
    )
    reminder_max_per_user: int = Field(default=1, alias="REMINDER_MAX_PER_USER")
    reminder_timezone: str = Field(default="Europe/Moscow", alias="REMINDER_TIMEZONE")

    lottery_enabled: bool = Field(default=False, alias="LOTTERY_ENABLED")
    lottery_variants: int = Field(default=0, alias="LOTTERY_VARIANTS")
    lottery_weights: list[float] = Field(default_factory=list, alias="LOTTERY_WEIGHTS")
    lottery_results: list[str] = Field(default_factory=list, alias="LOTTERY_RESULTS")
    lottery_coupon_campaign_map: Dict[str, str] = Field(
        default_factory=dict, alias="LOTTERY_COUPON_CAMPAIGN_MAP"
    )
    lottery_cooldown_days: int = Field(default=1, alias="LOTTERY_COOLDOWN_DAYS")
    lottery_title: str = Field(default="Выбирай окно 🎁", alias="LOTTERY_TITLE")
    lottery_button_emoji: str = Field(default="🎯", alias="LOTTERY_BUTTON_EMOJI")
    lottery_button_label: str = Field(default="🎲 Лотерея", alias="LOTTERY_BTN_LABEL")
    draw_prefix: str = Field(default="draw_", alias="DRAW_PREFIX")
    lottery_ab_test: bool = Field(default=False, alias="LOTTERY_A_B_TEST")

    alerts_enabled: bool = Field(default=False, alias="ALERTS_ENABLED")
    alerts_mention: str | None = Field(default=None, alias="ALERTS_MENTION")
    alerts_rate_limit: int = Field(default=30, alias="ALERTS_RATE_LIMIT")
    alerts_bundle_window: int = Field(default=60, alias="ALERTS_BUNDLE_WINDOW")
    alerts_mask_phone: bool = Field(default=True, alias="ALERTS_MASK_PHONE")
    alerts_timezone: str = Field(default="Europe/Moscow", alias="ALERTS_TZ")
    alerts_time_format: str = Field(
        default="%Y-%m-%d %H:%M:%S", alias="ALERTS_TIME_FORMAT"
    )
    qa_enabled: bool = Field(default=False, alias="QA_ENABLED")
    qa_fallback_to_menu: bool = Field(default=True, alias="QA_FALLBACK_TO_MENU")
    qa_unknown_answer: str = Field(
        default=(
            "Не нашёл ответ 🤔. Выберите тему ниже или нажмите «📝 Записаться»."
        ),
        alias="QA_UNKNOWN_ANSWER",
    )
    qa_rate_limit_seconds: int = Field(default=2, alias="QA_RATE_LIMIT_SECONDS")
    qa_buttons_shown: bool = Field(default=True, alias="QA_BUTTONS_SHOWN")

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

    @cached_property
    def admin_chat_ids(self) -> tuple[int, ...]:
        raw_value = self.admin_chat_id_raw
        if not raw_value:
            return ()
        if isinstance(raw_value, (list, tuple)):
            values = [str(item).strip() for item in raw_value]
        else:
            values = [part.strip() for part in str(raw_value).replace(";", ",").split(",")]
        result: list[int] = []
        for item in values:
            if not item:
                continue
            if item.startswith("chat:") or item.startswith("user:"):
                item = item.split(":", 1)[1].strip()
            if not item or item in {"0", "-0"}:
                continue
            try:
                result.append(int(item))
            except ValueError:
                continue
        seen: set[int] = set()
        unique: list[int] = []
        for value in result:
            if value in seen:
                continue
            seen.add(value)
            unique.append(value)
        return tuple(unique)

    @property
    def admin_chat_id(self) -> int | None:
        ids = self.admin_chat_ids
        if not ids:
            return None
        return ids[0]

    @staticmethod
    def _parse_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value in (None, ""):
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @field_validator("leads_upsert", mode="before")
    @classmethod
    def parse_leads_upsert(cls, value: Any) -> bool:
        return cls._parse_bool(value)

    @field_validator("alerts_enabled", mode="before")
    @classmethod
    def parse_alerts_enabled(cls, value: Any) -> bool:
        return cls._parse_bool(value)

    @field_validator("alerts_mask_phone", mode="before")
    @classmethod
    def parse_alerts_mask_phone(cls, value: Any) -> bool:
        return cls._parse_bool(value, default=True)

    @field_validator("qa_enabled", mode="before")
    @classmethod
    def parse_qa_enabled(cls, value: Any) -> bool:
        return cls._parse_bool(value)

    @field_validator("qa_fallback_to_menu", "qa_buttons_shown", mode="before")
    @classmethod
    def parse_qa_flags(cls, value: Any) -> bool:
        return cls._parse_bool(value, default=True)

    @field_validator("qa_rate_limit_seconds")
    @classmethod
    def validate_qa_rate_limit(cls, value: int) -> int:
        return max(0, value)

    @field_validator(
        "reminder_enabled",
        "reminder_only_if_no_lead",
        "reminder_only_if_not_used",
        mode="before",
    )
    @classmethod
    def parse_reminder_flags(cls, value: Any) -> bool:
        return cls._parse_bool(value)

    @field_validator("lottery_enabled", mode="before")
    @classmethod
    def parse_lottery_enabled(cls, value: Any) -> bool:
        return cls._parse_bool(value)

    @field_validator("lottery_ab_test", mode="before")
    @classmethod
    def parse_lottery_ab_test(cls, value: Any) -> bool:
        return cls._parse_bool(value)

    @field_validator("alerts_rate_limit")
    @classmethod
    def validate_alerts_rate_limit(cls, value: int) -> int:
        return max(0, value)

    @field_validator("alerts_bundle_window")
    @classmethod
    def validate_alerts_bundle_window(cls, value: int) -> int:
        return max(0, value)

    @field_validator("reminder_delay_hours")
    @classmethod
    def validate_delay(cls, value: int) -> int:
        return max(0, value)

    @field_validator("reminder_max_per_user")
    @classmethod
    def validate_max_per_user(cls, value: int) -> int:
        return max(0, value)

    @field_validator("reminder_work_hours", mode="before")
    @classmethod
    def parse_work_hours(cls, value: Any) -> tuple[int, int]:
        if isinstance(value, (list, tuple)) and len(value) >= 2:
            start, end = int(value[0]), int(value[1])
        elif isinstance(value, str):
            parts = [part.strip() for part in value.replace("–", "-").split("-") if part.strip()]
            if len(parts) != 2:
                return (10, 20)
            start, end = int(parts[0]), int(parts[1])
        else:
            return (10, 20)
        if start == end:
            end = (end + 1) % 24
        if start < 0 or start > 23 or end < 0 or end > 24:
            return (10, 20)
        if start > end:
            start, end = end, start
        return (start, end)

    @field_validator("lottery_variants")
    @classmethod
    def validate_lottery_variants(cls, value: int) -> int:
        if value < 0:
            return 0
        return value

    @field_validator("lottery_weights", mode="before")
    @classmethod
    def parse_lottery_weights(cls, value: Any) -> list[float]:
        if isinstance(value, (list, tuple)):
            return [float(x) for x in value if str(x).strip() != ""]
        if isinstance(value, str):
            if not value.strip():
                return []
            parts = [part.strip() for part in value.split(",") if part.strip()]
            return [float(part) for part in parts]
        if value in (None, ""):
            return []
        return [float(value)]

    @field_validator("lottery_results", mode="before")
    @classmethod
    def parse_lottery_results(cls, value: Any) -> list[str]:
        if isinstance(value, (list, tuple)):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            if not value.strip():
                return []
            if value.strip().startswith("["):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        return [str(item).strip() for item in parsed if str(item).strip()]
                except json.JSONDecodeError:
                    pass
            parts = [part.strip() for part in value.split(",")]
            return [part for part in parts if part]
        return []

    @field_validator("lottery_coupon_campaign_map", mode="before")
    @classmethod
    def parse_lottery_campaign_map(cls, value: Any) -> Dict[str, str]:
        if isinstance(value, dict):
            return {str(k): str(v) for k, v in value.items() if str(k).strip()}
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return {}
            try:
                parsed = json.loads(value)
                if isinstance(parsed, dict):
                    return {
                        str(k): str(v)
                        for k, v in parsed.items()
                        if str(k).strip()
                    }
            except json.JSONDecodeError:
                pairs = [part.strip() for part in value.split(",") if part.strip()]
                mapping: Dict[str, str] = {}
                for pair in pairs:
                    if ":" in pair:
                        key, val = pair.split(":", 1)
                        key = key.strip()
                        val = val.strip()
                        if key:
                            mapping[key] = val
                return mapping
        return {}

    @field_validator("lottery_cooldown_days")
    @classmethod
    def validate_lottery_cooldown(cls, value: int) -> int:
        return max(0, value)

    @field_validator("draw_prefix")
    @classmethod
    def normalize_draw_prefix(cls, value: str) -> str:
        prefix = (value or "").strip()
        return prefix or "draw_"

    @cached_property
    def google_service_credentials(self) -> Dict[str, Any]:
        decoded = base64.b64decode(self.google_service_json_b64)
        return json.loads(decoded)


@lru_cache()
def get_settings() -> Settings:
    return Settings()
