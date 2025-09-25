from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, TypeVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import gspread
from google.oauth2.service_account import Credentials

from app.config import get_settings

logger = logging.getLogger(__name__)

_client_lock = asyncio.Lock()
_client: gspread.Client | None = None

DEFAULT_SHEETS_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

T = TypeVar("T")


@dataclass(frozen=True)
class SheetTimestamp:
    moment: dt.datetime
    utc_text: str
    local_text: str

    @property
    def meta(self) -> Dict[str, str]:
        return {"utc": self.utc_text, "msk": self.local_text}


def _column_letter(index: int) -> str:
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


async def get_client() -> gspread.Client:
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                settings = get_settings()
                info = settings.google_service_credentials

                def _create_client() -> gspread.Client:
                    creds = Credentials.from_service_account_info(
                        info,
                        scopes=[
                            "https://www.googleapis.com/auth/spreadsheets",
                            "https://www.googleapis.com/auth/drive",
                        ],
                    )
                    return gspread.authorize(creds)

                _client = await asyncio.to_thread(_create_client)
    return _client


async def _with_worksheet(sheet: str, worker: Callable[[gspread.Worksheet], T]) -> T:
    client = await get_client()
    settings = get_settings()

    def _call_worker() -> T:
        worksheet = client.open_by_key(settings.google_sheets_id).worksheet(sheet)
        return worker(worksheet)

    return await asyncio.to_thread(_call_worker)


@lru_cache(maxsize=4)
def _resolve_sheet_timezone(name: str | None) -> dt.tzinfo:
    if not name:
        return dt.timezone.utc
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        logger.warning("Unknown SHEETS_TZ '%s', falling back to UTC", name)
        return dt.timezone.utc


def current_timestamp() -> SheetTimestamp:
    settings = get_settings()
    aware_utc = dt.datetime.now(dt.timezone.utc)
    utc_text = aware_utc.isoformat().replace("+00:00", "Z")
    timezone = _resolve_sheet_timezone(settings.sheets_tz)
    localized = aware_utc.astimezone(timezone)
    time_format = settings.sheets_time_format or DEFAULT_SHEETS_TIME_FORMAT
    try:
        local_text = localized.strftime(time_format)
    except Exception:
        logger.warning(
            "Invalid SHEETS_TIME_FORMAT '%s', falling back to default", time_format
        )
        local_text = localized.strftime(DEFAULT_SHEETS_TIME_FORMAT)
    return SheetTimestamp(moment=aware_utc, utc_text=utc_text, local_text=local_text)


def _ensure_headers(ws: gspread.Worksheet, required_headers: Iterable[str]) -> List[str]:
    headers = ws.row_values(1)
    ordered_required: List[str] = []
    seen: set[str] = set()
    for header in required_headers:
        if not header:
            continue
        if header in seen:
            continue
        ordered_required.append(header)
        seen.add(header)

    if not headers:
        if ordered_required:
            end_col = _column_letter(len(ordered_required))
            ws.update(f"A1:{end_col}1", [ordered_required])
            logger.warning(
                "Sheet %s had empty header row, added headers: %s",
                ws.title,
                ", ".join(ordered_required),
            )
            headers = ordered_required
        return headers

    missing = [header for header in ordered_required if header not in headers]
    if missing:
        new_headers = headers + missing
        end_col = _column_letter(len(new_headers))
        ws.update(f"A1:{end_col}1", [new_headers])
        logger.warning(
            "Added missing headers %s to sheet %s",
            ", ".join(missing),
            ws.title,
        )
        headers = new_headers

    return headers


async def append(
    sheet: str,
    row: Dict[str, Any],
    *,
    optional_headers: Iterable[str] | None = None,
    meta: Dict[str, Any] | None = None,
) -> None:
    optional_set = {header for header in (optional_headers or []) if header}

    def _append(ws: gspread.Worksheet) -> None:
        headers = _ensure_headers(
            ws, [key for key in row.keys() if key not in optional_set]
        )
        missing_optional = [header for header in optional_set if header not in headers]
        if missing_optional:
            logger.warning(
                "Sheet %s is missing optional columns: %s",
                ws.title,
                ", ".join(missing_optional),
            )
        if meta:
            logger.info("Appending to %s with timestamp meta: %s", ws.title, meta)
        if headers:
            values = [row.get(header, "") for header in headers]
        else:
            values = list(row.values())
        ws.append_row(values, value_input_option="USER_ENTERED")

    await _with_worksheet(sheet, _append)


async def read(sheet: str) -> List[Dict[str, Any]]:
    def _read(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
        records = ws.get_all_records()
        result: List[Dict[str, Any]] = []
        for idx, record in enumerate(records, start=2):
            item = dict(record)
            item["row"] = idx
            result.append(item)
        return result

    return await _with_worksheet(sheet, _read)


async def update_row(
    sheet: str,
    row: int,
    data: Dict[str, Any],
    *,
    optional_headers: Iterable[str] | None = None,
    meta: Dict[str, Any] | None = None,
) -> None:
    optional_set = {header for header in (optional_headers or []) if header}

    def _update(ws: gspread.Worksheet) -> None:
        headers = _ensure_headers(
            ws, [key for key in data.keys() if key not in optional_set]
        )
        if not headers:
            return
        missing_optional = [header for header in optional_set if header not in headers]
        if missing_optional:
            logger.warning(
                "Sheet %s is missing optional columns: %s",
                ws.title,
                ", ".join(missing_optional),
            )
        if meta:
            logger.info(
                "Updating %s row %d with timestamp meta: %s", ws.title, row, meta
            )
        current_values = ws.row_values(row)
        merged = {header: "" for header in headers}
        for header, value in zip(headers, current_values):
            merged[header] = value
        merged.update(data)
        values = [merged.get(header, "") for header in headers]
        end_col = _column_letter(len(values))
        ws.update(f"A{row}:{end_col}{row}", [values])

    await _with_worksheet(sheet, _update)
