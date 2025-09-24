from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Sequence, TypeVar

import gspread
from google.oauth2.service_account import Credentials

from app.config import get_settings

_client_lock = asyncio.Lock()
_client: gspread.Client | None = None

logger = logging.getLogger(__name__)

T = TypeVar("T")


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


async def ensure_columns(sheet: str, columns: Sequence[str]) -> List[str]:
    def _ensure(ws: gspread.Worksheet) -> List[str]:
        headers = ws.row_values(1) or []
        missing = [column for column in columns if column not in headers]
        if missing:
            headers.extend(missing)
            ws.update("1:1", [headers])
            logger.warning(
                "Added missing columns %s to sheet '%s'", ", ".join(missing), sheet
            )
        return headers

    return await _with_worksheet(sheet, _ensure)


async def append(sheet: str, row: Dict[str, Any]) -> None:
    def _append(ws: gspread.Worksheet) -> None:
        headers = ws.row_values(1)
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


async def update_row(sheet: str, row: int, data: Dict[str, Any]) -> None:
    def _update(ws: gspread.Worksheet) -> None:
        headers = ws.row_values(1)
        if not headers:
            return
        current_values = ws.row_values(row)
        merged = {header: "" for header in headers}
        for header, value in zip(headers, current_values):
            merged[header] = value
        merged.update(data)
        values = [merged.get(header, "") for header in headers]
        end_col = _column_letter(len(values))
        ws.update(f"A{row}:{end_col}{row}", [values])

    await _with_worksheet(sheet, _update)
