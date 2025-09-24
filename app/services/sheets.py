from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import gspread
from google.oauth2.service_account import Credentials

from app.config import get_settings

_client_lock = asyncio.Lock()
_client: gspread.Client | None = None


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
                info = settings.google_service_credentials()
                creds = Credentials.from_service_account_info(info, scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ])
                _client = gspread.authorize(creds)
    return _client


async def _worksheet(sheet: str):
    client = await get_client()
    spreadsheet = client.open_by_key(get_settings().google_sheets_id)
    return spreadsheet.worksheet(sheet)


async def append(sheet: str, row: Dict[str, Any]) -> None:
    ws = await _worksheet(sheet)
    headers = ws.row_values(1)
    if headers:
        values = [row.get(header, "") for header in headers]
    else:
        values = list(row.values())
    ws.append_row(values, value_input_option="USER_ENTERED")


async def read(sheet: str) -> List[Dict[str, Any]]:
    ws = await _worksheet(sheet)
    records = ws.get_all_records()
    result: List[Dict[str, Any]] = []
    for idx, record in enumerate(records, start=2):
        item = dict(record)
        item["row"] = idx
        result.append(item)
    return result


async def update_row(sheet: str, row: int, data: Dict[str, Any]) -> None:
    ws = await _worksheet(sheet)
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
