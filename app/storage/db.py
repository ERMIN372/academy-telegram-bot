from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from typing import Optional

import aiosqlite

_db_lock = asyncio.Lock()
_db_path = Path("data")
_db_file = _db_path / "bot.sqlite3"


async def init_db() -> None:
    _db_path.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(_db_file) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS issued (
                user_id INTEGER NOT NULL,
                campaign TEXT NOT NULL,
                code TEXT NOT NULL,
                ts TEXT NOT NULL,
                PRIMARY KEY(user_id, campaign)
            )
            """
        )
        await db.commit()


async def fetch_user_coupon(user_id: int, campaign: str) -> Optional[dict]:
    await init_db()
    async with aiosqlite.connect(_db_file) as db:
        cursor = await db.execute(
            "SELECT code, ts FROM issued WHERE user_id=? AND campaign=?",
            (user_id, campaign),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row:
            return {"code": row[0], "ts": row[1]}
        return None


async def insert_coupon(user_id: int, campaign: str, code: str) -> None:
    await init_db()
    async with aiosqlite.connect(_db_file) as db:
        await db.execute(
            "REPLACE INTO issued(user_id, campaign, code, ts) VALUES(?,?,?,?)",
            (user_id, campaign, code, dt.datetime.utcnow().isoformat()),
        )
        await db.commit()
