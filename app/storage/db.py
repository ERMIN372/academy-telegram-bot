from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

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
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                user_id INTEGER NOT NULL,
                campaign TEXT NOT NULL,
                code TEXT,
                scheduled_at TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                sent_at TEXT,
                cancelled_at TEXT,
                reason TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(user_id, campaign)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                user_id INTEGER NOT NULL,
                campaign TEXT NOT NULL,
                created_at TEXT NOT NULL,
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


async def upsert_lead(user_id: int, campaign: str) -> None:
    await init_db()
    async with aiosqlite.connect(_db_file) as db:
        await db.execute(
            "INSERT OR REPLACE INTO leads(user_id, campaign, created_at) VALUES(?,?,?)",
            (user_id, campaign, dt.datetime.utcnow().isoformat()),
        )
        await db.commit()


async def has_lead(user_id: int, campaign: str) -> bool:
    await init_db()
    async with aiosqlite.connect(_db_file) as db:
        cursor = await db.execute(
            "SELECT 1 FROM leads WHERE user_id=? AND campaign=? LIMIT 1",
            (user_id, campaign),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return row is not None


async def get_reminder(user_id: int, campaign: str) -> Optional[Dict[str, Any]]:
    await init_db()
    async with aiosqlite.connect(_db_file) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id, campaign, code, scheduled_at, status, attempts, sent_at, cancelled_at, reason "
            "FROM reminders WHERE user_id=? AND campaign=?",
            (user_id, campaign),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return dict(row)


async def upsert_reminder(
    user_id: int,
    campaign: str,
    code: str,
    scheduled_at: str,
    attempts: int,
    status: str = "scheduled",
    reason: str | None = None,
) -> None:
    await init_db()
    async with aiosqlite.connect(_db_file) as db:
        now = dt.datetime.utcnow().isoformat()
        await db.execute(
            """
            INSERT INTO reminders(
                user_id,
                campaign,
                code,
                scheduled_at,
                status,
                attempts,
                reason,
                created_at,
                updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(user_id, campaign) DO UPDATE SET
                code=excluded.code,
                scheduled_at=excluded.scheduled_at,
                status=excluded.status,
                attempts=excluded.attempts,
                reason=excluded.reason,
                updated_at=excluded.updated_at
            """,
            (
                user_id,
                campaign,
                code,
                scheduled_at,
                status,
                attempts,
                reason or "",
                now,
                now,
            ),
        )
        await db.commit()


async def update_reminder(
    user_id: int,
    campaign: str,
    *,
    status: str | None = None,
    scheduled_at: str | None = None,
    attempts: int | None = None,
    reason: str | None = None,
    sent_at: str | None = None,
    cancelled_at: str | None = None,
) -> None:
    await init_db()
    fields: List[str] = []
    values: List[Any] = []
    if status is not None:
        fields.append("status=?")
        values.append(status)
    if scheduled_at is not None:
        fields.append("scheduled_at=?")
        values.append(scheduled_at)
    if attempts is not None:
        fields.append("attempts=?")
        values.append(attempts)
    if reason is not None:
        fields.append("reason=?")
        values.append(reason)
    if sent_at is not None:
        fields.append("sent_at=?")
        values.append(sent_at)
    if cancelled_at is not None:
        fields.append("cancelled_at=?")
        values.append(cancelled_at)
    if not fields:
        return
    fields.append("updated_at=?")
    values.append(dt.datetime.utcnow().isoformat())
    values.extend([user_id, campaign])

    async with aiosqlite.connect(_db_file) as db:
        await db.execute(
            f"UPDATE reminders SET {', '.join(fields)} WHERE user_id=? AND campaign=?",
            values,
        )
        await db.commit()


async def fetch_pending_reminders() -> List[Dict[str, Any]]:
    await init_db()
    async with aiosqlite.connect(_db_file) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id, campaign, code, scheduled_at, attempts FROM reminders WHERE status=?",
            ("scheduled",),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]
