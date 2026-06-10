"""Write-behind recorder: sync emit() into a queue, async consumer into SQLite.

The splice path calls emit() and never awaits; if the queue is full the event is
dropped and counted — recording must never block or break pass-through.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from mcpscope.proxy import frames
from mcpscope.store.schema import open_db

_STOP = object()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Recorder:
    def __init__(
        self,
        db_path: Path | str,
        transport: str,
        server_cmd: str,
        *,
        batch_size: int = 50,
        batch_interval: float = 0.25,
        queue_size: int = 10_000,
    ) -> None:
        self._db_path = db_path
        self._transport = transport
        self._server_cmd = server_cmd
        self._batch_size = batch_size
        self._batch_interval = batch_interval
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._rowids: dict[int, int] = {}  # matcher key -> calls.id
        self._consumer: asyncio.Task | None = None
        self._db = None
        self.session_id: int | None = None
        self.dropped = 0

    async def start(self) -> int:
        self._db = await open_db(self._db_path)
        cur = await self._db.execute(
            "INSERT INTO sessions (started_at, transport, server_cmd) VALUES (?, ?, ?)",
            (_now_iso(), self._transport, self._server_cmd),
        )
        self.session_id = cur.lastrowid
        await self._db.commit()
        self._consumer = asyncio.create_task(self._consume())
        return self.session_id

    def emit(self, event: frames.Event) -> None:
        """Sync, non-blocking. Drops (and counts) when the queue is full."""
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1

    async def close(self) -> None:
        await self._queue.put(_STOP)
        if self._consumer is not None:
            await self._consumer
        if self._db is not None:
            await self._db.execute(
                "UPDATE sessions SET ended_at = ? WHERE id = ?",
                (_now_iso(), self.session_id),
            )
            await self._db.commit()
            await self._db.close()
            self._db = None

    async def _consume(self) -> None:
        uncommitted = 0
        while True:
            try:
                event = await asyncio.wait_for(
                    self._queue.get(), timeout=self._batch_interval
                )
            except TimeoutError:
                if uncommitted:
                    await self._db.commit()
                    uncommitted = 0
                continue
            if event is _STOP:
                break
            await self._apply(event)
            uncommitted += 1
            if uncommitted >= self._batch_size:
                await self._db.commit()
                uncommitted = 0
        if uncommitted:
            await self._db.commit()

    async def _apply(self, event: frames.Event) -> None:
        db = self._db
        sid = self.session_id
        match event:
            case frames.RequestSeen():
                cur = await db.execute(
                    "INSERT INTO calls (session_id, jsonrpc_id, direction, method,"
                    " tool_name, ts_request, request_json)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        sid,
                        event.jsonrpc_id,
                        event.direction,
                        event.method,
                        event.tool_name,
                        event.ts,
                        event.json_text,
                    ),
                )
                self._rowids[event.key] = cur.lastrowid
            case frames.ResponseSeen():
                rowid = self._rowids.pop(event.key, None)
                if rowid is None:
                    return  # request event was dropped under pressure
                await db.execute(
                    "UPDATE calls SET ts_response = ?, latency_ms = ?, is_error = ?,"
                    " response_json = ? WHERE id = ?",
                    (
                        event.ts,
                        event.latency_ms,
                        int(event.is_error),
                        event.json_text,
                        rowid,
                    ),
                )
            case frames.UnmatchedResponseSeen():
                await db.execute(
                    "INSERT INTO calls (session_id, jsonrpc_id, direction, ts_request,"
                    " ts_response, is_error, response_json)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        sid,
                        event.jsonrpc_id,
                        event.direction,
                        event.ts,
                        event.ts,
                        int(event.is_error),
                        event.json_text,
                    ),
                )
            case frames.NotificationSeen():
                await db.execute(
                    "INSERT INTO calls (session_id, direction, method, ts_request,"
                    " request_json) VALUES (?, ?, ?, ?, ?)",
                    (sid, event.direction, event.method, event.ts, event.json_text),
                )
            case frames.RawSeen():
                await db.execute(
                    "INSERT INTO calls (session_id, direction, ts_request,"
                    " request_json, raw) VALUES (?, ?, ?, ?, 1)",
                    (sid, event.direction, event.ts, event.text),
                )
            case frames.ClientInfoSeen():
                await db.execute(
                    "UPDATE sessions SET client_info = ? WHERE id = ?",
                    (event.client_info_json, sid),
                )
