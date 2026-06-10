"""SQLite schema (v1) and migrations, gated on PRAGMA user_version."""

from __future__ import annotations

import os
from pathlib import Path

import aiosqlite

SCHEMA_VERSION = 1

DDL_V1 = """
CREATE TABLE sessions (
  id INTEGER PRIMARY KEY,
  started_at TEXT NOT NULL,            -- ISO-8601 UTC
  ended_at TEXT,
  transport TEXT NOT NULL,             -- 'stdio' | 'http'
  server_cmd TEXT,                     -- argv as JSON (stdio) or upstream URL (http)
  client_info TEXT                     -- from initialize params, when seen
);
CREATE TABLE calls (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES sessions(id),
  jsonrpc_id TEXT,                     -- NULL for notifications
  direction TEXT NOT NULL,             -- 'client->server' | 'server->client'
  method TEXT,                         -- e.g. tools/call; NULL on unparseable frames
  tool_name TEXT,                      -- params.name when method = tools/call
  ts_request TEXT NOT NULL,
  ts_response TEXT,
  latency_ms REAL,
  is_error INTEGER NOT NULL DEFAULT 0, -- JSON-RPC error or tool isError
  request_json TEXT,                   -- full frame; truncate >256KB with marker
  response_json TEXT,
  raw INTEGER NOT NULL DEFAULT 0       -- 1 = unparseable, stored verbatim
);
CREATE INDEX idx_calls_session ON calls(session_id);
CREATE INDEX idx_calls_tool ON calls(tool_name);
"""


def default_db_path() -> Path:
    """~/.mcpscope/mcpscope.db, overridable via $MCPSCOPE_DB."""
    env = os.environ.get("MCPSCOPE_DB")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".mcpscope" / "mcpscope.db"


async def open_db(path: Path | str) -> aiosqlite.Connection:
    """Open (and create/migrate if needed) the database at *path*."""
    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    db = await aiosqlite.connect(path)
    await db.execute("PRAGMA journal_mode=WAL")  # TUI reads while proxy writes
    await db.execute("PRAGMA synchronous=NORMAL")
    async with db.execute("PRAGMA user_version") as cur:
        (version,) = await cur.fetchone()
    if version < 1:
        await db.executescript(DDL_V1)
        await db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        await db.commit()
    elif version > SCHEMA_VERSION:
        await db.close()
        raise RuntimeError(
            f"database {path} has schema v{version}, newer than this "
            f"mcpscope (v{SCHEMA_VERSION}) — upgrade mcpscope"
        )
    return db
