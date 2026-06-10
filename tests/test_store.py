import asyncio
import json
import sqlite3

from mcpscope.proxy.frames import (
    CLIENT_TO_SERVER,
    SERVER_TO_CLIENT,
    ClientInfoSeen,
    NotificationSeen,
    RawSeen,
    RequestSeen,
    ResponseSeen,
    UnmatchedResponseSeen,
)
from mcpscope.store.writer import Recorder

TS = "2026-06-10T00:00:00.000+00:00"


async def test_recorder_full_lifecycle(db_path):
    rec = Recorder(db_path, "stdio", json.dumps(["echo-server"]), batch_interval=0.01)
    session_id = await rec.start()

    rec.emit(ClientInfoSeen(json.dumps({"name": "test-client", "version": "1"})))
    rec.emit(RequestSeen(1, "1", CLIENT_TO_SERVER, "tools/call", "echo", TS, "{...}"))
    rec.emit(ResponseSeen(1, TS, 12.5, False, '{"ok":1}'))
    rec.emit(RequestSeen(2, "2", CLIENT_TO_SERVER, "tools/call", "fail", TS, "{...}"))
    rec.emit(ResponseSeen(2, TS, 3.0, True, '{"err":1}'))
    rec.emit(NotificationSeen(CLIENT_TO_SERVER, "notifications/initialized", TS, "{}"))
    rec.emit(RawSeen(SERVER_TO_CLIENT, TS, "%% junk %%"))
    rec.emit(UnmatchedResponseSeen("9", SERVER_TO_CLIENT, TS, False, "{}"))
    rec.emit(RequestSeen(3, "3", CLIENT_TO_SERVER, "ping", None, TS, "{}"))  # unanswered
    await rec.close()

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    sess = db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    assert sess["transport"] == "stdio"
    assert sess["ended_at"] is not None
    assert json.loads(sess["client_info"])["name"] == "test-client"

    calls = db.execute(
        "SELECT * FROM calls WHERE session_id=? ORDER BY id", (session_id,)
    ).fetchall()
    assert len(calls) == 6

    echo = calls[0]
    assert (echo["method"], echo["tool_name"]) == ("tools/call", "echo")
    assert echo["latency_ms"] == 12.5
    assert echo["is_error"] == 0
    assert echo["response_json"] == '{"ok":1}'

    fail = calls[1]
    assert fail["is_error"] == 1

    notif = calls[2]
    assert notif["jsonrpc_id"] is None
    assert notif["method"] == "notifications/initialized"

    raw = calls[3]
    assert raw["raw"] == 1
    assert raw["request_json"] == "%% junk %%"

    unmatched = calls[4]
    assert unmatched["jsonrpc_id"] == "9"
    assert unmatched["ts_response"] is not None

    unanswered = calls[5]
    assert unanswered["ts_response"] is None
    assert unanswered["method"] == "ping"


async def test_queue_full_drops_and_counts(db_path):
    rec = Recorder(db_path, "stdio", "[]", queue_size=2)
    # consumer not started: the queue fills and emit must not block or raise
    for i in range(5):
        rec.emit(RawSeen(CLIENT_TO_SERVER, TS, f"line {i}"))
    assert rec.dropped == 3
    await rec.start()
    await rec.close()
    db = sqlite3.connect(db_path)
    assert db.execute("SELECT COUNT(*) FROM calls").fetchone()[0] == 2


async def test_two_sessions_same_db(db_path):
    for cmd in ("a", "b"):
        rec = Recorder(db_path, "stdio", cmd)
        await rec.start()
        await rec.close()
    db = sqlite3.connect(db_path)
    rows = db.execute("SELECT server_cmd FROM sessions ORDER BY id").fetchall()
    assert [r[0] for r in rows] == ["a", "b"]
