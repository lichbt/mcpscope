import asyncio
import json
import sqlite3

import httpx
import pytest
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from mcpscope.proxy.http import HttpProxy, SSETee

# ---------------------------------------------------------------- SSETee unit


def test_sse_tee_single_event():
    tee = SSETee()
    assert tee.feed(b'data: {"a":1}\n\n') == [b'{"a":1}']


def test_sse_tee_event_split_across_chunks():
    tee = SSETee()
    assert tee.feed(b"event: message\nda") == []
    assert tee.feed(b'ta: {"a":1}\n') == []
    assert tee.feed(b"\n") == [b'{"a":1}']


def test_sse_tee_multiline_data_and_crlf():
    tee = SSETee()
    [payload] = tee.feed(b"data: line1\r\ndata: line2\r\n\r\n")
    assert payload == b"line1\nline2"


def test_sse_tee_multiple_events_one_chunk():
    tee = SSETee()
    assert tee.feed(b"data: 1\n\ndata: 2\n\n") == [b"1", b"2"]


def test_sse_tee_comment_and_eventless_lines_ignored():
    tee = SSETee()
    assert tee.feed(b": keepalive\n\nevent: ping\n\ndata: x\n\n") == [b"x"]


# ------------------------------------------------------------- upstream stub


def make_upstream() -> Starlette:
    async def mcp(request):
        body = json.loads(await request.body())
        rid = body.get("id")
        method = body.get("method")
        if rid is None:  # notification
            return Response(status_code=202)
        if method == "initialize":
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": rid,
                    "result": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "fake-http-server", "version": "1"},
                    },
                },
                headers={"Mcp-Session-Id": "sess-abc"},
            )
        if method == "tools/call":
            text = body["params"]["arguments"].get("text", "")

            async def gen():
                yield (
                    b"event: message\n"
                    b'data: {"jsonrpc":"2.0","method":"notifications/progress",'
                    b'"params":{"progress":0.5}}\n\n'
                )
                yield b"data: %% sse garbage %%\n\n"
                resp = json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": rid,
                        "result": {"content": [{"type": "text", "text": text}]},
                    }
                )
                yield f"event: message\ndata: {resp}\n\n".encode()

            return StreamingResponse(gen(), media_type="text/event-stream")
        return JSONResponse(
            {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "nope"}}
        )

    return Starlette(routes=[Route("/mcp", mcp, methods=["POST"])])


async def serve(app) -> tuple[uvicorn.Server, asyncio.Task, int]:
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    return server, task, port


INIT = {
    "jsonrpc": "2.0",
    "id": 0,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-11-25",
        "capabilities": {},
        "clientInfo": {"name": "pytest-http-client", "version": "1.0"},
    },
}
INITIALIZED = {"jsonrpc": "2.0", "method": "notifications/initialized"}
CALL = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {"name": "echo", "arguments": {"text": "over http"}},
}


@pytest.fixture
async def stack(db_path):
    """Upstream stub + proxy in front of it; yields (proxy, local_url, direct_url)."""
    upstream_server, upstream_task, upstream_port = await serve(make_upstream())
    proxy = HttpProxy(
        f"http://127.0.0.1:{upstream_port}/mcp", db_path, port=0, quiet=True
    )
    local_port = await proxy.start()
    yield proxy, f"http://127.0.0.1:{local_port}/mcp", f"http://127.0.0.1:{upstream_port}/mcp"
    await proxy.stop()
    upstream_server.should_exit = True
    await upstream_task


async def test_http_e2e_records_and_passes_through(stack, db_path):
    proxy, local, direct = stack
    async with httpx.AsyncClient() as client:
        r_init = await client.post(local, json=INIT)
        r_notif = await client.post(local, json=INITIALIZED)
        r_call = await client.post(local, json=CALL)

        # pass-through fidelity vs. hitting the upstream directly
        d_init = await client.post(direct, json=INIT)
        d_call = await client.post(direct, json=CALL)

    assert r_init.status_code == 200
    assert r_init.json() == d_init.json()
    assert r_init.headers["mcp-session-id"] == "sess-abc"  # custom headers forwarded
    assert r_notif.status_code == 202
    assert r_call.headers["content-type"].startswith("text/event-stream")
    assert r_call.content == d_call.content  # SSE bytes identical

    await proxy.stop()  # flush recorder

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    sess = db.execute("SELECT * FROM sessions").fetchone()
    assert sess["transport"] == "http"
    assert json.loads(sess["client_info"])["name"] == "pytest-http-client"
    assert direct in sess["server_cmd"]

    calls = db.execute("SELECT * FROM calls ORDER BY id").fetchall()
    by_method = {}
    for r in calls:
        by_method.setdefault(r["method"], []).append(r)

    init_row = by_method["initialize"][0]
    assert init_row["ts_response"] is not None
    assert init_row["latency_ms"] is not None
    assert init_row["is_error"] == 0

    call_row = by_method["tools/call"][0]
    assert call_row["tool_name"] == "echo"
    assert call_row["ts_response"] is not None  # matched out of the SSE stream
    assert "over http" in call_row["response_json"]

    assert len(by_method["notifications/initialized"]) == 1  # client notification
    assert len(by_method["notifications/progress"]) == 1  # server SSE notification

    raw_rows = [r for r in calls if r["raw"] == 1]
    assert len(raw_rows) == 1
    assert "sse garbage" in raw_rows[0]["request_json"]


async def test_http_error_response_recorded(stack, db_path):
    proxy, local, _ = stack
    async with httpx.AsyncClient() as client:
        r = await client.post(
            local, json={"jsonrpc": "2.0", "id": 9, "method": "bogus/method"}
        )
    assert r.json()["error"]["code"] == -32601
    await proxy.stop()
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT * FROM calls WHERE method='bogus/method'").fetchone()
    assert row["is_error"] == 1
    assert row["latency_ms"] is not None


async def test_http_upstream_down_returns_502(db_path):
    proxy = HttpProxy("http://127.0.0.1:9/mcp", db_path, port=0, quiet=True)
    local_port = await proxy.start()
    async with httpx.AsyncClient() as client:
        r = await client.post(f"http://127.0.0.1:{local_port}/mcp", json=INIT)
    assert r.status_code == 502
    await proxy.stop()
