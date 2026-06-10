import asyncio
import json
import sqlite3
import sys

from conftest import pipe_pair

from mcpscope.proxy import stdio
from mcpscope.proxy.frames import (
    CLIENT_TO_SERVER,
    FrameMatcher,
    RawSeen,
    RequestSeen,
)
from mcpscope.store.writer import Recorder


def _frame(obj) -> bytes:
    return (json.dumps(obj) + "\n").encode()


INIT = _frame(
    {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "pytest-client", "version": "1.0"},
        },
    }
)
INITIALIZED = _frame({"jsonrpc": "2.0", "method": "notifications/initialized"})
LIST = _frame({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})


def _call(rid, name, **arguments) -> bytes:
    return _frame(
        {
            "jsonrpc": "2.0",
            "id": rid,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
    )


async def run_proxy_e2e(
    argv: list[str], db_path, input_bytes: bytes
) -> tuple[bytes, Recorder, int]:
    """Drive splice() with real pipes and a real subprocess; return proxy output."""
    client_in_reader, client_in_writer = await pipe_pair()
    client_out_reader, client_out_writer = await pipe_pair()
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        limit=2**20,
    )
    rec = Recorder(db_path, "stdio", json.dumps(argv), batch_interval=0.01)
    await rec.start()

    async def drive() -> None:
        client_in_writer.write(input_bytes)
        await client_in_writer.drain()
        client_in_writer.close()

    out = bytearray()

    async def read_out() -> None:
        while True:
            chunk = await client_out_reader.read(4096)
            if not chunk:
                return
            out.extend(chunk)

    rc, _, _ = await asyncio.gather(
        stdio.splice(client_in_reader, client_out_writer, proc, rec),
        drive(),
        read_out(),
    )
    await rec.close()
    return bytes(out), rec, rc


async def test_e2e_records_conversation_and_passes_through(db_path, fake_server_argv):
    input_bytes = (
        INIT
        + INITIALIZED
        + LIST
        + _call(2, "echo", text="hello")
        + _call(3, "fail")
        + _call(4, "rpc_error")
        + b"%% client garbage %%\n"
    )
    out, rec, rc = await run_proxy_e2e(fake_server_argv, db_path, input_bytes)
    assert rc == 0
    assert rec.dropped == 0

    # output is valid newline-delimited JSON responses, one per request
    lines = out.decode().splitlines()
    by_id = {json.loads(l)["id"]: json.loads(l) for l in lines}
    assert set(by_id) == {0, 1, 2, 3, 4}
    assert by_id[0]["result"]["serverInfo"]["name"] == "fake-server"
    assert by_id[2]["result"]["content"][0]["text"] == "hello"

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    sess = db.execute("SELECT * FROM sessions").fetchone()
    assert sess["ended_at"] is not None
    assert json.loads(sess["client_info"])["name"] == "pytest-client"
    assert json.loads(sess["server_cmd"]) == fake_server_argv

    calls = db.execute("SELECT * FROM calls ORDER BY id").fetchall()
    by_method = {}
    for r in calls:
        by_method.setdefault(r["method"], []).append(r)

    init_row = by_method["initialize"][0]
    assert init_row["ts_response"] is not None
    assert init_row["latency_ms"] is not None

    tool_rows = {r["jsonrpc_id"]: r for r in by_method["tools/call"]}
    assert tool_rows["2"]["tool_name"] == "echo"
    assert tool_rows["2"]["is_error"] == 0
    assert tool_rows["3"]["is_error"] == 1  # result.isError
    assert tool_rows["4"]["is_error"] == 1  # JSON-RPC error

    assert len(by_method["notifications/initialized"]) == 1
    raw_rows = [r for r in calls if r["raw"] == 1]
    assert len(raw_rows) == 1
    assert "client garbage" in raw_rows[0]["request_json"]


async def test_byte_identical_passthrough(db_path, fake_server_argv):
    """Proxy output must equal what the server emits when driven directly."""
    input_bytes = INIT + INITIALIZED + LIST + _call(2, "echo", text="verbatim?")
    direct = await asyncio.create_subprocess_exec(
        *fake_server_argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    direct_out, _ = await direct.communicate(input_bytes)

    proxied_out, _, _ = await run_proxy_e2e(fake_server_argv, db_path, input_bytes)
    assert proxied_out == direct_out


async def test_out_of_order_responses_recorded(db_path, fake_server_argv):
    input_bytes = (
        INIT + _call(10, "echo", text="slow", delay_ms=60) + _call(11, "echo", text="fast")
    )
    out, _, _ = await run_proxy_e2e(fake_server_argv, db_path, input_bytes)
    lines = [json.loads(l) for l in out.decode().splitlines()]
    assert [l["id"] for l in lines] == [0, 11, 10]  # out of order on the wire

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    rows = {
        r["jsonrpc_id"]: r
        for r in db.execute("SELECT * FROM calls WHERE method='tools/call'")
    }
    assert rows["10"]["ts_response"] is not None
    assert rows["11"]["ts_response"] is not None
    assert rows["10"]["latency_ms"] > rows["11"]["latency_ms"]


async def test_garbage_interleaved_from_server(db_path, fake_server_argv):
    input_bytes = INIT + _call(2, "echo", text="x", garbage_first=True)
    out, _, _ = await run_proxy_e2e(fake_server_argv, db_path, input_bytes)
    assert b"%% this is not JSON at all %%\n" in out  # forwarded verbatim
    db = sqlite3.connect(db_path)
    raw_count = db.execute("SELECT COUNT(*) FROM calls WHERE raw=1").fetchone()[0]
    assert raw_count == 1


async def test_pump_is_verbatim_including_partial_trailing_line(collector):
    src_reader, src_writer = await pipe_pair()
    dst_reader, dst_writer = await pipe_pair()
    payload = (
        _frame({"jsonrpc": "2.0", "id": 1, "method": "ping"})
        + b"garbage no newline at eof"
    )
    src_writer.write(payload)
    src_writer.close()
    await stdio._pump(src_reader, dst_writer, CLIENT_TO_SERVER, FrameMatcher(), collector)
    received = await dst_reader.read()
    assert received == payload
    kinds = [type(e) for e in collector.events]
    assert kinds == [RequestSeen, RawSeen]


async def test_oversized_line_passes_through_unparsed(collector, monkeypatch):
    monkeypatch.setattr(stdio, "MAX_PARSE_LINE", 64)
    src_reader, src_writer = await pipe_pair()
    dst_reader, dst_writer = await pipe_pair()
    # the oversized line arrives split across reads, newline only at the end
    huge_head = b"A" * 500
    huge_tail = b"AAA\n"
    normal = _frame({"jsonrpc": "2.0", "id": 1, "method": "ping"})

    async def drive():
        src_writer.write(huge_head)
        await src_writer.drain()
        await asyncio.sleep(0.02)  # let the pump see the partial line first
        src_writer.write(huge_tail + normal)
        await src_writer.drain()
        src_writer.close()

    out = bytearray()

    async def read_out():
        while True:
            chunk = await dst_reader.read(4096)
            if not chunk:
                return
            out.extend(chunk)

    await asyncio.gather(
        stdio._pump(src_reader, dst_writer, CLIENT_TO_SERVER, FrameMatcher(), collector),
        drive(),
        read_out(),
    )
    assert bytes(out) == huge_head + huge_tail + normal  # verbatim throughout
    raw_events = [e for e in collector.events if isinstance(e, RawSeen)]
    assert len(raw_events) == 1
    assert "oversized" in raw_events[0].text
    assert any(isinstance(e, RequestSeen) for e in collector.events)


async def test_run_stdio_proxy_via_cli_subprocess(db_path, fake_server_argv, tmp_path):
    """The real entry path: mcpscope run -- <server>, as a subprocess."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "mcpscope",
        "run",
        "--db",
        str(db_path),
        "--quiet",
        "--",
        *fake_server_argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, errout = await proc.communicate(INIT + _call(2, "echo", text="cli"))
    assert proc.returncode == 0, errout.decode()
    by_id = {json.loads(l)["id"]: json.loads(l) for l in out.decode().splitlines()}
    assert by_id[2]["result"]["content"][0]["text"] == "cli"

    db = sqlite3.connect(db_path)
    n = db.execute(
        "SELECT COUNT(*) FROM calls WHERE tool_name='echo' AND ts_response IS NOT NULL"
    ).fetchone()[0]
    assert n == 1
