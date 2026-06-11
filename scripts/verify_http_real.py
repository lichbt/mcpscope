"""Reality check (plan §4): real MCP SDK client+server through the HTTP proxy.

Starts a FastMCP streamable-HTTP server, puts the mcpscope proxy in front,
drives it with the official SDK client, then prints what got recorded.
Run: .venv/bin/python scripts/verify_http_real.py
"""

import asyncio
import json
import multiprocessing
import socket
import sqlite3
import sys
import tempfile
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from mcpscope.proxy.http import HttpProxy


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def serve_fastmcp(port: int) -> None:
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("real-sdk-server", host="127.0.0.1", port=port)

    @mcp.tool()
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    mcp.run(transport="streamable-http")


async def main() -> int:
    upstream_port = free_port()
    server_proc = multiprocessing.Process(
        target=serve_fastmcp, args=(upstream_port,), daemon=True
    )
    server_proc.start()

    db_path = Path(tempfile.mkdtemp()) / "verify.db"
    proxy = HttpProxy(
        f"http://127.0.0.1:{upstream_port}/mcp", db_path, port=0, quiet=True
    )

    # wait for the upstream to accept connections
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", upstream_port), timeout=0.1):
                break
        except OSError:
            await asyncio.sleep(0.1)
    else:
        print("FAIL: upstream never came up")
        return 1

    local_port = await proxy.start()
    url = f"http://127.0.0.1:{local_port}/mcp"

    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            info = await session.initialize()
            tools = await session.list_tools()
            result = await session.call_tool("add", {"a": 2, "b": 3})

    await proxy.stop()
    server_proc.terminate()

    print(f"server: {info.serverInfo.name} (proto {info.protocolVersion})")
    print(f"tools: {[t.name for t in tools.tools]}")
    print(f"add(2,3) -> {result.content[0].text}")

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT method, tool_name, direction, latency_ms, is_error, raw,"
        " ts_response IS NOT NULL AS answered FROM calls ORDER BY id"
    ).fetchall()
    print(f"\nrecorded {len(rows)} frames:")
    for r in rows:
        print(
            f"  {r['direction']:<16} {r['method'] or '(raw)':<34}"
            f" tool={r['tool_name'] or '-':<6} answered={r['answered']}"
            f" err={r['is_error']} raw={r['raw']}"
        )

    call = db.execute(
        "SELECT * FROM calls WHERE method='tools/call' AND tool_name='add'"
    ).fetchone()
    ok = call and call["ts_response"] is not None and not call["is_error"]
    sess = db.execute("SELECT client_info FROM sessions").fetchone()
    client_ok = sess["client_info"] and "mcp" in sess["client_info"].lower()
    print(f"\nclient_info: {sess['client_info']}")
    print("PASS" if (ok and client_ok) else "FAIL")
    return 0 if (ok and client_ok) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
