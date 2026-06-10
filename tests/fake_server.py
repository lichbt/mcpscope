"""Fake stdio MCP server for tests. Offline, deterministic unless delays are used.

Tools:
  echo       — replies with the given text
  fail       — tool-level failure (result.isError = true)
  rpc_error  — JSON-RPC error response
Special arguments:
  delay_ms      — sleep before responding (lets tests force out-of-order replies)
  garbage_first — emit a non-JSON line before the real response
"""

import asyncio
import json
import sys


async def handle(line: bytes, write) -> None:
    try:
        obj = json.loads(line)
    except ValueError:
        return
    method = obj.get("method")
    rid = obj.get("id")
    if method is None or rid is None:
        return  # response or notification: nothing to do
    params = obj.get("params") or {}
    if method == "initialize":
        resp = {
            "jsonrpc": "2.0",
            "id": rid,
            "result": {
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fake-server", "version": "0.0.1"},
            },
        }
    elif method == "tools/list":
        resp = {
            "jsonrpc": "2.0",
            "id": rid,
            "result": {"tools": [{"name": "echo", "inputSchema": {"type": "object"}}]},
        }
    elif method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        if args.get("delay_ms"):
            await asyncio.sleep(args["delay_ms"] / 1000)
        if args.get("garbage_first"):
            write(b"%% this is not JSON at all %%\n")
        if name == "fail":
            resp = {
                "jsonrpc": "2.0",
                "id": rid,
                "result": {
                    "content": [{"type": "text", "text": "boom"}],
                    "isError": True,
                },
            }
        elif name == "rpc_error":
            resp = {
                "jsonrpc": "2.0",
                "id": rid,
                "error": {"code": -32000, "message": "server exploded"},
            }
        else:
            resp = {
                "jsonrpc": "2.0",
                "id": rid,
                "result": {
                    "content": [{"type": "text", "text": args.get("text", "")}]
                },
            }
    else:
        resp = {
            "jsonrpc": "2.0",
            "id": rid,
            "error": {"code": -32601, "message": "method not found"},
        }
    write((json.dumps(resp) + "\n").encode())


async def main() -> None:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer
    )
    out = sys.stdout.buffer

    def write(data: bytes) -> None:
        out.write(data)
        out.flush()

    tasks = []
    while True:
        line = await reader.readline()
        if not line:
            break
        tasks.append(asyncio.create_task(handle(line, write)))
    if tasks:
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
