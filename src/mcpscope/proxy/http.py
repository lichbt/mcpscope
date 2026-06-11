"""Streamable HTTP transport proxy: local reverse proxy with a recording tee.

    agent ──HTTP──▶ localhost:<port>  ──verbatim──▶ upstream MCP server
                        │
                        └─ parse copies (POST bodies, JSON/SSE responses)
                           → write-behind queue → SQLite

Spec status (checked 2026-06-11): Streamable HTTP is the current remote
transport (spec 2025-11-25; the 2026-07-28 RC only adds routing headers, which
we forward untouched). The pre-2025 HTTP+SSE transport is being switched off
by vendors and is not implemented here.

Pass-through rules: every request is forwarded to the upstream origin with the
incoming path/query and body verbatim; response bytes are streamed back
verbatim. We only strip hop-by-hop headers and force identity encoding so the
tee sees plain bytes. Parsing happens on copies and can never fail a request.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from mcpscope.proxy.frames import (
    CLIENT_TO_SERVER,
    SERVER_TO_CLIENT,
    FrameMatcher,
)
from mcpscope.store.writer import Recorder

# not forwarded in either direction (RFC 9110 §7.6.1 connection-level headers,
# plus those the proxy/httpx layer re-derives)
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "accept-encoding",
}

_MAX_TEE_BODY = 8 * 1024 * 1024  # parse-side cap; pass-through is unaffected


class SSETee:
    """Incremental SSE parser for the copy side: feed chunks, get data payloads."""

    def __init__(self) -> None:
        self._buf = b""

    def feed(self, chunk: bytes) -> list[bytes]:
        self._buf += chunk
        payloads: list[bytes] = []
        # events are separated by a blank line (\n\n or \r\n\r\n)
        normalized = self._buf.replace(b"\r\n", b"\n")
        while (i := normalized.find(b"\n\n")) >= 0:
            event, normalized = normalized[:i], normalized[i + 2 :]
            data_lines = [
                line[5:].removeprefix(b" ")
                for line in event.split(b"\n")
                if line.startswith(b"data:")
            ]
            if data_lines:
                payloads.append(b"\n".join(data_lines))
        self._buf = normalized
        return payloads


class HttpProxy:
    def __init__(
        self,
        upstream: str,
        db_path: Path | str,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        quiet: bool = False,
    ) -> None:
        self.upstream = httpx.URL(upstream)
        self._origin = self.upstream.copy_with(path="/", query=None, fragment=None)
        self._db_path = db_path
        self._host = host
        self._port = port
        self._quiet = quiet
        self.matcher = FrameMatcher()
        self.recorder = Recorder(db_path, "http", str(self.upstream))
        self._client: httpx.AsyncClient | None = None
        self._server: uvicorn.Server | None = None
        self._serve_task: asyncio.Task | None = None

    # -- copy-side observation (never raises into the splice path) -------------

    def _observe(self, payload: bytes, direction: str) -> None:
        try:
            for event in self.matcher.feed(payload, direction):
                self.recorder.emit(event)
        except Exception:  # observation must never break forwarding
            pass

    # -- request handling -------------------------------------------------------

    async def _handle(self, request: Request) -> Response:
        body = await request.body()
        if body:
            self._observe(body, CLIENT_TO_SERVER)

        headers = [
            (k, v)
            for k, v in request.headers.items()
            if k.lower() not in _HOP_BY_HOP
        ]
        target = self._origin.copy_with(
            raw_path=request.url.path.encode()
            + (b"?" + request.url.query.encode() if request.url.query else b"")
        )
        upstream_req = self._client.build_request(
            request.method, target, headers=headers, content=body
        )
        try:
            upstream_resp = await self._client.send(upstream_req, stream=True)
        except httpx.HTTPError as exc:
            return Response(f"mcpscope: upstream unreachable: {exc}", status_code=502)

        resp_headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        content_type = upstream_resp.headers.get("content-type", "")

        if "text/event-stream" in content_type:
            tee = SSETee()

            async def stream():
                try:
                    async for chunk in upstream_resp.aiter_raw():
                        for payload in tee.feed(chunk):
                            self._observe(payload, SERVER_TO_CLIENT)
                        yield chunk
                finally:
                    await upstream_resp.aclose()

            return StreamingResponse(
                stream(), status_code=upstream_resp.status_code, headers=resp_headers
            )

        # buffered tee for ordinary bodies (JSON-RPC responses are small)
        teed = bytearray()

        async def stream_plain():
            try:
                async for chunk in upstream_resp.aiter_raw():
                    if len(teed) <= _MAX_TEE_BODY:
                        teed.extend(chunk)
                    yield chunk
            finally:
                await upstream_resp.aclose()
                if teed and "json" in content_type and len(teed) <= _MAX_TEE_BODY:
                    self._observe(bytes(teed), SERVER_TO_CLIENT)

        return StreamingResponse(
            stream_plain(), status_code=upstream_resp.status_code, headers=resp_headers
        )

    # -- lifecycle --------------------------------------------------------------

    async def start(self) -> int:
        """Bind and serve in the background; returns the actual local port."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=30, read=None, write=None, pool=None)
        )
        await self.recorder.start()
        app = Starlette(
            routes=[
                Route(
                    "/{path:path}",
                    self._handle,
                    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
                )
            ]
        )
        config = uvicorn.Config(
            app,
            host=self._host,
            port=self._port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._serve_task = asyncio.create_task(self._server.serve())
        while not self._server.started:
            if self._serve_task.done():
                self._serve_task.result()  # surface bind errors
                raise RuntimeError("uvicorn exited before startup")
            await asyncio.sleep(0.01)
        self.port = self._server.servers[0].sockets[0].getsockname()[1]
        if not self._quiet:
            local = self.upstream.copy_with(
                scheme="http", host=self._host, port=self.port
            )
            print(
                f"mcpscope: session {self.recorder.session_id} proxying "
                f"{local} -> {self.upstream} (db {self._db_path})",
                file=sys.stderr,
                flush=True,
            )
        return self.port

    async def wait(self) -> None:
        await self._serve_task

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
            await self._serve_task
        if self._client is not None:
            await self._client.aclose()
        await self.recorder.close()


async def run_http_proxy(
    upstream: str, db_path: Path | str, *, port: int = 0, quiet: bool = False
) -> int:
    """Entry point for `mcpscope run --http <url>`. Serves until SIGINT/SIGTERM."""
    proxy = HttpProxy(upstream, db_path, port=port, quiet=quiet)
    await proxy.start()
    try:
        await proxy.wait()  # uvicorn installs its own signal handlers
    finally:
        await proxy.stop()
    return 0
