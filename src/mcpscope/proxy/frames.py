"""JSON-RPC frame interpretation — copy-side only, never on the splice path.

The matcher receives a *copy* of each newline-delimited frame and emits store
events. It must accept anything: unparseable or unknown frames become RawSeen
events and are never an error. Frames are matched request<->response by JSON-RPC
id, per request direction (a response always travels opposite to its request).
"""

from __future__ import annotations

import itertools
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone

CLIENT_TO_SERVER = "client->server"
SERVER_TO_CLIENT = "server->client"

MAX_STORED_JSON = 256 * 1024
TRUNCATION_MARKER = "...[truncated by mcpscope]"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def clip(text: str) -> str:
    if len(text) <= MAX_STORED_JSON:
        return text
    return text[: MAX_STORED_JSON - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER


def _opposite(direction: str) -> str:
    return SERVER_TO_CLIENT if direction == CLIENT_TO_SERVER else CLIENT_TO_SERVER


@dataclass(slots=True)
class RequestSeen:
    key: int
    jsonrpc_id: str
    direction: str
    method: str
    tool_name: str | None
    ts: str
    json_text: str


@dataclass(slots=True)
class ResponseSeen:
    key: int  # matches a prior RequestSeen.key
    ts: str
    latency_ms: float
    is_error: bool
    json_text: str


@dataclass(slots=True)
class UnmatchedResponseSeen:
    jsonrpc_id: str
    direction: str
    ts: str
    is_error: bool
    json_text: str


@dataclass(slots=True)
class NotificationSeen:
    direction: str
    method: str
    ts: str
    json_text: str


@dataclass(slots=True)
class RawSeen:
    direction: str
    ts: str
    text: str


@dataclass(slots=True)
class ClientInfoSeen:
    client_info_json: str


Event = (
    RequestSeen
    | ResponseSeen
    | UnmatchedResponseSeen
    | NotificationSeen
    | RawSeen
    | ClientInfoSeen
)


class FrameMatcher:
    """Stateful per-session matcher. feed() one frame, get zero+ events back."""

    def __init__(self) -> None:
        self._keys = itertools.count(1)
        # (request_direction, normalized_id) -> (key, monotonic_seconds)
        self._pending: dict[tuple[str, str], tuple[int, float]] = {}

    def feed(self, line: bytes, direction: str) -> list[Event]:
        ts = now_iso()
        text = line.decode("utf-8", errors="replace").rstrip("\r\n")
        if not text.strip():
            return []
        try:
            obj = json.loads(text)
        except ValueError:
            return [RawSeen(direction, ts, clip(text))]
        if not isinstance(obj, dict):
            return [RawSeen(direction, ts, clip(text))]

        method = obj.get("method")
        rpc_id = obj.get("id")
        has_id = rpc_id is not None

        if isinstance(method, str) and has_id:
            return self._on_request(obj, method, rpc_id, direction, ts, text)
        if isinstance(method, str):
            return [NotificationSeen(direction, method, ts, clip(text))]
        if has_id and ("result" in obj or "error" in obj):
            return self._on_response(obj, rpc_id, direction, ts, text)
        return [RawSeen(direction, ts, clip(text))]

    def _on_request(
        self, obj: dict, method: str, rpc_id: object, direction: str, ts: str, text: str
    ) -> list[Event]:
        events: list[Event] = []
        params = obj.get("params")
        params = params if isinstance(params, dict) else {}

        tool_name = None
        if method == "tools/call":
            name = params.get("name")
            tool_name = name if isinstance(name, str) else None

        if method == "initialize":
            client_info = params.get("clientInfo")
            if client_info is not None:
                events.append(ClientInfoSeen(json.dumps(client_info)))

        key = next(self._keys)
        norm_id = json.dumps(rpc_id)
        self._pending[(direction, norm_id)] = (key, time.monotonic())
        events.append(
            RequestSeen(key, norm_id, direction, method, tool_name, ts, clip(text))
        )
        return events

    def _on_response(
        self, obj: dict, rpc_id: object, direction: str, ts: str, text: str
    ) -> list[Event]:
        norm_id = json.dumps(rpc_id)
        is_error = "error" in obj
        result = obj.get("result")
        if isinstance(result, dict) and result.get("isError"):
            is_error = True  # tools/call soft failure

        match = self._pending.pop((_opposite(direction), norm_id), None)
        if match is None:
            return [UnmatchedResponseSeen(norm_id, direction, ts, is_error, clip(text))]
        key, started = match
        latency_ms = (time.monotonic() - started) * 1000.0
        return [ResponseSeen(key, ts, latency_ms, is_error, clip(text))]

    @property
    def pending_count(self) -> int:
        return len(self._pending)
