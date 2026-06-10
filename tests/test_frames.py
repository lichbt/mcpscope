import json

from mcpscope.proxy import frames
from mcpscope.proxy.frames import (
    CLIENT_TO_SERVER,
    SERVER_TO_CLIENT,
    ClientInfoSeen,
    FrameMatcher,
    NotificationSeen,
    RawSeen,
    RequestSeen,
    ResponseSeen,
    UnmatchedResponseSeen,
)


def _line(obj) -> bytes:
    return (json.dumps(obj) + "\n").encode()


def test_request_then_response_matches_with_latency():
    m = FrameMatcher()
    [req] = m.feed(
        _line({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}), CLIENT_TO_SERVER
    )
    assert isinstance(req, RequestSeen)
    assert req.method == "tools/list"
    assert req.jsonrpc_id == "1"
    assert req.tool_name is None

    [resp] = m.feed(
        _line({"jsonrpc": "2.0", "id": 1, "result": {"tools": []}}), SERVER_TO_CLIENT
    )
    assert isinstance(resp, ResponseSeen)
    assert resp.key == req.key
    assert resp.latency_ms >= 0
    assert resp.is_error is False
    assert m.pending_count == 0


def test_tools_call_captures_tool_name():
    m = FrameMatcher()
    [req] = m.feed(
        _line(
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {"name": "read_file", "arguments": {"path": "/x"}},
            }
        ),
        CLIENT_TO_SERVER,
    )
    assert req.tool_name == "read_file"


def test_initialize_emits_client_info_and_request():
    m = FrameMatcher()
    events = m.feed(
        _line(
            {
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "clientInfo": {"name": "claude-desktop", "version": "1.0"},
                },
            }
        ),
        CLIENT_TO_SERVER,
    )
    kinds = [type(e) for e in events]
    assert kinds == [ClientInfoSeen, RequestSeen]
    assert json.loads(events[0].client_info_json)["name"] == "claude-desktop"


def test_notification_has_no_id():
    m = FrameMatcher()
    [ev] = m.feed(
        _line({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        CLIENT_TO_SERVER,
    )
    assert isinstance(ev, NotificationSeen)
    assert ev.method == "notifications/initialized"
    assert m.pending_count == 0


def test_garbage_is_raw_never_dropped():
    m = FrameMatcher()
    [ev] = m.feed(b"%% not json %%\n", SERVER_TO_CLIENT)
    assert isinstance(ev, RawSeen)
    assert "not json" in ev.text
    # valid JSON but not an object is also raw
    [ev2] = m.feed(b"[1,2,3]\n", SERVER_TO_CLIENT)
    assert isinstance(ev2, RawSeen)
    # blank lines produce nothing
    assert m.feed(b"\n", SERVER_TO_CLIENT) == []


def test_unmatched_response():
    m = FrameMatcher()
    [ev] = m.feed(
        _line({"jsonrpc": "2.0", "id": 99, "result": {}}), SERVER_TO_CLIENT
    )
    assert isinstance(ev, UnmatchedResponseSeen)
    assert ev.jsonrpc_id == "99"


def test_out_of_order_responses_match_correct_requests():
    m = FrameMatcher()
    [r1] = m.feed(_line({"jsonrpc": "2.0", "id": 1, "method": "a"}), CLIENT_TO_SERVER)
    [r2] = m.feed(_line({"jsonrpc": "2.0", "id": 2, "method": "b"}), CLIENT_TO_SERVER)
    [resp2] = m.feed(_line({"jsonrpc": "2.0", "id": 2, "result": {}}), SERVER_TO_CLIENT)
    [resp1] = m.feed(_line({"jsonrpc": "2.0", "id": 1, "result": {}}), SERVER_TO_CLIENT)
    assert resp2.key == r2.key
    assert resp1.key == r1.key


def test_int_and_str_ids_are_distinct():
    m = FrameMatcher()
    m.feed(_line({"jsonrpc": "2.0", "id": 1, "method": "a"}), CLIENT_TO_SERVER)
    m.feed(_line({"jsonrpc": "2.0", "id": "1", "method": "b"}), CLIENT_TO_SERVER)
    assert m.pending_count == 2
    [resp] = m.feed(_line({"jsonrpc": "2.0", "id": "1", "result": {}}), SERVER_TO_CLIENT)
    assert isinstance(resp, ResponseSeen)
    assert m.pending_count == 1  # int id 1 still pending


def test_same_id_both_directions_do_not_collide():
    m = FrameMatcher()
    [c_req] = m.feed(_line({"jsonrpc": "2.0", "id": 5, "method": "x"}), CLIENT_TO_SERVER)
    [s_req] = m.feed(
        _line({"jsonrpc": "2.0", "id": 5, "method": "sampling/createMessage"}),
        SERVER_TO_CLIENT,
    )
    # client answers the server's request; must match s_req, not c_req
    [resp] = m.feed(_line({"jsonrpc": "2.0", "id": 5, "result": {}}), CLIENT_TO_SERVER)
    assert resp.key == s_req.key
    assert m.pending_count == 1


def test_jsonrpc_error_and_tool_iserror_flag():
    m = FrameMatcher()
    m.feed(_line({"jsonrpc": "2.0", "id": 1, "method": "tools/call"}), CLIENT_TO_SERVER)
    [resp] = m.feed(
        _line({"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "x"}}),
        SERVER_TO_CLIENT,
    )
    assert resp.is_error is True

    m.feed(_line({"jsonrpc": "2.0", "id": 2, "method": "tools/call"}), CLIENT_TO_SERVER)
    [resp2] = m.feed(
        _line({"jsonrpc": "2.0", "id": 2, "result": {"content": [], "isError": True}}),
        SERVER_TO_CLIENT,
    )
    assert resp2.is_error is True


def test_clip_truncates_huge_payloads():
    big = "x" * (frames.MAX_STORED_JSON + 100)
    clipped = frames.clip(big)
    assert len(clipped) == frames.MAX_STORED_JSON
    assert clipped.endswith(frames.TRUNCATION_MARKER)
    assert frames.clip("small") == "small"
