import json

import pytest

from mcpscope.proxy.frames import (
    CLIENT_TO_SERVER,
    RawSeen,
    RequestSeen,
    ResponseSeen,
)
from mcpscope.store.writer import Recorder
from mcpscope.tui.app import MCPScopeApp
from textual.widgets import DataTable, Static

TS = "2026-06-11T00:00:00.000+00:00"


@pytest.fixture
async def seeded_db(db_path):
    rec = Recorder(db_path, "stdio", json.dumps(["demo-server"]), batch_interval=0.01)
    await rec.start()
    rec.emit(
        RequestSeen(1, "1", CLIENT_TO_SERVER, "initialize", None, TS, '{"method":"initialize"}')
    )
    rec.emit(ResponseSeen(1, TS, 42.0, False, '{"result":{"ok":true}}'))
    rec.emit(
        RequestSeen(2, "2", CLIENT_TO_SERVER, "tools/call", "read_file", TS,
                    '{"method":"tools/call","params":{"name":"read_file"}}')
    )
    rec.emit(ResponseSeen(2, TS, 1500.0, False, '{"result":{"content":[]}}'))
    rec.emit(
        RequestSeen(3, "3", CLIENT_TO_SERVER, "tools/call", "write_file", TS,
                    '{"method":"tools/call","params":{"name":"write_file"}}')
    )
    rec.emit(ResponseSeen(3, TS, 5.0, True, '{"error":{"code":-1}}'))
    rec.emit(RawSeen(CLIENT_TO_SERVER, TS, "%% junk %%"))
    await rec.close()
    return db_path


async def test_tui_shows_calls_and_stats(seeded_db):
    app = MCPScopeApp(seeded_db)
    async with app.run_test(size=(120, 36)) as pilot:
        await pilot.pause()
        table = app.query_one("#feed", DataTable)
        assert table.row_count == 4
        stats = app.query_one("#stats", Static)
        text = str(stats.render())
        assert "4 calls" in text
        assert "1 errors" in text
        assert "p95" in text


async def test_tui_detail_pane_updates_on_selection(seeded_db):
    app = MCPScopeApp(seeded_db)
    async with app.run_test(size=(120, 36)) as pilot:
        await pilot.pause()
        table = app.query_one("#feed", DataTable)
        table.focus()
        table.move_cursor(row=1)  # the read_file call
        await pilot.pause()
        # the detail pane holds the rich JSON renderable of the selected call
        assert "read_file" in str(app.query_one("#req", Static).content.text)


async def test_tui_filter_narrows_rows(seeded_db):
    app = MCPScopeApp(seeded_db)
    async with app.run_test(size=(120, 36)) as pilot:
        await pilot.pause()
        await pilot.press("slash")
        for ch in "write_file":
            await pilot.press(ch)
        await pilot.press("enter")
        await pilot.pause()
        table = app.query_one("#feed", DataTable)
        assert table.row_count == 1
        # esc clears the filter
        await pilot.press("escape")
        await pilot.pause()
        assert table.row_count == 4


async def test_tui_status_filter(seeded_db):
    app = MCPScopeApp(seeded_db)
    async with app.run_test(size=(120, 36)) as pilot:
        await pilot.pause()
        await pilot.press("slash")
        for ch in "error":
            await pilot.press(ch)
        await pilot.press("enter")
        await pilot.pause()
        table = app.query_one("#feed", DataTable)
        assert table.row_count == 1


async def test_tui_live_update_and_in_place_response(seeded_db):
    """New rows appear and pending rows update while the app is open."""
    app = MCPScopeApp(seeded_db, follow=True)
    async with app.run_test(size=(120, 36)) as pilot:
        await pilot.pause()
        table = app.query_one("#feed", DataTable)
        assert table.row_count == 4

        rec = Recorder(seeded_db, "stdio", "[]", batch_interval=0.01)
        await rec.start()  # follow mode should jump to this newer session
        rec.emit(
            RequestSeen(1, "9", CLIENT_TO_SERVER, "tools/call", "late_tool", TS, "{}")
        )
        await pilot.pause(0.6)
        assert table.row_count == 1  # switched to the new session

        rec.emit(ResponseSeen(1, TS, 7.0, False, '{"result":{}}'))
        await pilot.pause(0.6)
        await rec.close()
        assert table.row_count == 1
        assert app._row_state[5] == (True, False)  # answered in place
